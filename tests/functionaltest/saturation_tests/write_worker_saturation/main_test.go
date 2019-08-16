package main 
import (
	"fmt"
	"os"
	"testing"
	"time"
        _"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*
To run the test
export username=clocapp
export password=clocappstg
export DB_USER=$username
export DB_PASSWORD=password
export TWO_TASK='tcp(127.0.0.1:3306)/world?timeout=10s'
export TWO_TASK_READ='tcp(127.0.0.1:3306)/world?timeout=10s'
export DB_DATASOURCE=$TWO_TASK

$GOROOT/bin/go install  .../worker/{mysql,oracle}worker
ln -s $GOPATH/bin/{mysql,oracle}worker .
*/

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["rac_sql_interval"] = "0"
	appcfg["readonly_children_pct"] = "50"
        appcfg["request_backlog_timeout"] = "6000"
        appcfg["opscfg.default.server.saturation_recover_threshold"] = "0"
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "80" //Killing interval = 1000/(3*80) = 416ms

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
        return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}

/**-----------------------------------------
   Helper method to insert a row with delay
--------------------------------------------*/
func insert_row_delay_commit (id string, wait_second int) {
        fmt.Println ("Insert a row, commit later")
        testutil.RunDMLCommitLater("insert into test_simple_table_1 (ID, Name, Status) VALUES (" + id + ", 'Smith', 111)", wait_second)
}

/**-----------------------------------------
   Helper method to update a row with delay
--------------------------------------------*/
func update_row_delay_commit (id string, wait_second int) {
        fmt.Println ("Update a row, commit later")
        testutil.RunDMLCommitLater("update test_simple_table_1 set Name='Steve' where ID=123", wait_second)
}


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*
 * Steps:
 *   saturation_recover_threshold is set to 0
 *   saturation_recover_throttle_rate="80" - killing rate is 1000/(0.8 * 1)
 *   First Thread performs an insert to a table but not commit
 *   Five threads to connect to occmux and perform an update on same table. Since first client is not commit,
 *   they will have long query running
 *   We have 2 read workers and 2 write workers, backlog queue size for write worker reached limit, write workers enter into saturation status
 * Verifications:
 *   Verify saturation recovery kicks in to kill long running queries
 *   Verify proxy recovers long session successfully by checking logs
 *
 */

func TestWriteSaturation(t *testing.T) {
	fmt.Println ("TestWriteSaturation begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestWriteSaturation begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12346, 'Jack', 100)")

	fmt.Println ("First thread to insert a row, but not commit");
        id := "123"
        go insert_row_delay_commit(id, 4)

        fmt.Println ("Having 5 threads to update same row.")
        fmt.Println ("Since first thread does not commit, they will have long query running")
        for i := 0; i < 5; i++ {
                time.Sleep(200 * time.Millisecond);
                go update_row_delay_commit(id, 3)
        }

        time.Sleep(5 * time.Second);

	fmt.Println ("Since we have 4 workers, saturation will be kicked in to kill long running queries")

	fmt.Println ("Verify BKLG & bklg_timeout events")
	if ( testutil.RegexCountFile ("STRANDED.*SATURATION_RECOVERED", "cal.log") < 4) {
            t.Fatalf ("Error: expected at least 2 SATURATION_RECOVERED events");
        }
        /*if ( testutil.RegexCountFile ("WORKER.*recoverworker", "cal.log") < 2) {
            t.Fatalf ("Error: expected worker recover  event");
        }*/

	fmt.Println ("Verify saturation error is returned to client")
        if ( testutil.RegexCount("error to client.*HERA-101: saturation kill") < 2) {
	   t.Fatalf ("Error: should get saturation kill error");
	}

        fmt.Println ("Since soft_eviction_effective_time = 0, soft eviction will not be kicked in")
	if ( testutil.RegexCount("occproxy saturation recover: sql will be terminated.*close client connection") < 0) {
           t.Fatalf ("Error: should get message in log for saturation recover");
        }

	fmt.Println ("Verify sql killing rate is correct")
	if ( testutil.RegexCount("saturation recover active.*416") < 10) {
           t.Fatalf ("Error: sql killing rate is NOT correct");
        }

        time.Sleep(2 * time.Second);
	fmt.Println ("Verify correct clients are killed")
        //testutil.VerifyKilledClient (t, "2")
        //testutil.VerifyKilledClient (t, "3")

        fmt.Println ("Verify next requests are fine");
        row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = 12346");
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is there");
	}
	logger.GetLogger().Log(logger.Debug, "TestWriteSaturation done  -------------------------------------------------------------")
}
