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
The test will start Mysql docker and OCC connects to this Mysql DB docker
No setup needed

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
	//appcfg["readonly_children_pct"] = "50"
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
 *   Five threads to connect to occmux and perform an update on same table. 
 *   Since first client does not commit, other clients will have long query running
 *   We have 3 write workers, backlog queue size for write worker reached limit, write workers enter into saturation status
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

	fmt.Println ("Verify saturation recover events")
	if ( testutil.RegexCountFile ("HARD_EVICTION.*1629405935", "cal.log")  < 2){
            t.Fatalf ("Error: expected at least 2 HARD_EVICTION events");
        }
	count := testutil.RegexCountFile ("STRANDED.*REC.*ED_SATURATION_RECOVERED", "cal.log")
	if ( count < 2) {
            t.Fatalf ("Error: expected at least 2 SATURATION_RECOVERED events")
        }

	fmt.Println ("Verify saturation error is returned to client")
        if ( testutil.RegexCount("error to client.*HERA-101: saturation kill") < count/2) {
	   t.Fatalf ("Error: should get saturation kill error %d time", count/2);
	}

	if ( testutil.RegexCount("occproxy saturation recover: sql will be terminated.*close client connection") < 0) {
           t.Fatalf ("Error: should get message in log for saturation recover");
        }

	fmt.Println ("Verify sql killing rate is correct")
	if ( testutil.RegexCount("saturation recover active.*416") < 10) {
           t.Fatalf ("Error: sql killing rate is NOT correct");
        }

        time.Sleep(2 * time.Second);
	fmt.Println ("Verify correct clients are killed")
        testutil.VerifyKilledClient (t, "2")
        testutil.VerifyKilledClient (t, "3")

        fmt.Println ("Verify killing SQLs are NOT in DB");
        row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = 124");
        if (row_count >  0) {
            t.Fatalf ("Error: Row should NOT be in DB");
	}
        row_count = testutil.Fetch ("Select Name from test_simple_table_1 where ID = 125");
        if (row_count >  0) {
            t.Fatalf ("Error: Row should NOT be in DB");
	}

        fmt.Println ("Verify next requests are fine");
        row_count = testutil.Fetch ("Select Name from test_simple_table_1 where ID = 12346");
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is there");
	}
	logger.GetLogger().Log(logger.Debug, "TestWriteSaturation done  -------------------------------------------------------------")
}
