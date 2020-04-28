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

The test will start Mysql server docker and Hera server connects to this Mysql DB docker
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
        appcfg["request_backlog_timeout"] = "6000"
	appcfg["soft_eviction_probability"] = "100"
	appcfg["max_stranded_time_interval"] = "1"
        appcfg["opscfg.default.server.saturation_recover_threshold"] = "1000"  
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "50" //Killing interval = 1000/0.5*4 = 500ms

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "4"
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
 * This test case to test if we get SATURATION_RECYCLED event
 */


func TestSaturationRecycle(t *testing.T) {
	fmt.Println ("TestSaturationRecycle begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestSaturationRecycle begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12346, 'Jack', 100)")

	fmt.Println ("First thread to insert a row, but not commit");
        id := "123"
        go insert_row_delay_commit(id, 5)

        fmt.Println ("Having 10 threads to update same row.")
        fmt.Println ("Since first thread does not commit, update will have long running query")
        for i := 0; i < 10; i++ {
                time.Sleep(200 * time.Millisecond);
                go update_row_delay_commit(id, 3)
        }

        time.Sleep(10 * time.Second);

	fmt.Println ("Since all workers are busy, saturation will be kicked in to kill long running queries")
	hcount := testutil.RegexCountFile ("E.*HARD_EVICTION.*1629405935", "cal.log")
	if ( hcount < 4) {
            t.Fatalf ("Error: expected at least 4 HARD_EVICTION events");
        }

	fmt.Println ("Verify worker recovery events after saturation kill")
	count := testutil.RegexCountFile ("STRANDED.*RECOVERED_SATURATION_RECOVERED", "cal.log")
	if ( count < 4) {
            t.Fatalf ("Error: expected at least 4 SATURATION_RECOVERED events" );
        }

	fmt.Println ("Verify saturation recycle events after saturation kill")
	count = testutil.RegexCountFile ("STRANDED.*RECYCLED_SATURATION_RECOVERED", "cal.log")
	if ( count < 1) {
            t.Fatalf ("Error: expected 1 RECYCLED_SATURATION_RECOVERED events" );
	}

	fmt.Println ("Verify saturation error is returned to client")
        if ( testutil.RegexCount("error to client.*HERA-101: saturation kill") < 4) {
	   t.Fatalf ("Error: should get saturation kill in log");
	}

	fmt.Println ("Verify sql killing rate is correct")
	if ( testutil.RegexCount("saturation recover active.*500") < 10) {
           t.Fatalf ("Error: sql killing rate is NOT correct");
        }

	fmt.Println ("Verify correct clients are killed")
        testutil.VerifyKilledClient (t, "2")
        testutil.VerifyKilledClient (t, "3")
        testutil.VerifyKilledClient (t, "4")
        testutil.VerifyKilledClient (t, "5")

        fmt.Println ("Verify next requests are fine");
        row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = 12346");
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is there");
	}
	logger.GetLogger().Log(logger.Debug, "TestSaturationRecycle done  -------------------------------------------------------------")
}
