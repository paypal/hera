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
The test will start Mysql server docker and Hera connects to this Mysql DB docker
No setup needed

*/

var mx testutil.Mux

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["rac_sql_interval"] = "0"
        appcfg["request_backlog_timeout"] = "6000"
        appcfg["opscfg.default.server.saturation_recover_threshold"] = "500" 
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "10" //Killing interval = 1000/(0.1*3) = 3333ms

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
        testutil.RunDMLCommitLater("update test_simple_table_1 set Name='Steve' where ID=" + id, wait_second)
}


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*
 * change saturation throttle rate at runtime 
 */


func TestChangeThrottleRateAtRuntime(t *testing.T) {
	fmt.Println ("TestChangeThrottleRateAtRuntime begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestChangeThrottleRateAtRuntime begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12346, 'Jack', 100)")

	fmt.Println ("First thread to insert a row, but commit later");
        id := "123"
        go insert_row_delay_commit(id, 5)

        fmt.Println ("Having 5 threads to update same row, so all workers are busy")
        for i := 0; i < 5; i++ {
                time.Sleep(200 * time.Millisecond);
                go update_row_delay_commit(id, 3)
        }

        time.Sleep(9 * time.Second);

        fmt.Println ("Verify next requests are fine");
        row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = 12346");
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is there");
	}
	fmt.Println ("Since we have only 3 workers, saturation will be kicked in to kill long running queries")

	fmt.Println ("Verify SATURATION event")
	hcount := testutil.RegexCountFile ("HARD_EVICTION", "cal.log")
	if ( hcount < 0) {
            t.Fatalf ("Error: expected at least 1 HARD_EVICTION event");
        }
	count := testutil.RegexCountFile ("STRANDED.*RECOVERED_SATURATION_RECOVERED", "cal.log")
	if ( count < hcount) {
            t.Fatalf ("Error: expected %d RECOVERED_SATURATION_RECOVERED events", hcount);
        }

	fmt.Println ("Verify saturation error is returned to client")
        if ( testutil.RegexCount("error to client.*saturation kill") < hcount) {
	   t.Fatalf ("Error: should get saturation kill error");
	}
	fmt.Println ("Verify sql killing rate is correct")
        if ( testutil.RegexCount("saturation recover active.*3333") < 3) {
	   t.Fatalf ("Error: saturate recover rate is not 3333ms");
	}

	fmt.Println ("We now change saturation_recover_throttle_rate at runtime");
        testutil.ModifyOpscfgParam (t, "hera.txt", "saturation_recover_throttle_rate", "70")
        //Wait for opsfcg change to take effect
        time.Sleep(50 * time.Second)

	fmt.Println ("First thread to insert a row, but commit later");
        id = "456"
        go insert_row_delay_commit(id, 4)

        fmt.Println ("Having 5 threads to update same row, so all workers are busy, query will be killed")
        for i := 0; i < 5; i++ {
                time.Sleep(200 * time.Millisecond);
                go update_row_delay_commit(id, 2)
        }

        time.Sleep(8 * time.Second);
        fmt.Println ("Verify SATURATION events")
        hcount1 := testutil.RegexCountFile ("HARD_EVICTION", "cal.log")
        if ( hcount1 < hcount + 2) {
            t.Fatalf ("Error: expected at least 2 HARD_EVICTION events");
        }
        count = testutil.RegexCountFile ("STRANDED.*RECOVERED_SATURATION_RECOVERED", "cal.log")
        if ( count < hcount1) {
            t.Fatalf ("Error: expected %d RECOVERED_SATURATION_RECOVERED events", hcount);
        }

        fmt.Println ("Verify saturation error is returned to client")
        if ( testutil.RegexCount("error to client.*saturation kill") < hcount1) {
           t.Fatalf ("Error: should get saturation kill error");
        }
        fmt.Println ("Verify sql killing rate is correct")
        if ( testutil.RegexCount("saturation recover active.*476") < 3) {
		t.Fatalf ("Error: saturate recover rate is not 476ms");
        }
	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestChangeThrottleRateAtRuntime done  -------------------------------------------------------------")

}
