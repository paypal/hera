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
        appcfg["soft_eviction_effective_time"] = "500"
        appcfg["soft_eviction_probability"] = "80"
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "30" //Killing interval = 1000/(3*80) = 416ms
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "60" //Killing interval = 1000/(3*80) = 416ms

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
 * Verify saturation is NOT triggered when saturation feature is disabled @ runtime 
 */


func TestSaturationDisabledAtRuntime(t *testing.T) {
	fmt.Println ("TestSaturationDisabledAtRuntime begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestSaturationDisabledAtRuntime begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12346, 'Jack', 100)")

	fmt.Println ("First thread to insert a row, but commit later");
        id := "123"
        go insert_row_delay_commit(id, 5)

        fmt.Println ("Having 5 threads to update same row, so all workers are busy")
        for i := 0; i < 5; i++ {
                time.Sleep(200 * time.Millisecond);
                go update_row_delay_commit(id, 3)
        }

        time.Sleep(10 * time.Second);

        fmt.Println ("Verify next requests are fine");
        row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = 12346");
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is there");
	}
	fmt.Println ("Since we have only 3 workers, saturation will be kicked in to kill long running queries")

	fmt.Println ("Verify SATURATION events")
	hcount := testutil.RegexCountFile ("HARD_EVICTION", "cal.log")
	if ( hcount < 2) {
            t.Fatalf ("Error: expected at least 2 HARD_EVICTION events");
        }
	//Check for RECYCLED/RECOVERED_SATURATION_RECOVERED event
	count := testutil.RegexCountFile ("STRANDED.*REC.*ED_SATURATION_RECOVERED", "cal.log")
	if ( count < hcount) {
            t.Fatalf ("Error: expected %d RECOVERED_SATURATION_RECOVERED events", hcount);
        }
        /*if ( testutil.RegexCountFile ("RECOVER.*dedicated", "cal.log") < hcount ) {
            t.Fatalf ("Error: expected %d recover  event", hcount);
        }*/

	fmt.Println ("Verify saturation error is returned to client")
        if ( testutil.RegexCount("error to client.*saturation kill") < hcount) {
	   t.Fatalf ("Error: should get saturation kill error");
	}

	fmt.Println ("We now enable saturation feature at runtime");
        testutil.ModifyOpscfgParam (t, "hera.txt", "saturation_recover_throttle_rate", "0")
        //Wait for opsfcg change to take effect
        time.Sleep(50 * time.Second)

        fmt.Println ("First thread to insert a row, but commit later");
        id = "456"
        go insert_row_delay_commit(id, 4)

        fmt.Println ("Having 5 threads to update same row, so all workers are busy, query will NOT be killed because saturation is disabled")
        for i := 0; i < 5; i++ {
                time.Sleep(200 * time.Millisecond);
                go update_row_delay_commit(id, 2)
        }

        time.Sleep(8 * time.Second);
        fmt.Println ("Verify NO SATURATION events")
        hcount1 := testutil.RegexCountFile ("HARD_EVICTION", "cal.log")
        if ( hcount1 > hcount) {
            t.Fatalf ("Error: expected NO new HARD_EVICTION events = %d", hcount );
        }
        count = testutil.RegexCountFile ("STRANDED.*RECOVERED_SATURATION_RECOVERED", "cal.log")
        if ( count > hcount) {
            t.Fatalf ("Error: expected no new RECOVERED_SATURATION_RECOVERED events %d", count);
        }
	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestSaturationDisabledAtRuntime done  -------------------------------------------------------------")
}
