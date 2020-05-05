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
        appcfg["opscfg.default.server.saturation_recover_threshold"] = "0" 
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "0" //Killing interval = 1000/(3*80) = 416ms

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
 *   Verify saturation is  triggered when saturation feature is enabled at runtime
 */


func TestSaturationEnabledAtRuntime(t *testing.T) {
	fmt.Println ("TestSaturationEnabledAtRuntime begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestSaturationEnabledAtRuntime begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12346, 'Jack', 100)")

	fmt.Println ("First thread to insert a row, but commit later");
        id := "123"
        go insert_row_delay_commit(id, 4)

        fmt.Println ("Having 5 threads to update same row, so all workers are busy")
        for i := 0; i < 5; i++ {
                time.Sleep(200 * time.Millisecond);
                go update_row_delay_commit(id, 2)
        }

        time.Sleep(8 * time.Second);

        fmt.Println ("Verify saturation is not kicked in because it is disabled by default");
	hcount := testutil.RegexCountFile ("HARD_EVICTION", "cal.log")
	if ( hcount > 0 ) {
            t.Fatalf ("Error: expected NO HARD_EVICTION events");
        }
	count := testutil.RegexCountFile ("STRANDED.*RECOVERED_SATURATION_RECOVERED", "cal.log")
	if ( count > 0 ) {
            t.Fatalf ("Error: expected 0 RECOVERED_SATURATION_RECOVERED event");
        }

        fmt.Println ("We now enable saturation feature at runtime");
	testutil.ModifyOpscfgParam (t, "hera.txt", "saturation_recover_threshold", "30")
	testutil.ModifyOpscfgParam (t, "hera.txt", "saturation_recover_throttle_rate", "70")
	//Wait for opsfcg change to take effect
	time.Sleep(60 * time.Second)

	fmt.Println ("First thread to insert a row, but commit later");
        id = "456"
        go insert_row_delay_commit(id, 4)

        fmt.Println ("Having 5 threads to update same row, so all workers are busy, query will be killed by saturation feature")
        for i := 0; i < 5; i++ {
                time.Sleep(200 * time.Millisecond);
                go update_row_delay_commit(id, 2)
        }

        time.Sleep(8 * time.Second);
	fmt.Println ("Verify SATURATION events")
        hcount = testutil.RegexCountFile ("HARD_EVICTION", "cal.log")
        if ( hcount < 2) {
            t.Fatalf ("Error: expected at least 2 HARD_EVICTION events");
        }
        count = testutil.RegexCountFile ("STRANDED.*RECOVERED_SATURATION_RECOVERED", "cal.log")
        if ( count < hcount) {
            t.Fatalf ("Error: expected %d RECOVERED_SATURATION_RECOVERED events", hcount);
        }
        if ( testutil.RegexCountFile ("RECOVER.*dedicated", "cal.log") < hcount ) {
            t.Fatalf ("Error: expected %d recover  event", hcount);
        }

        fmt.Println ("Verify saturation error is returned to client")
        if ( testutil.RegexCount("error to client.*saturation kill") < hcount) {
           t.Fatalf ("Error: should get saturation kill error");
        }
        fmt.Println ("Verify sql killing rate is correct")
        if ( testutil.RegexCount("saturation recover active.*476") < 3) {
           t.Fatalf ("Error: saturate recover rate is not 476ms");
        }

	logger.GetLogger().Log(logger.Debug, "TestSaturationEnabledAtRuntime done  -------------------------------------------------------------")
}
