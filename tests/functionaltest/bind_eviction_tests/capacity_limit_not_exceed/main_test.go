package main 
import (
	"fmt"
	"os"
	"testing"
	"time"
        _"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/tests/functionaltest/bind_eviction_tests/util"
	"github.com/paypal/hera/utility/logger"
)

/*
The test will start Mysql server docker and Hera connects to this Mysql DB docker
No setup needed

*/

var mx testutil.Mux

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to choose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["rac_sql_interval"] = "0"
        appcfg["request_backlog_timeout"] = "6000"
        appcfg["soft_eviction_effective_time"] = "500"
        appcfg["soft_eviction_probability"] = "100"
        appcfg["opscfg.default.server.saturation_recover_threshold"] = "30" 
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "50" //Killing interval = 1000/(3*80) = 416ms

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "5"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
        return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*
 *  Bind Eviction should not happen when capcacity limit of Hera is not reached 
 *
 */


func TestCapacityLimitNotReach(t *testing.T) {
	fmt.Println ("TestCapacityLimitNotReach begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestCapacityLimitNotReach begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12345699, 'Jack', 100)")

        id1 := "01234599"
	fmt.Println ("Insert a row with id: ", id1);
	util.InsertBinding (id1, 0)

	fmt.Println ("First thread to insert a row, but commit later");
        id := "223466667"
        go util.InsertBinding (id, 3)


        fmt.Println ("Having 4 threads to update same rows, all 5 workers are busy")
        for i := 0; i < 4; i++ {
                time.Sleep(200 * time.Millisecond);
                go util.FetchBinding(id, "FOR UPDATE")
        }

        time.Sleep(6 * time.Second);
        fmt.Println ("Verify fetch requests are fine");
        row_count := util.FetchBinding (id1, ""); 
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is NOT there");
	}
	fmt.Println ("Since all workers are busy, saturation will be kicked in to kill long running queries")

	fmt.Println ("Verify no SATURATION events")
	hcount := testutil.RegexCountFile ("HARD_EVICTION", "cal.log")
	if ( hcount > 0) {
            t.Fatalf ("Error: expected no HARD_EVICTION events");
        }
	
	fmt.Println ("Verify no bind_eviction event for queries with bind length < 8")
	hcount = testutil.RegexCountFile ("BIND_EVICT", "cal.log")
	if ( hcount  > 0) {
            t.Fatalf ("Error: should not get any BIND_EVICTION event");
	}

	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestCapacityLimitNotReach done  -------------------------------------------------------------")
}
