package main 
import (
	"fmt"
	"strconv"
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
        appcfg["bind_eviction_threshold_pct"] = "50"
        appcfg["soft_eviction_effective_time"] = "500"
        appcfg["soft_eviction_probability"] = "100"
	appcfg["opscfg.default.server.saturation_recover_threshold"] = "10" 
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "80"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "8"
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
 *  Sending queries with bind values and the queries use < 50% of connection
 *  Bind Eviction should not happen because bind_eviction_threshold_pct = 50% 
 *
 */


func TestThresholdConfigNotReach(t *testing.T) {
	fmt.Println ("TestThresholdConfigNotReach begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestThresholdConfigNotReach begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12345699, 'Jack', 100)")
	
        id1 := "11234599"
	fmt.Println ("Insert a row with id: ", id1);
	util.InsertBinding (id1, 0)

	fmt.Println ("Inserts rows, but commit later");
        id := 22345666
	for i := 0; i < 4; i++ {
            go util.InsertBinding (strconv.Itoa(id + i), 5)
        }

        fmt.Println ("Having 6 queries with bind values and the queries use  < 50% of connections ")
        for i := 0; i < 3; i++ {
                time.Sleep(100 * time.Millisecond);
		fmt.Println ("Update a row with id: ", id);
                go util.FetchBinding(strconv.Itoa(id), "FOR UPDATE")
        }

	fmt.Println ("Having  queries with bind values and the queries use  20% of connections ")
        for i := 0; i < 3; i++ {
                time.Sleep(200 * time.Millisecond);
                fmt.Println ("Update a row with id: ", id);
                go util.UpdateBinding(strconv.Itoa(id+i), 3 )
        }

        time.Sleep(7 * time.Second);
        fmt.Println ("Verify fetch request is fine");
        row_count := util.FetchBinding (id1, "");
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is NOT there");
	}
	fmt.Println ("Since all workers are busy, long request will be killed")
	hcount := testutil.RegexCountFile ("HARD_EVICTION", "cal.log")
	if ( hcount < 1) {
            t.Fatalf ("Error: expected at least 1 HARD_EVICTION event");
        }
	count := testutil.RegexCountFile ("BKLG0", "cal.log")
	if ( count < 1) {
            t.Fatalf ("Error: not expected %d BLKG0", count);
        }
	fmt.Println ("Verify no bind_eviction event because  bind_eviction_threshold_pct 25% is not reached")
	count = testutil.RegexCountFile ("BIND_EVICT", "cal.log")
	if ( count  > 0) {
            t.Fatalf ("Error: should not get any BIND_EVICTION event");
	}
	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestThresholdConfigNotReach done  -------------------------------------------------------------")
}
