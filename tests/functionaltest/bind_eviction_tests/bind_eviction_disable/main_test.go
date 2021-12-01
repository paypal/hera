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
 *  Bind Eviction should not happen when saturation is disabled
 *
 */


func TestBindEvictionDisabled(t *testing.T) {
	fmt.Println ("TestBindEvictionDisabled begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestBindEvictionDisabled begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12345699, 'Jack', 100)")
	
        id1 := "01234599"
	fmt.Println ("Insert a row with id: ", id1);
	util.InsertBinding (id1, 0)

	fmt.Println ("First thread to insert a row, but commit later");
        id := "22345678"
        go util.InsertBinding (id, 5)

        fmt.Println ("Having 6 threads to update same row, bind length < 8 bytes, all workers are busy")
        for i := 0; i < 6; i++ {
                time.Sleep(200 * time.Millisecond);
                go util.UpdateBinding(id, 3)
        }

        time.Sleep(7 * time.Second);
        fmt.Println ("Verify fetch requests are fine");
        row_count := util.FetchBinding (id1, "For Update");
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is NOT there");
	}
	fmt.Println ("Since all workers are busy, requests will go to backlog queue")
	hcount := testutil.RegexCountFile ("WARNING.*bklg0_timeout", "cal.log")
	if ( hcount < 1) {
            t.Fatalf ("Error: expected at least 1 bklg0_timeout events");
        }
	count := testutil.RegexCountFile ("BKLG0.*3", "cal.log")
	if ( count < 1) {
            t.Fatalf ("Error: expected %d BLKG0", count);
        }
	fmt.Println ("Verify no bind_eviction event because saturation is NOT ON")
	count = testutil.RegexCountFile ("BIND_EVICT", "cal.log")
	if ( count  > 0) {
            t.Fatalf ("Error: should not get any BIND_EVICTION event");
	}

	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestBindEvictionDisabled done  -------------------------------------------------------------")
}
