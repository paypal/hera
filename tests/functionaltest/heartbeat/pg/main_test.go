package main

import (
	//"fmt"
	"os"
	"testing"
	"time"

	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*

The test is for testing write queries only go to write connections. 
1. Set up two DB instances, one among them is read-only (but not real replica for simplicity)
2. Verify both read/write connections are healthy
3. Verify write queries in write workers and read queries in read workers
4. In middle of the test, update readonly DB to writabel, verify that read connection should continue without recycle by HB.
*/

var mx testutil.Mux
var tableName string
func cfg() (map[string]string, map[string]string, testutil.WorkerType) {
	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["db_heartbeat_interval"] = "1"
	appcfg["child.executable"] = "postgres"
	appcfg["database_type"] = "mysql"
	appcfg["readonly_children_pct"] = "50"
	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "2"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.PostgresWorker
}

func setupDb() error {
        testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
        return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*******************
 ** Validate heartbeat with MySQL worker (master-slave db setup). 
 *******************/
func TestHeartbeat(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "Test Heartbeat begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	/*
	The test hasn't setup true postgres master-slave
	*/
	time.Sleep(5 * time.Second)

	count := testutil.RegexCountFile("retry-attempt", "hera.log") // read connect tries to find read-only
	if count != 2 {
		t.Fatalf ("Error: INITDB count expects 2 but get %d", count)
	}

	count = testutil.RegexCountFile("ReadOnMaster=true", "hera.log")
	if count < 2 {
		t.Fatalf ("Error: Expect more than 1 ReadOnMaster=true but get %d", count)
	} 

	count = testutil.RegexCountFile("HB ReadOnMaster", "hera.log")
	if count < 10 {
		t.Fatalf ("Error: Expect at least 10 HB ReadOnMaster but get %d", count)
	} 
	testutil.DoDefaultValidation(t);
	logger.GetLogger().Log(logger.Debug, "Test Heartbeat done  -------------------------------------------------------------")
}
