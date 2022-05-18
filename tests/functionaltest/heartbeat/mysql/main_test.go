package main

import (
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
	appcfg["child.executable"] = "mysqlworker"
	appcfg["database_type"] = "mysql"
	appcfg["readonly_children_pct"] = "50"
	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "2"
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

/*******************
 ** Validate heartbeat with MySQL worker (master-slave db setup). 
 *******************/
func TestHeartbeat(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "Test Heartbeat begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	/*
	Init state
	heratestdb write    1 write worker
	heratestdr readonly 1 read worker
	TWO_TASK heratestdb||heratestdbr
	TWO_TASK_READY heratestdbr || heratestdb
	
	change master to readonly, triggering write connections to recycle, backoff with short wait time
	heratestdb readonly 0 
	heratestdr readonly 1 read worker
	logger.GetLogger().Log(logger.Debug, "Update MYSQL_IP:" + os.Getenv("MYSQL_IP") + "heratestdb to readonly")
	*/
	err := testutil.UpdateDBState(os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL, 1)
	if err != nil {
		logger.GetLogger().Log(logger.Debug, "UpdateDBState error ", err.Error())
	}

	time.Sleep(3 * time.Second)

	/* 
	restore master writable.
	heratestdb write    1 write worker
	heratestdr readonly 1 read worker
	*/
	logger.GetLogger().Log(logger.Debug, "Update MYSQL_IP:" + os.Getenv("MYSQL_IP") + "heratestdb to writable")
	err = testutil.UpdateDBState(os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL, 0)
	if err != nil {
		logger.GetLogger().Log(logger.Debug, "UpdateDBState error ", err.Error())
	}

	time.Sleep(7* time.Second)
	/*
	set readonly db writable. 
	Observe read connection abort by HB and then retry all endpoints to find read node until fall back to last detected writable
	heratestdb write 1 write worker + 1 read worker
	heratestdr write 0
	*/
	logger.GetLogger().Log(logger.Debug, "Update MYSQLR_IP:" + os.Getenv("MYSQLR_IP") + "heratestdbr to writable")
	err = testutil.UpdateDBState(os.Getenv("MYSQLR_IP"), "heratestdbr", testutil.MySQL, 0)
	if err != nil {
		logger.GetLogger().Log(logger.Debug, "UpdateDBState error ", err.Error())
	}

	time.Sleep(7 * time.Second)

	count := testutil.RegexCountFile("Heartbeat error", "hera.log")
	if count != 2{
		t.Fatalf ("Error: Expect 2 HB error but get %d", count)
	}

	count  = testutil.RegexCountFile("retry-attempt", "hera.log")
	if count != 4 {
		t.Fatalf ("Error: INITDB count expects 4 but get %d", count)
	}

	count = testutil.RegexCountFile("ReadOnMaster=true", "hera.log")
	if count < 1 {
		t.Fatalf ("Error: Expect more than 1 ReadOnMaster=true but get %d", count)
	} 
	testutil.DoDefaultValidation(t);
	logger.GetLogger().Log(logger.Debug, "Test Heartbeat done  -------------------------------------------------------------")
}
