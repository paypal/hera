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
	appcfg["rac_sql_interval"] = "4"
	appcfg["db_heartbeat_interval"] = "2"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["database_type"] = "mysql"
	appcfg["readonly_children_pct"] = "50"
	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "4"
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
 ** Validate ReadWrite split."
 *******************/

func TestRWSplit(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "Test RW split begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML1("INSERT into test_simple_table_1 (ID, Name, Status) VALUES (123, 'Jack', 100)")
	testutil.Fetch  ("SELECT ID from TABLEINREPLICA");
	testutil.RunDML1("UPDATE test_simple_table_1 set Name = 'Mike' where ID = 123")
	testutil.RunDML1("INSERT into test_simple_table_1 (ID, Name, Status) VALUES (456, 'Helen', 100)")

	//make read instance writable. This will trigger recycle read connections
	err := testutil.UpdateDBState(os.Getenv("MYSQLR_IP"), "heratestdbr", testutil.MySQL, 0)
	logger.GetLogger().Log(logger.Debug, "MYSQLR_IP:" + os.Getenv("MYSQLR_IP"))
	if err != nil {
		logger.GetLogger().Log(logger.Debug, "UpdateDBState error ", err.Error())
	}

	testutil.Fetch  ("SELECT ID from TABLEINREPLICA");
	testutil.RunDML1("UPDATE test_simple_table_1 set Name = 'Jane' where ID = 456")


	// the test doesn't really setup replicating process
	time.Sleep(5 * time.Second)

	count := testutil.RegexCountFile("init=0&acpt=2", "cal.log")
	if count == 0 {
		t.Fatalf ("Error: no (healthy) read worker");
        }

	count  = testutil.RegexCountFile("INITDB	0", "cal.log")
	if count != 6 {
		t.Fatalf ("Error: INITDB count expects 4 but get %d", count)
	}

	count = testutil.RegexCountFile("CLIENT_SESSION$", "cal.log") 
	if count != 6 {
		t.Fatalf ("Error: CLIENT_SESSION expects 6 but get %d", count)
	}

	count = testutil.RegexCountFile("CLIENT_SESSION_R$", "cal.log") 
	if count != 4 {
		t.Fatalf ("Error: CLIENT_SESSION_R expects 4 but get %d", count)
	}

	count = testutil.RegexCountFile("[WORKER*SELECT", "hera.log") 
	if count > 0 {
		t.Fatalf( "Error: write worker runs SELECT query")
	}
	count = testutil.RegexCountFile("[R-WORKER*INSERT", "hera.log") 
	if count > 0 {
		t.Fatalf( "Error: write worker runs INSERT query")
	}

	testutil.DoDefaultValidation(t);
	logger.GetLogger().Log(logger.Debug, "Test RW split done  -------------------------------------------------------------")
}
