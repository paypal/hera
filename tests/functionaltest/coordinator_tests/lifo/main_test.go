package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*

The test will start Mysql server docker. Hera sever connects to this Mysql DB docker
No setup needed

*/

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	twoTask := os.Getenv("TWO_TASK")
        os.Setenv ("TWO_TASK_READ", twoTask)
        twoTask = os.Getenv("TWO_TASK_READ")
        fmt.Println ("TWO_TASK_READ: ", twoTask)

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["database_type"] = "mysql"

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

/*******************
 ** Validate default lifo configuration, lifo_scheduler_enabled="true"
 *******************/

func TestLIFO(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestLIFO begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12346, 'Jack', 100)")
	fmt.Println ("Load the row in test_simple_table_1 4 times")
	for  i := 0; i < 4; i++ {
		time.Sleep(2 * time.Second)
        	testutil.Fetch ("Select Name from test_simple_table_1 where ID = 12346");
	}

	time.Sleep(1 * time.Second)
	fmt.Println ("Verify default lifo is used when worker is assigned")
	if ( testutil.IsLifoUsed (t, "hera.log") == false ){
 		t.Fatalf("***Error: Expect LIFO used")
        }
	testutil.DoDefaultValidation(t);

	logger.GetLogger().Log(logger.Debug, "TestLIFO done  -------------------------------------------------------------")
}
