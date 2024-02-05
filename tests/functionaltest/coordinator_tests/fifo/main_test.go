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

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["rac_sql_interval"] = "0"
	appcfg["lifo_scheduler_enabled"] = "false"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["database_type"] = "mysql"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "8"
	opscfg["opscfg.default.server.log_level"] = "5"
	if os.Getenv("WORKER") == "postgres" {
                return appcfg, opscfg, testutil.PostgresWorker
        } 

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
        testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
	if os.Getenv("WORKER") == "postgres" {
        	return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID NUMERIC PRIMARY KEY, NAME VARCHAR(128), STATUS NUMERIC, PYPL_TIME_TOUCHED NUMERIC)")
        } 
       	return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*******************
 ** Validate default lifo configuration, lifo_scheduler_enabled="true"
 *******************/

func TestFIFO(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestFIFO begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12346, 'Jack', 100)")
	fmt.Println ("Load the row in test_simple_table_1 2 times")
	for  i := 0; i < 2; i++ {
        	testutil.Fetch ("Select Name from test_simple_table_1 where ID = 12346");
	}

	time.Sleep(1 * time.Second)
	fmt.Println ("Verify fifo worker assign algorithm is used")
	if ( testutil.IsLifoUsed (t, "hera.log") == true ){
 		t.Fatalf("***Error: Expected FIFO used")
        }
	testutil.DoDefaultValidation(t);

	logger.GetLogger().Log(logger.Debug, "TestFIFO done  -------------------------------------------------------------")
}
