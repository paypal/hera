package main

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"

	_ "github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*

The test will start Mysql server docker. Hera sever connects to this Mysql DB docker
No setup needed

*/

var mx testutil.Mux
var tableName string

var testCapacities = []struct {
	testSize int
}{
	{testSize: 1000},
	{testSize: 2000},
	{testSize: 10000},
	{testSize: 65536},
	{testSize: 131072},
}

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["lifo_scheduler_enabled"] = "false"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["database_type"] = "mysql"
	appcfg["debug_mux"] = "true"

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
func BenchmarkTestFIFO(b *testing.B) {
	logger.GetLogger().Log(logger.Debug, "Benchmark TestFIFO begins +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	for _, capacity := range testCapacities {
		b.Run(fmt.Sprintf("bench_input_size_%d", capacity.testSize), func(b *testing.B) {
			var idsMap map[int]bool = map[int]bool{}

			for i := 0; i < capacity.testSize; i++ {
				id := testutil.RangeInt(10000, 99999)
				status := testutil.RangeInt(50, 100)
				_, ok := idsMap[id]

				for ok {
					id = testutil.RangeInt(10000, 99999)
					_, ok = idsMap[id]
				}
				idsMap[id] = true
				name := fmt.Sprintf("%s_%d", "Jack", id)
				query := fmt.Sprintf("insert into test_simple_table_1 (ID, Name, Status) VALUES (%d, %s, %d)", id, name, status)
				testutil.RunDML1(query)
			}

			fmt.Println("Load the row in test_simple_table_1 2 times")
			IdsList := reflect.ValueOf(idsMap).MapKeys()

			for i := 0; i < capacity.testSize; i++ {
				var queryID int = IdsList[rand.Intn(len(IdsList))].Interface().(int)
				testutil.Fetch(fmt.Sprintf("Select Name from test_simple_table_1 where ID = %d", queryID))
			}
		})
	}

	logger.GetLogger().Log(logger.Info, "Benchmark TestFIFO done  -------------------------------------------------------------")
}
