package main

import (
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
	"os"
	"strings"
	"testing"
	"time"
)

var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["db_heartbeat_interval"] = "20"
	appcfg["enable_caching"] = "true"
	appcfg["caching_cfg_reload_interval"] = "5"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"] = "500"

	appcfg["child.executable"] = "mysqlworker"

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}

	return appcfg, opscfg, testutil.MySQLWorker
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "hera_caching"
	}
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		// mysql
		err := testutil.DBDirect(
			"create table hera_caching(query_id varchar(30),sqlhash varchar(40),sqltext varchar(4000),"+
				"bind_variables varchar(1000),TTL_sec BIGINT,enable_shadow_test varchar(1),tableName varchar(30),"+
				"invalidation_clause varchar(1000),caching_enabled varchar(1),cache_by_corrid varchar(1), caching_enabled_apps varchar(4000),remarks varchar(4000),hera_module varchar(100))",
			os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
func TestCacheCfgReload(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestCacheCfgReload begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	err := testutil.RunDML("DELETE from hera_caching")
	testutil.CheckError(err, t)

	time.Sleep(10 * time.Second)

	if testutil.RegexCountFile("Loaded 0 sqlhashes, 0 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should not have cacheCfg entries...table is empty")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 0", "hera.log") < 1 {
		t.Fatalf("Error: should not have cacheCfg entries...table is empty")
	}

	err = testutil.RunDML("INSERT into hera_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, cache_by_corrid, caching_enabled_apps, remarks, hera_module) VALUES  ('1', '1774480566', 'MyTestQuery', 'abc=123', 30, 'N', 'MyTestTable', '', 'Y', 'Y', 'all','', 'hera-test')")
	testutil.CheckError(err, t)

	err = testutil.RunDML("INSERT into hera_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, cache_by_corrid, caching_enabled_apps, remarks, hera_module) VALUES  ('1', '1774480567', 'MyTestQuery', 'abc=123', 30, 'N', 'MyTestTable', '', 'Y', 'Y', 'all','', 'hera-test')")
	testutil.CheckError(err, t)

	time.Sleep(10 * time.Second)

	if testutil.RegexCountFile("Loaded 2 sqlhashes, 2 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should have two cacheCfg entries...")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 2", "hera.log") < 1 {
		t.Fatalf("Error: should have two cacheCfg entries...")
	}

	err = testutil.RunDML("DELETE from hera_caching WHERE sqlhash = '1774480567'")
	testutil.CheckError(err, t)

	time.Sleep(10 * time.Second)

	if testutil.RegexCountFile("Loaded 1 sqlhashes, 1 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should have reloaded and have just one cacheCfg entry...")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 1", "hera.log") < 1 {
		t.Fatalf("Error: should have reloaded and have just one cacheCfg entry...")
	}

	logger.GetLogger().Log(logger.Debug, "TestCacheCfgReload done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}
