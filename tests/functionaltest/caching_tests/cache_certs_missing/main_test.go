package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
	"os"
	"strings"
	"testing"
	"time"
)

var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	cacheHost, ok := os.LookupEnv("CACHE_HOST")
	if !ok {
		cacheHost = "localhost"
	}
	appcfg["cache_endpoint"] = fmt.Sprintf("%s:5080", cacheHost) //Point to incorrect endpoint to simulate connection refused errors
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["db_heartbeat_interval"] = "10"
	appcfg["enable_caching"] = "true"
	appcfg["caching_cfg_reload_interval"] = "60"
	appcfg["cache_cert_file_path"] = "/home/"
	appcfg["cache_response_timeout_ms"] = "3000"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"] = "5"

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

func TestTTLCacheJunoCertsMissing(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheJunoCertsMissing begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML("DELETE from hera_caching")
	testutil.RunDML("INSERT into hera_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, cache_by_corrid, caching_enabled_apps, remarks, hera_module) VALUES  ('1', '2904134799', 'MyTestQuery', 'abc=123', 30, 'N', 'MyTestTable', '', 'Y', 'N', 'all', '', 'hera-test')")

	time.Sleep(5 * time.Second)

	if testutil.RegexCountFile("Loaded 1 sqlhashes, 1 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should have loaded the cacheCfg entry...")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 1", "hera.log") < 1 {
		t.Fatalf("Error: should have loaded the cacheCfg entry...")
	}

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	rows, _ := conn.QueryContext(ctx, "SELECT version()")

	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	rows.Close()
	time.Sleep(3 * time.Second)
	if testutil.RegexCountFile("2904134799 CachingEnabled for  GET : true", "hera.log") < 1 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile(".*Error in junoclient.*", "hera.log") < 1 {
		t.Fatalf("Error: should see an error in junoclient::GetTLSConfig")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for GET returned:.*tls.*", "hera.log") < 1 {
		t.Fatalf("Error: should have exited from CachingSession for GET with bad cert")
	}

	if testutil.RegexCountFile("coordinator dispatchrequest", "hera.log") < 1 {
		t.Fatalf("Error: should have dispatched the request to database")
	}

	if testutil.RegexCountFile("Error in setRecordToCache:.*tls.*", "hera.log") < 1 {
		t.Fatalf("Error: should have exited from CachingSession for SET with bad cert")
	}

	if testutil.RegexCountFile(".*EXEC\t2904134799.*", "cal.log") < 1 {
		t.Fatalf("Error: query should be sent to the database")
	}

	if testutil.RegexCountFile(".*GET\t2904134799\t2", "cal.log") < 1 {
		t.Fatalf("Error: GET should have failed")
	}

	if testutil.RegexCountFile(".*SET\t2904134799\t2", "cal.log") < 1 {
		t.Fatalf("Error: SET should have failed")
	}
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheJunoCertsMissing done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}
