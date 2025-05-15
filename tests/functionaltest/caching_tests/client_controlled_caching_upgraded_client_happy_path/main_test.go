package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/paypal/hera/client/gosqldriver"
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
	cacheCertsPath, ok := os.LookupEnv("CACHE_CERTS_PATH")
	if !ok {
		cacheCertsPath, _ = os.Getwd()
	}
	appcfg["cache_cert_file_path"] = cacheCertsPath
	appcfg["cache_endpoint"] = fmt.Sprintf("%s:5080", cacheHost)
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["db_heartbeat_interval"] = "10"
	appcfg["enable_caching"] = "false"
	appcfg["caching_cfg_reload_interval"] = "60"
	appcfg["cache_response_timeout_ms"] = "3000"
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

func populateCache() error {

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		return err
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	mux := gosqldriver.InnerConn(conn)
	mux.SetCalCorrID("5af5e4a2758e")
	err = mux.SetClientInfoWithPayload("testApplication", "localhost", "ClientSupportedProtocolVersions: 2")
	if err != nil {
		return err
	}
	rows, _ := conn.QueryContext(ctx, "SELECT 'pqr' from dual")

	if !rows.Next() {
		return fmt.Errorf("Expected 1 row")
	}
	rows.Close()
	cancel()
	conn.Close()
	return err
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
		testutil.DBDirect("create table hera_caching(query_id varchar(30),sqlhash varchar(40),sqltext varchar(4000),"+
			"bind_variables varchar(1000),TTL_sec BIGINT,enable_shadow_test varchar(1),tableName varchar(30),"+
			"invalidation_clause varchar(1000),caching_enabled varchar(1),cache_by_corrid varchar(1), caching_enabled_apps varchar(4000),remarks varchar(4000),hera_module varchar(100))", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)
	}
	return nil
}

// GET (Cache MISS) + SET
func TestClientControlledCacheUpgradedClientHappyPath(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestClientControlledCacheUpgradedClientHappyPath begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML("DELETE from hera_caching")
	testutil.RunDML("INSERT into hera_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, cache_by_corrid, caching_enabled_apps, remarks, hera_module) VALUES  ('1', '3029497934', 'MyTestQuery', 'abc=123', 30, 'N', 'MyTestTable', '', 'Y', 'N', 'all','', 'hera-test')")
	time.Sleep(10 * time.Second)

	if testutil.RegexCountFile("Loaded 1 sqlhashes, 1 cacheCfg entries", "hera.log") > 0 {
		t.Fatalf("Error: should not have loaded the cacheCfg entry when caching is disabled...")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 1", "hera.log") > 0 {
		t.Fatalf("Error: should not have loaded the cacheCfg entry...")
	}

	err := populateCache()

	if err != nil {
		t.Fatal("Error:", err)
		return
	}

	time.Sleep(10 * time.Second)

	if testutil.RegexCountFile(".*sendCacheResponseMetadata.*", "cal.log") > 0 {
		t.Fatalf("Error: should not see sendCacheResponseMetadata event")
	}

	if testutil.RegexCountFile("server info:.*ServerSupportedProtocolVersion: 2", "hera.log") > 0 {
		t.Fatalf("Error: should not respond with ServerSupportedProtocolVersion")
	}

	if testutil.RegexCountFile("PreprocessCaching:.*", "hera.log") > 0 {
		t.Fatalf("Error: should not get into PreprocessCaching when caching is disabled")
	}

	if testutil.RegexCountFile("isClientControlledCachingRequest.*rewrite", "hera.log") > 0 {
		t.Fatalf("Error: should not rewrite request when caching is disabled")
	}

	if testutil.RegexCountFile("3029497934 CachingEnabled for  GET : true", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for GET returned: error: no key", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block")
	}

	if testutil.RegexCountFile("Trying SET with key", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered setRecordToCache when caching is disabled")
	}

	// GET should fail with no key
	if testutil.RegexCountFile(".*GET\t3029497934\t2.*", "cal.log") > 0 {
		t.Fatalf("Error: should not see GET when caching is disabled")
	}

	if testutil.RegexCountFile(".*SET\t3029497934\t0.*", "cal.log") > 0 {
		t.Fatalf("Error: should not see SET when caching is disabled")
	}

	if testutil.RegexCountFile(".*EXEC\t3029497934\t0.*", "cal.log") < 1 {
		t.Fatalf("Error: query should be sent to the database")
	}

	if testutil.RegexCountFile("Before ResponseMetadata: 1:6,", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block when caching is disabled")
	}

	if testutil.RegexCountFile("After ResponseMetadata: 6:6 1020,", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block when caching is disabled")
	}

	time.Sleep(2 * time.Second)

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	mux := gosqldriver.InnerConn(conn)
	mux.SetCalCorrID("5af5e4a2758e")

	err = mux.SetClientInfoWithPayload("testApplication", "localhost", "ClientSupportedProtocolVersions: 2")
	if err != nil {
		t.Fatalf("Unable to set CLIENT_INFO")
	}

	rows, _ := conn.QueryContext(ctx, "SELECT 'pqr' from dual")

	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	rows.Close()

	time.Sleep(3 * time.Second)

	if testutil.RegexCountFile("server info:.*ServerSupportedProtocolVersion: 2", "hera.log") > 0 {
		t.Fatalf("Error: should not respond with ServerSupportedProtocolVersion")
	}

	if testutil.RegexCountFile(".*sendCacheResponseMetadata.*", "cal.log") > 0 {
		t.Fatalf("Error: should not see sendCacheResponseMetadata event...")
	}

	if testutil.RegexCountFile("PreprocessCaching:.*", "hera.log") > 0 {
		t.Fatalf("Error: should not get into PreprocessCaching when caching is disabled")
	}

	if testutil.RegexCountFile("isClientControlledCachingRequest.*rewrite", "hera.log") > 0 {
		t.Fatalf("Error: should not rewrite request when caching is disabled")
	}

	if testutil.RegexCountFile(".*GET\t3029497934\t0.*", "cal.log") > 0 {
		t.Fatalf("Error: should not see GET when caching is disabled")
	}

	if testutil.RegexCountFile(".*EXEC\t3029497934\t0.*", "cal.log") < 2 {
		t.Fatalf("Error: query should be sent to the database")
	}

	if testutil.RegexCountFile("Before ResponseMetadata: 1:6,", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block...")
	}

	if testutil.RegexCountFile("After ResponseMetadata: 6:6 1020,", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block...")
	}

	mux.SetCalCorrID("5af5e4a2758e")
	// Re-use same connection
	rows, _ = conn.QueryContext(ctx, "SELECT 'pqr' from dual")

	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	rows.Close()
	time.Sleep(3 * time.Second)

	if testutil.RegexCountFile("PreprocessCaching:.*", "hera.log") > 0 {
		t.Fatalf("Error: should not get into PreprocessCaching when caching is disabled")
	}

	if testutil.RegexCountFile("isClientControlledCachingRequest.*rewrite", "hera.log") > 0 {
		t.Fatalf("Error: should not rewrite request when caching is disabled")
	}

	if testutil.RegexCountFile("3029497934 CachingEnabled for  GET : true", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block")
	}

	if testutil.RegexCountFile("Trying GET with key", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered getRecordFromCache when caching is disabled")
	}

	if testutil.RegexCountFile(".*GET\t3029497934\t0.*", "cal.log") > 0 {
		t.Fatalf("Error: should not see GET when caching is disabled")
	}

	if testutil.RegexCountFile("Before ResponseMetadata: 1:6,", "hera.log") > 0 {
		t.Fatalf("Error: should have entered this block when caching is disabled")
	}

	if testutil.RegexCountFile("After ResponseMetadata: 6:6 1020,", "hera.log") > 0 {
		t.Fatalf("Error: should have entered this block when caching is disabled")
	}

	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestClientControlledCacheUpgradedClientHappyPath done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}
