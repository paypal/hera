package main

import (
	"context"
	"database/sql"
	"fmt"
	speedbump "github.com/kffl/speedbump/lib"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/paypal/hera/client/gosqldriver"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var tableName string
var sb *speedbump.Speedbump
var destAddr string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["db_heartbeat_interval"] = "10"
	appcfg["enable_caching"] = "true"
	appcfg["caching_cfg_reload_interval"] = "5"
	//appcfg["idle_timeout_ms"] = "1000"
	appcfg["cache_connection_pool_size"] = "10"
	appcfg["cache_response_timeout_ms"] = "25000"
	appcfg["cache_endpoint"] = fmt.Sprintf("%s:%d", "localhost", 9090)
	appcfg["cache_ssl_enabled"] = "false"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.idle_timeout_ms"] = "100"
	opscfg["opscfg.default.server.max_connections"] = "5"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"] = "500"

	appcfg["child.executable"] = "mysqlworker"

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}

	return appcfg, opscfg, testutil.MySQLWorker
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
		err = testutil.DBDirect(
			"INSERT into hera_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, cache_by_corrid, caching_enabled_apps, remarks, hera_module) VALUES  ('1', '1397234510', 'MyTestQuery', 'abc=123', 120, 'N', 'MyTestTable', '', 'Y', 'Y', 'all', '', 'hera-test')",
			os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func populateCache() error {

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		return err
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	mux := gosqldriver.InnerConn(conn)
	mux.SetCalCorrID("5af5e4a2758e")

	rows, err := conn.QueryContext(ctx, "SELECT 'xyz' from dual")
	if err != nil || !rows.Next() {
		return fmt.Errorf("Expected 1 row")
	}
	rows.Close()
	conn.Close()
	return err
}

func TestMain(m *testing.M) {
	cacheHost, ok := os.LookupEnv("CACHE_HOST")
	if !ok {
		cacheHost = "localhost"
	}
	destAddr = fmt.Sprintf("%s:%d", cacheHost, 8080)
	sb, _ = testutil.StartSpeedBumpProxy(9090, destAddr, 5000, 5000, 5)
	err := sb.Start()
	if err != nil {
		os.Exit(-1)
	}
	defer sb.Stop()
	os.Exit(testutil.UtilMain(m, cfg, before))
}

// GET (Cache MISS) + SET
func TestCacheSessionClientDisconnectWithTimeout(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestCacheSessionClientDisconnectWithTimeout begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	time.Sleep(6 * time.Second)
	if testutil.RegexCountFile("Loaded 1 sqlhashes, 1 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should have loaded the cacheCfg entry...")
	}

	err := populateCache()

	if err != nil {
		t.Fatal("Error populating cache:", err)
		return
	}
	time.Sleep(5 * time.Second)
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	mux := gosqldriver.InnerConn(conn)
	mux.SetCalCorrID("5af5e4a2758e")

	conn.QueryContext(ctx, "SELECT 'xyz' from dual")
	// if err != nil {
	// 	t.Fatalf("Error QueryContext %s\n", err.Error())
	// 	return
	// }
	conn.Close()
	time.Sleep(5 * time.Second)
	if testutil.RegexCountFile("1397234510 CachingEnabled for  GET : true", "hera.log") < 2 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("coordinator doCacheRequest: starting", "hera.log") < 2 {
		t.Fatalf("Error: should have entered doCacheRequest when caching is enabled")
	}

	if testutil.RegexCountFile("Trying GET with key", "hera.log") < 2 {
		t.Fatalf("Error: should have entered getRecordFromCache when caching is enabled")
	}

	if testutil.RegexCountFile("doCacheRequest: request canceled", "hera.log") < 1 {
		t.Fatalf("Error: client request should be canceled... Should be captured by cache session")
	}

	if testutil.RegexCountFile(".*doCacheRequest\tclient_req_canceled.*", "cal.log") < 1 {
		t.Fatalf("Error: should see client_req_canceled event when request is canceled")
	}

	// cancel()
	// conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestCacheSessionClientDisconnectWithTimeout done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}
