package main

import (
	"context"
	"database/sql"
	"fmt"
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
	appcfg["db_heartbeat_interval"] = "10"
	appcfg["enable_caching"] = "true"
	appcfg["caching_cfg_reload_interval"] = "60"
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
				"invalidation_clause varchar(1000),caching_enabled varchar(1), cache_by_corrid varchar(1), caching_enabled_apps varchar(4000), remarks varchar(4000),hera_module varchar(100))",
			os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestTTLCacheEnabledEmptyCacheCfg(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheEnabledEmptyCacheCfg begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	err := testutil.RunDML("DELETE from hera_caching")
	testutil.CheckError(err, t)

	time.Sleep(5 * time.Second)
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

	rows, err := conn.QueryContext(ctx, "SELECT version()")

	if err != nil {
		t.Fatalf("expected 1 row but received an unexpected error %v", err)
	}

	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	rows.Close()

	time.Sleep(5 * time.Second)

	if testutil.RegexCountFile("Loaded 0 sqlhashes, 0 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should not have cacheCfg entries...table is empty")
	}

	if testutil.RegexCountFile("2904134799 CachingEnabled for  GET : false", "hera.log") < 1 {
		t.Fatalf("Error: should have exited from CachingSession for GET")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for GET returned: sql is not enabled for caching", "hera.log") < 1 {
		t.Fatalf("Error: should have exited from CachingSession for GET")
	}

	if testutil.RegexCountFile("coordinator dispatchrequest", "hera.log") < 1 {
		t.Fatalf("Error: should have dispatched the request to database")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for SET returned: sql is not enabled for caching", "hera.log") < 1 {
		t.Fatalf("Error: should have exited from CachingSession for SET")
	}

	// Both the queries (cacheCfg query and the select query should be sent to the database)
	if testutil.RegexCountFile("T.*CLIENT_SESSION.*", "cal.log") < 2 {
		t.Fatalf("Error: both the requests should be sent to the database")
	}

	if testutil.RegexCountFile(".*\tGET\t.*", "cal.log") > 0 {
		t.Fatalf("Error: should not see GET when cacheCfg is empty")
	}

	if testutil.RegexCountFile(".*\tSET\t.*", "cal.log") > 0 {
		t.Fatalf("Error: should not see SET when cacheCfg is empty")
	}

	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheEnabledEmptyCacheCfg done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}
