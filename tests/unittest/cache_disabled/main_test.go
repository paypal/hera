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
	appcfg["enable_caching"] = "false"
	appcfg["caching_cfg_reload_interval"] = "2"

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
		err := testutil.DBDirect(
			"create table hera_caching(query_id varchar(30),sqlhash varchar(40),sqltext varchar(4000),"+
				"bind_variables varchar(1000),TTL_sec BIGINT,enable_shadow_test varchar(1),tableName varchar(30),"+
				"invalidation_clause varchar(1000),caching_enabled varchar(1),remarks varchar(4000),hera_module varchar(100))",
			os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestTTLCacheDisabled(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheDisabled begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	time.Sleep(5 * time.Second)
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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

	if testutil.RegexCountFile("Begin loading CacheCfg", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block! Caching is disabled")
	}

	if testutil.RegexCountFile("InitCachingCfg for module", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block! Caching is disabled")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block! Caching is disabled")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block! Caching is disabled")
	}
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheDisabled done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}
