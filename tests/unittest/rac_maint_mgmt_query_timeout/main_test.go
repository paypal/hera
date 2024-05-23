package main

import (
	"database/sql"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
	"os"
	"testing"
	"time"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "1"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	appcfg["management_queries_timeout_us"] = "200"

	//return appcfg, opscfg, testutil.OracleWorker
	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	os.Setenv("PARALLEL", "1")
	pfx := os.Getenv("MGMT_TABLE_PREFIX")
	if pfx == "" {
		pfx = "hera"
	}
	tableName = pfx + "_maint"
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestRacMaintWithWithTimeout(t *testing.T) {

	logger.GetLogger().Log(logger.Debug, "TestRacMaintWithWithTimeout begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	time.Sleep(16 * time.Second)
	hostname, _ := os.Hostname()
	db, err := sql.Open("heraloop", hostname+":31002")
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	time.Sleep(2 * time.Second)
	out := testutil.RegexCountFile("rac maint for shard = 0 ,err : context deadline exceeded", "hera.log")
	if out < 1 {
		err = nil
		t.Fatalf("rac maint management query should fail with context timeout")
	}

	logger.GetLogger().Log(logger.Debug, "TestRacMaintWithWithTimeout done  -------------------------------------------------------------")
}
