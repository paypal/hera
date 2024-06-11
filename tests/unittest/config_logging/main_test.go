package main

import (
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
	"time"

	//"fmt"
	//	"os"
	"testing"
)

var mx testutil.Mux
var racmaint bool

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["config_logging_reload_time_hours"] = "0.01"
	appcfg["enable_sharding"] = "true"
	appcfg["enable_taf"] = "true"
	appcfg["readonly_children_pct"] = "30"
	appcfg["saturation_recover_throttle_rate"] = "40"
	appcfg["soft_eviction_effective_time"] = "10000"
	appcfg["bind_eviction_threshold_pct"] = "60"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	return nil
}

func TestMain(m *testing.M) {
	//os.Exit(testutil.UtilMain(m, cfg, before))
	racmaint = false
	testutil.UtilMain(m, cfg, before)
}

func TestConfigLogging(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "Test config-logging begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	if testutil.RegexCountFile("OCC_CONFIG", "cal.log") < 6 {
		t.Fatalf("Can't find OCC_CONFIG cal event")
	}

	if testutil.RegexCountFile("BACKLOG", "cal.log") < 1 {
		t.Fatalf("All or some BACKLOG config-logs are missing")
	}

	if testutil.RegexCountFile("SHARDING", "cal.log") < 1 {
		t.Fatalf("All or some sharding config-logs are missing")
	}

	if testutil.RegexCountFile("TAF", "cal.log") < 1 {
		t.Fatalf("All or some TAF config-logs are missing")
	}

	if testutil.RegexCountFile("R-W-SPLIT", "cal.log") < 1 {
		t.Fatalf("All or some R-W-SPLIT config-logs are missing")
	}

	if testutil.RegexCountFile("SOFT-EVICTION", "cal.log") < 1 {
		t.Fatalf("All or some SOFT-EVICTION config-logs are missing")
	}

	if testutil.RegexCountFile("BIND-EVICTION", "cal.log") < 1 {
		t.Fatalf("All or some BIND-EVICTION config-logs are missing")
	}

	time.Sleep(5 * time.Second)
	logger.GetLogger().Log(logger.Debug, "Test config-logging done  -------------------------------------------------------------")
}