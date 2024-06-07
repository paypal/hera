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

	appcfg["config_logging_reload_time_hours"] = "1"
	appcfg["backlog_pct"] = "30"
	appcfg["enable_sharding"] = "true"

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

	if 4 == testutil.RegexCountFile("OCC_CONFIG", "cal.log") {
		t.Fatalf("Can't find OCC_CONFIG cal event")
	}

	time.Sleep(5 * time.Second)
	logger.GetLogger().Log(logger.Debug, "Test config-logging done  -------------------------------------------------------------")
}
