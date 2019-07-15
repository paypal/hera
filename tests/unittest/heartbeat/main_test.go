package main

import (
	//"fmt"
//	"os"
	"testing"
	"time"

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
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
	if racmaint {
		appcfg["rac_sql_interval"] = "3"
	} else {
		appcfg["rac_sql_interval"] = "0"
	}
	
	appcfg["db_heartbeat_interval"] = "5"

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


func TestHB(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestHB begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	time.Sleep(12* time.Second)
	hb_count := 3*2 // 3 connections, 2 hb check in 12 seconds.
	if hb_count != testutil.RegexCountFile("sending heartbeat to DB", "hera.log") {
		t.Fatalf("incorrect heartbeat count")
	}

	logger.GetLogger().Log(logger.Debug, "TestHB done  -------------------------------------------------------------")
}
