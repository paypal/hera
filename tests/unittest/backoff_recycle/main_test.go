package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"
	"testing"

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
//	appcfg["x-mysql"] = "manual" // disable test framework spawning mysql server
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "2"
	appcfg["db_heartbeat_interval"] = "3"
	appcfg["max_desire_healthy_worker_pct"] = "90"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "100"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"]="4"

	appcfg["child.executable"] = "mysqlworker"

	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_hera_test"
	}
	return nil
}


func TestMain(m *testing.M) {
	// startup mysql DBs
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestBackOffRecycle(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestBackOffRecycle begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	_, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	time.Sleep(20*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err", "cal.log") > 1 {
		t.Fatalf("Error: should have INITDB as error ")
	}
	twotask :=os.Getenv("TWO_TASK")
	pwd :=os.Getenv("password")
// Incorrect TWO_TASK
	os.Setenv("TWO_TASK", "tcp(dummy:3306)/"+"test"+"?timeout=11s")
	time.Sleep(40*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err.*", "cal.log") < 1 {
		t.Fatalf("Error: should have INITDB as error ")
	}
	os.Setenv("TWO_TASK", twotask)
	os.Truncate("cal.log",0)
	time.Sleep(20*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err..*", "cal.log") > 1 {
		t.Fatalf("Error: should have INITDB as error ")
	}
// Incorrect Password
	os.Setenv("username","root")
	os.Setenv("password","sdasdas")
	time.Sleep(20*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err.*", "cal.log") < 1 {
		t.Fatalf("Error: should have INITDB as error ")
	}
	os.Setenv("password", pwd)
	os.Truncate("cal.log",0)
	time.Sleep(20*time.Second)
	init, _ := testutil.StatelogGetField(1)
	acpt, _ := testutil.StatelogGetField(2)
	if( init >  (acpt + init) * 30/100) {
		t.Fatalf("Error: should not have more than 30 percent workers in INIT")
	}
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err..*", "cal.log") > 1 {
		t.Fatalf("Error: should have INITDB as error ")
	}
// Incorrect username
	os.Setenv("username","root1")
	os.Setenv("password","sdasdas")
	time.Sleep(20*time.Second)
	init, _ = testutil.StatelogGetField(1)
	acpt, _ = testutil.StatelogGetField(2)
	if( init >  (acpt + init) * 30/100) {
		t.Fatalf("Error: should not have more than 30 percent workers in INIT")
	}
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err.*", "cal.log") < 1 {
		t.Fatalf("Error: should have INITDB as error ")
	}
	os.Setenv("username", "root")
	os.Setenv("password", pwd)
	os.Truncate("cal.log",0)
	time.Sleep(20*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err..*", "cal.log") > 1 {
		t.Fatalf("Error: should have INITDB as error ")
	}
}
