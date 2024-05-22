package main

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {
	fmt.Println("setup() begin")
	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["rac_sql_interval"] = "0"
	appcfg["enable_query_bind_blocker"] = "true"
	appcfg["management_queries_timeout_us"] = "400"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}
	return appcfg, opscfg, testutil.MySQLWorker
}

func teardown() {
	mx.StopServer()
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, nil))
}

func TestQueryBindBlockerWithTimeout(t *testing.T) {
	testutil.DBDirect("create table hera_rate_limiter (herasqlhash numeric not null, herasqltext varchar(4000) not null, bindvarname varchar(200) not null, bindvarvalue varchar(200) not null, blockperc numeric not null, heramodule varchar(100) not null, end_time numeric not null, remarks varchar(200) not null)", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)

	logger.GetLogger().Log(logger.Debug, "TestQueryBindBlockerWithTimeout begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
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
	out := testutil.RegexCountFile("loading query bind blocker: context deadline exceeded", "hera.log")
	if out < 1 {
		err = nil
		t.Fatalf("query bind blocker management query should fail with context timeout")
	}

	logger.GetLogger().Log(logger.Debug, "TestQueryBindBlockerWithTimeout done  -------------------------------------------------------------")

}
