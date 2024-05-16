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

func TestQueryBindBlockerTableNotExistOrEmpty(t *testing.T) {
	testutil.RunDML("DROP TABLE IF EXISTS hera_rate_limiter")

	logger.GetLogger().Log(logger.Debug, "TestQueryBindBlockerTableNotExistOrEmpty begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	time.Sleep(6 * time.Second)
	if testutil.RegexCountFile("loading query bind blocker: SQL error: Error 1146", "hera.log") == 0 {
		t.Fatalf("expected to see table 'hera_rate_limiter' doesn't exist error")
	}

	testutil.RunDML("create table hera_rate_limiter (herasqlhash numeric not null, herasqltext varchar(4000) not null, bindvarname varchar(200) not null, bindvarvalue varchar(200) not null, blockperc numeric not null, heramodule varchar(100) not null, end_time numeric not null, remarks varchar(200) not null)")
	time.Sleep(15 * time.Second)
	if testutil.RegexCountFile("Loaded 0 sqlhashes, 0 entries, query bind blocker entries", "hera.log") == 0 {
		t.Fatalf("expected to 0 entries from hera_rate_limiter table")
	}
	logger.GetLogger().Log(logger.Debug, "TestQueryBindBlockerTableNotExistOrEmpty ends +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}
