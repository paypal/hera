package main

import (
	
	"os"
	"testing"
	"database/sql"
	"context"
	"time"
	"fmt"
	"github.com/paypal/hera/client/gosqldriver"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// appcfg["x-mysql"] = "manual" // disable test framework spawning mysql server
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["db_heartbeat_interval"] = "10"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"]="5"

	appcfg["child.executable"] = "mysqlworker"

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}

	return appcfg, opscfg, testutil.MySQLWorker
}

var ip1 string
var dbName = "heratestdb"

func TestMain(m *testing.M) {
	// startup mysql DBs
	// ip1 := testutil.MakeDB("mysql33", dbName, testutil.MySQL)
	// os.Setenv("TWO_TASK", "tcp("+ip1+":3306)/"+dbName+"?timeout=11s")
	os.Exit(testutil.UtilMain(m, cfg, nil))
}

func TestClientInfoToWorkerDisabled(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestClientInfoToWorkerDisabled begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	time.Sleep(5*time.Second)
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	conn, err := db.Conn(ctx);
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	mux := gosqldriver.InnerConn(conn)
	mux.SetCalCorrID("583f5e4a2758e")
	err = mux.SetClientInfoWithPoolStack("testApplication", "localhost", "testApplication:testURL*CalThreadId=0*TopLevelTxnStartTime=18840b76115*Host=localhost*pid=100")
	if err != nil {
		t.Fatalf("Unable to set CLIENT_INFO")
	}
	rows, _ := conn.QueryContext(ctx, "SELECT version()")
	// rows, _ := stmt.Query(1)
	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	rows.Close()

	if testutil.RegexCountFile(".*GetConfig().EnableCmdClientInfoToWorker.*", "hera.log") > 0 {
		t.Fatalf("Error: should not have entered this block in coordinator")
	}

	if testutil.RegexCountFile("CmdClientInfo:.*", "hera.log") > 0 {
		t.Fatalf("Error: worker should not have got this command")
	}

	if testutil.RegexCountFile("CLIENT_INFO_MUX.*", "cal.log") > 0 {
		t.Fatalf("Error: Mux should not rename the CLIENT_INFO event")
	}

	cancel()
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestClientInfoToWorkerDisabled done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}