package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*
Running the test
go install github.com/paypal/hera/tests/mocksqlsrv/runserver
$GOROOT/bin/go install  .../worker/{mysql,oracle}worker
$GOROOT/bin/go test -c .../tests/unittest/mysql_recycle && ./mysql_recycle.test
*/

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["child.executable"] = "mysqlworker"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

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
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestMysqlRecycle(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestMysqlRecycle begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// cleanup and insert one row in the table
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	// no such table error - non fatal
	stmt, _ := conn.PrepareContext(ctx, "select id, /*mockErr1146*/ int_val from "+tableName+" where id=?")
	stmt.Query(1)
	if testutil.RegexCount("1146") < 1 {
		t.Fatal("not enough 1146 error codes in logs, probably did not get the error")
	}
	if testutil.RegexCount("==== worker exits") >= 1 {
		t.Fatal("worker should not exit on 1146 non fatal error")
	}

	// error read fr pipe - fatal
	stmt, _ = conn.PrepareContext(ctx, "select id, /*mockErr1154*/ int_val from "+tableName+" where id=?")
	stmt.Query(1)
	if testutil.RegexCount("1154") < 1 {
		t.Fatal("not enough 1154 error codes in logs, probably did not get the error")
	}
	if testutil.RegexCount("==== worker exits") < 1 {
		t.Fatal("worker should exit on 1154 fatal error")
	}

	cancel() // conn ctx
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestMysqlRecycle done  -------------------------------------------------------------")
}
