package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
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
	appcfg["rac_sql_interval"] = "0"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["enable_otel"] = "true"
	appcfg["otel_resolution_time_in_sec"] = "10"
	appcfg["otel_agent_metrics_uri"] = "v2/metrics"
	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	os.Setenv("AVAILABILITY_ZONE", "test-dev")
	os.Setenv("ENVIRONMENT", "dev")
	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_hera_test"
	}
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		// mysql
		testutil.RunDML("create table jdbc_hera_test ( ID BIGINT, INT_VAL BIGINT, STR_VAL VARCHAR(500))")
	}
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestOTELMetricsIncorrectEndPoint(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestOTELMetricsIncorrectEndPoint begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

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
	tx, _ := conn.BeginTx(ctx, nil)
	sqlTxt := "/*cmd*/delete from " + tableName
	stmt, _ := tx.PrepareContext(ctx, sqlTxt)
	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("Error preparing test (delete table) %s with %s ==== sql\n", err.Error(), sqlTxt)
	}

	stmt, _ = tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (id, int_val, str_val) VALUES(?, ?, ?)")
	_, err = stmt.Exec(1, time.Now().Unix(), "val 1")
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	stmt, _ = conn.PrepareContext(ctx, "/*cmd*/Select id, int_val from "+tableName+" where id=?")
	rows, _ := stmt.Query(1)
	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}

	time.Sleep(10 * time.Second)
	rows.Close()
	stmt.Close()

	time.Sleep(10 * time.Second)
	publishingErrors := testutil.RegexCountFile("otel publishing error", "hera.log")
	if publishingErrors < 2 {
		t.Fatalf("otel publishing error should present in log because of in-correct OTEL port number")
	}

	time.Sleep(10 * time.Second)
	calPublishingErrors := testutil.RegexCountFile("failed to send metrics", "cal.log")
	if calPublishingErrors < 1 {
		t.Fatalf("otel publishing error should present in CAL log because of in-correct OTEL port number")
	}
	if calPublishingErrors > 1 {
		t.Fatalf("otel runtime errors logging interval is 60 secs, we should not see more than one error in CAL")
	}
	cancel()
	conn.Close()
	logger.GetLogger().Log(logger.Debug, "TestOTELMetricsIncorrectEndPoint done  -------------------------------------------------------------")
}
