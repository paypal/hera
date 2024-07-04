package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
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
	appcfg["otel_resolution_time_in_sec"] = "3"
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

func TestOTELMetricsBasic(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestOTELMetricsBasic begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

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

	cancel()
	conn.Close()
	//Read OTEL log file for metrics validation
	logFilePath := filepath.Join(testutil.GetOTELLogDirPath(), "otel_collector.log")
	count := testutil.RegexCountFile("{\"key\":\"application\",\"value\":{\"stringValue\":\"hera-test\"}", logFilePath)
	if count < 1 {
		t.Fatalf("OTEL event should contain application as hera-test")
	}
	initCount := testutil.RegexCountFile("\"name\":\"pp.occ.init_connection.count\"", logFilePath)
	if initCount < 1 {
		t.Fatalf("OTEL event should contain metric name pp.occ.init_connection.count")
	}
	tagsCount := testutil.RegexCountFile("{\"key\":\"InstanceId\",\"value\":{\"intValue\":\"0\"}},{\"key\":\"ShardId\",\"value\":{\"intValue\":\"0\"}},{\"key\":\"WorkerType\",\"value\":{\"intValue\":\"0\"}",
		logFilePath)
	if tagsCount < 1 {
		t.Fatalf("mandatory tags InstanceId, ShardId, WorkerType should present")
	}
	azCount := testutil.RegexCountFile("{\"key\":\"az\",\"value\":{\"stringValue\":\"test-dev\"}", logFilePath)
	if azCount < 1 {
		t.Fatalf("az configured as test-dev and its value should present in otel metric dimension")
	}
	envCount := testutil.RegexCountFile("{\"key\":\"environment\",\"value\":{\"stringValue\":\"dev\"}", logFilePath)
	if envCount < 1 {
		t.Fatalf("az configured as test-dev and its value should present in otel metric dimension")
	}
	logger.GetLogger().Log(logger.Debug, "TestOTELMetricsBasic done  -------------------------------------------------------------")
}
