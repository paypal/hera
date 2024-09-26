package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"syscall"
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
	os.Setenv("AVAILABILITY_ZONE", "test-dev")
	os.Setenv("ENVIRONMENT", "dev")
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
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		// mysql
		testutil.RunDML("create table jdbc_hera_test ( ID BIGINT, INT_VAL BIGINT, STR_VAL VARCHAR(500))")
	}
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestCoordinatorWithShutdownCleanup(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestCoordinatorWithShutdownCleanup begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	go func() {
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
		time.Sleep(500 * time.Millisecond)
		if err != nil {
			t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
		}
		err = tx.Commit()

		stmt.Close()

		cancel()
		conn.Close()
	}()

	time.Sleep(200 * time.Millisecond)
	// send SIGTERM signal to mux process
	proc, _ := os.FindProcess(os.Getpid())
	proc.Signal(syscall.SIGTERM)
	if testutil.RegexCountFile("Got SIGTERM", "hera.log") != 1 {
		t.Fatalf("workerbroker should see SIGTERM signal start perform cleanup")
	}

	//workerclient pid= 768  to be terminated, sending SIGTERM first for gracefull termination
	if testutil.RegexCountFile("workerclient pid=(\\s*\\d+)  to be terminated, sending SIGTERM first for gracefull termination", "hera.log") < 1 {
		t.Fatalf("workerbroker should send graceful termination to workers")
	}
	logger.GetLogger().Log(logger.Debug, "TestCoordinatorWithShutdownCleanup done  -------------------------------------------------------------")
}
