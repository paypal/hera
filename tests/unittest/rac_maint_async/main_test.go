package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
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
	appcfg["rac_sql_interval"] = "1"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "10"
	opscfg["opscfg.default.server.log_level"] = "5"

	//return appcfg, opscfg, testutil.OracleWorker
	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	os.Setenv("PARALLEL", "1")
	pfx := os.Getenv("MGMT_TABLE_PREFIX")
	if pfx == "" {
		pfx = "hera"
	}
	tableName = pfx + "_maint"
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestRacMaintWithRandomStatusChangeInAsync(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestRacMaintWithRandomStatusChangeInAsync begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	statusArray := []string{"U", "R", "F"}
	time.Sleep(5 * time.Second)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		for {
			status1 := rand.Intn(len(statusArray))
			status2 := rand.Intn(len(statusArray))
			var err error
			var conn *sql.Conn
			// cleanup and insert one row in the table
			conn, err = db.Conn(ctx)
			if err != nil {
				t.Fatalf("Error getting connection %s\n", err.Error())
			}
			tx, _ := conn.BeginTx(ctx, nil)
			stmt, _ := tx.PrepareContext(ctx, "/*cmd*/delete from "+tableName)
			_, err = stmt.Exec()
			if err != nil {
				t.Fatalf("Error preparing test (delete table) %s\n", err.Error())
			}
			stmt, _ = tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
			hostname, _ := os.Hostname()
			// how to do inst_id
			_, err = stmt.Exec(0 /*max instid*/, statusArray[status1], time.Now().Unix()+2, "hera-test", hostname)
			_, err = stmt.Exec(0, statusArray[status2], time.Now().Unix()+2, "hera-test_taf", hostname)
			if err != nil {
				t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
			}
			err = tx.Commit()
			if err != nil {
				t.Fatalf("Error commit %s\n", err.Error())
			}
			conn.Close()
			time.Sleep(1000 * time.Millisecond)
		}
	}()
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	time.Sleep(45000 * time.Millisecond)

	if 0 == testutil.RegexCountFile("Rac maint activating, worker", "hera.log") {
		t.Fatalf("requires rac maint activation for main module status")
	}

	if 0 == testutil.RegexCountFile("module:HERA-TEST_TAF", "cal.log") {
		t.Fatalf("Status 'U' should log the RACMAINT_INFO_CHANGE event")
	}
	if 0 != testutil.RegexCountFile("invalid_status", "cal.log") {
		t.Fatalf("ram maint status 'U' should not skip with invalid-status event")
	}

	logger.GetLogger().Log(logger.Debug, "TestRacMaintWithRandomStatusChangeInAsync done  -------------------------------------------------------------")
}
