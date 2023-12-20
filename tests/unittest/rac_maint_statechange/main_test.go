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
	opscfg["opscfg.default.server.max_connections"] = "3"
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

func TestRacMaintWithBothModulesAndStateChange(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestRacMaintWithBothModulesAndStateChange begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	testutil.ClearLogsData()
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// cleanup and insert one row in the table
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	defer conn.Close()
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*cmd*/delete from "+tableName)
	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("Error preparing test (delete table) %s\n", err.Error())
	}
	stmt, _ = tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
	hostname, _ := os.Hostname()
	// how to do inst_id
	_, err = stmt.Exec(0 /*max instid*/, "F", time.Now().Unix()+2, "hera-test", hostname)
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)
	recycleWorkersCount := testutil.RegexCountFile("Rac maint activating, worker", "hera.log")
	if 0 == recycleWorkersCount {
		t.Fatalf("requires rac maint activation for main module for status 'R'")
	}
	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}
	insS, err := tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
	if err != nil {
		t.Fatalf("Error prep ins %s\n", err.Error())
	}
	// mysql uses instId 0 since there isn't instid's
	timeInMillis := time.Now().Unix() + 1
	insS.Exec(0, "U", timeInMillis, "hera-test", hostname)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)
	if recycleWorkersCount != testutil.RegexCountFile("Rac maint activating, worker", "hera.log") {
		t.Fatalf("should not trigger additional rac maint activation for main module for status 'U'")
	}
	if 2 == testutil.RegexCountFile("RACMAINT_INFO_CHANGE", "cal.log") {
		t.Fatalf("We have 2 insert queries with different status, so should log the RACMAINT_INFO_CHANGE event for every change")
	}
	if 0 != testutil.RegexCountFile("invalid_status", "cal.log") {
		t.Fatalf("ram maint status 'U' should not skip with invalid-status event")
	}
	logger.GetLogger().Log(logger.Debug, "TestRacMaintWithBothModulesAndStateChange done  -------------------------------------------------------------")
}

func TestRacMaintWithBothModulesAndStateChangeFromR(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestRacMaintWithBothModulesAndStateChange2 begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	testutil.ClearLogsData()
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// cleanup and insert one row in the table
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	defer conn.Close()
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*cmd*/delete from "+tableName)
	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("Error preparing test (delete table) %s\n", err.Error())
	}
	stmt, _ = tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
	hostname, _ := os.Hostname()
	// how to do inst_id
	_, err = stmt.Exec(0 /*max instid*/, "R", time.Now().Unix()+2, "hera-test", hostname)
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)
	recycleWorkersCount := testutil.RegexCountFile("Rac maint activating, worker", "hera.log")
	t.Logf("Recycle worker count: %d", recycleWorkersCount)
	if 0 == recycleWorkersCount {
		t.Fatalf("requires rac maint activation for main module for status 'R'")
	}

	time.Sleep(4100 * time.Millisecond)
	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}
	insS, err := tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
	if err != nil {
		t.Fatalf("Error prep ins %s\n", err.Error())
	}
	// mysql uses instId 0 since there isn't instid's
	timeInMillis := time.Now().Unix() + 1
	insS.Exec(0, "U", timeInMillis, "hera-test", hostname)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)

	if 2 == testutil.RegexCountFile("RACMAINT_INFO_CHANGE", "cal.log") {
		t.Fatalf("we have 2 insert queries with different status, so should log the RACMAINT_INFO_CHANGE event for every change")
	}
	if 0 != testutil.RegexCountFile("invalid_status", "cal.log") {
		t.Fatalf("rac maint status 'U' should not skip with invalid-status event")
	}
	logger.GetLogger().Log(logger.Debug, "TestRacMaintWithBothModulesAndStateChange2 done  -------------------------------------------------------------")
}
