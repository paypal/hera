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

func TestRacMaintWithStatusChange(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestRacMaintWithStatusChange begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
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
	_, err = stmt.Exec(15 /*max instid*/, "F", time.Now().Unix()+2, "hera-test", hostname)
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)
	if 0 != testutil.RegexCountFile("Rac maint activating, worker", "hera.log") {
		t.Fatalf("should not have rac maint activation instance ID: 15")
	}

	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}
	delS, err := tx.PrepareContext(ctx, "/*cmd*/delete from "+tableName)
	if err != nil {
		t.Fatalf("Error prep del %s\n", err.Error())
	}
	delS.Exec()
	insS, err := tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
	if err != nil {
		t.Fatalf("Error prep ins %s\n", err.Error())
	}
	// mysql uses instId 0 since there isn't instid's
	insS.Exec(0, "U", time.Now().Unix()+1, "hera-test", hostname)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)
	if 0 != testutil.RegexCountFile("Rac maint activating, worker", "hera.log") {
		t.Fatalf("RAC Maint should not activate for status U")
	}

	if 0 == testutil.RegexCountFile("module:HERA-TEST", "cal.log") {
		t.Fatalf("Status 'U' should log the RACMAINT_INFO_CHANGE event")
	}

	// mysql uses instId 0 since there isn't instid's
	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}
	stmt2, err := tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
	if err != nil {
		t.Fatalf("Error prep ins %s\n", err.Error())
	}
	// mysql uses instId 0 since there isn't instid's
	timeInMillis := time.Now().Unix() + 1
	stmt2.Exec(0, "R", timeInMillis, "hera-test", hostname)
	stmt2.Exec(0, "R", timeInMillis, "hera-test_taf", hostname)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)

	if 0 == testutil.RegexCountFile("Rac maint activating, worker", "hera.log") {
		t.Fatalf("requires rac maint activation for main module status")
	}

	if 0 == testutil.RegexCountFile("module:HERA-TEST_TAF", "cal.log") {
		t.Fatalf("Status 'U' should log the RACMAINT_INFO_CHANGE event")
	}
	if 0 != testutil.RegexCountFile("invalid_status", "cal.log") {
		t.Fatalf("ram maint status 'U' should not skip with invalid-status event")
	}
	time.Sleep(4100 * time.Millisecond)
	//Clear logs
	testutil.ClearLogsData()

	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Error to start transaction %s\n", err.Error())
	}
	stmt2, err = tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
	if err != nil {
		t.Fatalf("Error prep ins %s\n", err.Error())
	}
	// mysql uses instId 0 since there isn't instid's
	timeInMillis = time.Now().Unix() + 1
	stmt2.Exec(0, "U", timeInMillis, "hera-test", hostname)
	stmt2.Exec(0, "U", timeInMillis, "hera-test_taf", hostname)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)

	if 0 == testutil.RegexCountFile("module:HERA-TEST_TAF", "cal.log") {
		t.Fatalf("Status 'U' should log the RACMAINT_INFO_CHANGE event")
	}

	if 0 == testutil.RegexCountFile("module:HERA-TEST", "cal.log") {
		t.Fatalf("Status 'U' should log the RACMAINT_INFO_CHANGE event")
	}

	if 2 == testutil.RegexCountFile("RACMAINT_INFO_CHANGE", "cal.log") {
		t.Fatalf("We have 2 insert queries with different status, so should log the RACMAINT_INFO_CHANGE event for every change")
	}

	if 0 != testutil.RegexCountFile("invalid_status", "cal.log") {
		t.Fatalf("ram maint status 'U' should not skip with invalid-status event")
	}

	//Clear logs
	testutil.ClearLogsData()

	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Error to start transaction %s\n", err.Error())
	}
	stmt2, err = tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
	if err != nil {
		t.Fatalf("Error prep ins %s\n", err.Error())
	}
	// mysql uses instId 0 since there isn't instid's
	timeInMillis = time.Now().Unix() + 1
	stmt2.Exec(0, "F", timeInMillis, "hera-test", hostname)
	stmt2.Exec(0, "F", timeInMillis, "hera-test_taf", hostname)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)

	if 0 == testutil.RegexCountFile("Rac maint activating, worker", "hera.log") {
		t.Fatalf("requires rac maint activation for main module status")
	}

	if 0 == testutil.RegexCountFile("module:HERA-TEST_TAF", "cal.log") {
		t.Fatalf("Status 'F' should log the RACMAINT_INFO_CHANGE event")
	}

	if 0 == testutil.RegexCountFile("module:HERA-TEST", "cal.log") {
		t.Fatalf("Status 'F' should log the RACMAINT_INFO_CHANGE event")
	}

	if 2 == testutil.RegexCountFile("RACMAINT_INFO_CHANGE", "cal.log") {
		t.Fatalf("We have 2 insert queries with different status, so should log the RACMAINT_INFO_CHANGE event for every change")
	}

	if 0 != testutil.RegexCountFile("invalid_status", "cal.log") {
		t.Fatalf("ram maint status 'U' should not skip with invalid-status event")
	}

	logger.GetLogger().Log(logger.Debug, "TestRacMaintWithStatusChange done  -------------------------------------------------------------")
}

func TestRcMaintInvalidStatus(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestRacMaintInvalidStatus begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	_, err = stmt.Exec(15 /*max instid*/, "F", time.Now().Unix()+2, "hera-test", hostname)
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)
	if 0 != testutil.RegexCountFile("Rac maint activating, worker", "hera.log") {
		t.Fatalf("should not have rac maint activation")
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
	insS.Exec(0, "T", time.Now().Unix()+1, "hera-test", hostname)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)

	if 0 == testutil.RegexCountFile("invalid_status", "cal.log") {
		t.Fatalf("ram maint should skip with invalid-status event")
	}
	if 0 != testutil.RegexCountFile("Rac maint activating, worker", "hera.log") {
		t.Fatalf("should not have rac maint activation for invalid status")
	}

	logger.GetLogger().Log(logger.Debug, "TestRacMaintInvalidStatus done  -------------------------------------------------------------")
}

func TestRcMaintNoChangeInTMAndStatus(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestRcMaintNoChangeInTM begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	hostname, _ := os.Hostname()
	insS, err := tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
	if err != nil {
		t.Fatalf("Error prep ins %s\n", err.Error())
	}
	// mysql uses instId 0 since there isn't instid's
	timeValInMillis := time.Now().Unix() + 1
	insS.Exec(0, "U", timeValInMillis, "hera-test", hostname)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)
	if 0 != testutil.RegexCountFile("Rac maint activating, worker", "hera.log") {
		t.Fatalf("RAC Maint should not activate for status U")
	}

	if 0 == testutil.RegexCountFile("module:HERA-TEST", "cal.log") {
		t.Fatalf("Status 'U' should log the RACMAINT_INFO_CHANGE event")
	}

	//Clear logs
	testutil.ClearLogsData()
	// mysql uses instId 0 since there isn't instid's
	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}
	stmt2, err := tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
	if err != nil {
		t.Fatalf("Error prep ins %s\n", err.Error())
	}

	stmt2.Exec(0, "U", timeValInMillis, "hera-test", hostname)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)

	if 0 != testutil.RegexCountFile("Rac maint activating, worker", "hera.log") {
		t.Fatalf("RAC Maint should not activate for status U")
	}

	if 0 != testutil.RegexCountFile("module:HERA-TEST", "cal.log") {
		t.Fatalf("Status 'U' same TM value should not log the RACMAINT_INFO_CHANGE event")
	}
	logger.GetLogger().Log(logger.Debug, "TestRcMaintNoChangeInTM done  -------------------------------------------------------------")
}
