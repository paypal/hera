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

func TestRacMaint(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestRacMaint begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

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
	insS.Exec(0, "F", time.Now().Unix()+1, "hera-test", hostname)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error 2commit %s\n", err.Error())
	}

	time.Sleep(4100 * time.Millisecond)
	if 0 == testutil.RegexCountFile("Rac maint activating, worker", "hera.log") {
		t.Fatalf("missed rac maint activation")
	}

	logger.GetLogger().Log(logger.Debug, "TestRacMaint done  -------------------------------------------------------------")
}
