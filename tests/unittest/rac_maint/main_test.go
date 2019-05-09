package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
	"os"
	"testing"
	"time"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "occ.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "1"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "occ_maint"
	}

	testutil.RunDML("DROP TABLE " + tableName)
	return testutil.RunDML("CREATE TABLE " + tableName + " ( inst_id bigint, machine varchar(512), status varchar(8), status_time bigint, module varchar(64) )")
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

func TestRacMaint(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestRacMaint begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	db, err := sql.Open("occloop", fmt.Sprintf("%d:0:0", shard))
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
	_, err = stmt.Exec(15/*max instid*/, "F", time.Now().Unix()+2, "occ", hostname)
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	delS, _ := conn.PrepareContext(ctx, "/*cmd*/delete from "+tableName)
	insS, _ := conn.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (inst_id, status, status_time, module, machine) values (?,?,?,?,?)")
	time.Sleep(1100 * time.Millisecond)
	if 0 != testutil.RegexCount("Rac maint activating, worker") {
		t.Fatalf("should not have rac maint activation")
	}

	delS.Exec()
	// mysql uses instId 0 since there isn't instid's
	insS.Exec(0, "F", time.Now().Unix()+1, "occ", hostname)

	time.Sleep(1100 * time.Millisecond)
	if 0 == testutil.RegexCount("Rac maint activating, worker") {
		t.Fatalf("missed rac maint activation")
	}

	logger.GetLogger().Log(logger.Debug, "TestRacMaint done  -------------------------------------------------------------")
}
