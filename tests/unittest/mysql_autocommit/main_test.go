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
	appcfg["rac_sql_interval"] = "0"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_mux_test"
	}

	testutil.RunDML("DROP TABLE " + tableName)
	return testutil.RunDML("CREATE TABLE " + tableName + " ( id bigint, int_val bigint, str_val varchar(128) )")
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

func TestMysqlAutocommit(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestMysqlAutocommit begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

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
	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting conn2 %s\n", err.Error())
	}
	defer conn2.Close()
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
	stmt, _ = tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (id, int_val, str_val) VALUES(?, ?, ?)")
	_, err = stmt.Exec(1, time.Now().Unix(), "val 1")
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	if getRows(1, conn) != 1 {
		t.Fatalf("exp 1 row")
	}

	// autocommit should see the row in other conn immediately
	stmt, _ = conn.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (id, int_val, str_val) VALUES(?, ?, ?)")
	_, err = stmt.Exec(2, time.Now().Unix(), "val 2")
	if err != nil {
		t.Fatalf("Error preparing test (create row id2 in table) %s\n", err.Error())
	}
	if getRows(2, conn) != 1 {
		t.Fatalf("exp 1 row id2")
	}
	/* hera blends Oracle implicit transaction starts
	so they must be committed to be visible.
	if getRows(2, conn2) != 1 {
		t.Fatalf("exp 1 row id2 conn2")
	} // */

	// in txn, other conn sees after commit
	tx, _ = conn.BeginTx(ctx, nil)
	_/*result*/, err = tx.ExecContext(ctx, "begin /* start transaction */")
	if err != nil {
		t.Fatalf("begin/start txn statement issue %s", err.Error())
	}

	stmt, _ = tx.PrepareContext(ctx, "/*cmd*/insert into "+tableName+" (id, int_val, str_val) VALUES(?, ?, ?)")
	_, err = stmt.Exec(3, time.Now().Unix(), "val 3")
	if err != nil {
		t.Fatalf("Error preparing test (create row id3 in table) %s\n", err.Error())
	}
	if getRows(3, conn) != 1 {
		t.Fatalf("exp 1 row id3")
	}
	if getRows(3, conn2) != 0 {
		t.Fatalf("exp 0 row id3 conn2")
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}
	if getRows(3, conn2) != 1 {
		t.Fatalf("exp 3 row id2 conn2")
	}

	logger.GetLogger().Log(logger.Debug, "TestMysqlAutocommit done  -------------------------------------------------------------")
}
func getRows(id int, conn *sql.Conn) (int) {
	out := 0
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	stmt, _ := conn.PrepareContext(ctx, "/*cmd*/Select id, int_val from "+tableName+" where id=?")
	rows, _ := stmt.Query(id)
	for rows.Next() {
		out++
	}

	rows.Close()
	stmt.Close()
	return out;
}
