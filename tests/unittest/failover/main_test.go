package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	appcfg["x-mysql"] = "manual" // disable test framework spawning mysql server
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "1"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	appcfg["child.executable"] = "mysqlworker"

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
	// startup two mysql DBs
	dbName := "failovertestdb"
	ip1 := testutil.MakeMysql("mysql33",dbName)
	ip2 := testutil.MakeMysql("mysql44",dbName)
	os.Setenv("TWO_TASK", "tcp("+ip1+":3306)/"+dbName+"?timeout=1s||tcp("+ip2+":3306)/"+dbName+"?timeout=1s")

	/*
	for {
		conn, err := net.Dial("tcp", ip2+":3306")
		if err != nil {
			time.Sleep(1 * time.Second)
			logger.GetLogger().Log(logger.Warning, "waiting for mysql server to come up")
			continue
		} else {
			conn.Close()
			break
		}
	} // */

	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestFailover(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestFailover begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

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
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	defer conn.Close()

	doCrud(conn, 1, t)
	doCrud(conn, 1, t)
	conn, err = db.Conn(ctx)
	if err != nil {
		logger.GetLogger().Log(logger.Debug, "reacq conn "+err.Error())
	}
	doCrud(conn, 1, t)

	logger.GetLogger().Log(logger.Debug, "TestFailover taking out first db +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
        cleanCmd := exec.Command("docker", "stop", "mysql33")
        cleanCmd.Run()
	logger.GetLogger().Log(logger.Debug, "TestFailover taken out first db +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	conn.Close()

	time.Sleep(4 * time.Second)
	/* It's easier just to wait for some time instead of trying to flush
	old connections */
	logger.GetLogger().Log(logger.Debug, "TestFailover flush wait done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	didWork := doCrud(conn, 2, t)
	didWork = didWork || doCrud(conn, 3, t)
	didWork = didWork || doCrud(conn, 4, t)
	didWork = didWork || doCrud(conn, 5, t)
	if !didWork {
		logger.GetLogger().Log(logger.Warning, "TestFailover post primary shutdown, no work done")
		t.Fatalf("failed to do any work after primary shutdown")
	}
	logger.GetLogger().Log(logger.Debug, "TestFailover done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")


}

func doCrud(conn *sql.Conn, id int, t* testing.T) (bool) {
	note := time.Now().Format("test note 2006-01-02j15:04:05.000 failover")

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	stmt, err := conn.PrepareContext(ctx, "create table test_failover ( id int, note varchar(55) )")
	if err != nil {
		return false
	}
	stmt.Exec()
	// ignore errors since table may already exist

	// not using txn since mysql
	stmt, err = conn.PrepareContext(ctx, "insert into test_failover ( id , note ) values ( ?, ? )")
	if err != nil {
		t.Fatalf("Error preparing test (insert table) %s\n", err.Error())
	}
	_, err = stmt.Exec(id, note)
	if err != nil {
		t.Fatalf("Error exec test (insert table) %s\n", err.Error())
	}

	stmt, err = conn.PrepareContext(ctx, "select note from test_failover where id = ?")
	if err != nil {
		t.Fatalf("Error preparing test (sel table) %s\n", err.Error())
	}
        rows, _ := stmt.Query(id)
        if !rows.Next() {
                t.Fatalf("Expected 1 row")
        }
        var str_val sql.NullString
        err = rows.Scan(&str_val)
	if err != nil {
		t.Fatalf("Error preparing test (sel scan table) %s\n", err.Error())
	}
	if !str_val.Valid {
		t.Fatalf("null str")
	}
	if str_val.String != note {
		t.Fatalf("data corrupt "+note+" dbHas:"+ str_val.String)
	}

        rows.Close()
        stmt.Close()


	stmt, err = conn.PrepareContext(ctx, "delete from test_failover where id = ?")
	if err != nil {
		t.Fatalf("Error preparing test (del table) %s\n", err.Error())
	}
	_, err = stmt.Exec(id)
	if err != nil {
		t.Fatalf("Error preparing test (del table) %s\n", err.Error())
	}
	return true
}
