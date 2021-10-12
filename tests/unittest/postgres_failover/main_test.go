package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// appcfg["x-mysql"] = "manual" // disable test framework spawning mysql server
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "32002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "2"
	appcfg["db_heartbeat_interval"] = "3"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	appcfg["child.executable"] = "postgresworker"

	return appcfg, opscfg, testutil.PostgresWorker
}

func before() error {
	pfx := os.Getenv("MGMT_TABLE_PREFIX")
	if pfx == "" {
		pfx = "hera"
	}
	tableName = pfx + "_maint"
	return nil
}

var ip1 string
var ip2 string
var dbName = "failovertestdb"

func TestMain(m *testing.M) {
	// startup two postgres DBs
	ip1 = testutil.MakePostgres("postgres33",dbName)
	ip2 = testutil.MakePostgres("postgres44",dbName)
	os.Setenv("TWO_TASK", ip1+"/"+dbName+"?connect_timeout=60&sslmode=disable||"+ip2+"/"+dbName+"?connect_timeout=60&sslmode=disable")
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

	logger.GetLogger().Log(logger.Debug, "TestFailover taking out first db +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	cleanCmd := exec.Command("docker", "stop", "postgres33")
	cleanCmd.Run()
	logger.GetLogger().Log(logger.Debug, "TestFailover taken out first db +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	time.Sleep(90 * time.Second)
	/* It's easier just to wait for some time instead of trying to flush
	old connections */
	logger.GetLogger().Log(logger.Debug, "TestFailover flush wait done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	conn2, err := db.Conn(ctx2)
	if err != nil {
		logger.GetLogger().Log(logger.Debug, "reacq conn "+err.Error())
	}
	defer conn2.Close()
	didWork := doCrud(conn2, 2, t)
	logger.GetLogger().Log(logger.Debug, "TestFailover crud 222222222222222222222222222222222 \n")
	didWork = didWork || doCrud(conn2, 3, t)
	logger.GetLogger().Log(logger.Debug, "TestFailover crud 333333333333333333333333333333333 \n")
	didWork = didWork || doCrud(conn2, 4, t)
	logger.GetLogger().Log(logger.Debug, "TestFailover crud 444444444444444444444444444444444 \n")
	didWork = didWork || doCrud(conn2, 5, t)
	logger.GetLogger().Log(logger.Debug, "TestFailover crud 555555555555555555555555555555555 \n")
	if !didWork {
		logger.GetLogger().Log(logger.Warning, "TestFailover post primary shutdown, no work done")
		t.Fatalf("failed to do any work after primary shutdown")
	}
	logger.GetLogger().Log(logger.Debug, "TestFailover done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	// clean up
	cleanCmd = exec.Command("docker", "rm", "postgres33")
	cleanCmd.Run()
	cleanCmd = exec.Command("docker", "stop", "postgres44")
	cleanCmd.Run()
	cleanCmd = exec.Command("docker", "rm", "postgres44")
	cleanCmd.Run()
}

func commit(conn *sql.Conn, t* testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
			t.Fatalf("Error begin tx %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
			t.Fatalf("Error commit %s\n", err.Error())
	}
}

func rollback(conn *sql.Conn, t* testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
			t.Fatalf("Error begin tx %s\n", err.Error())
	}
	err = tx.Rollback()
	if err != nil {
			t.Fatalf("Error rollback %s\n", err.Error())
	}
}

func doCrud(conn *sql.Conn, id int, t* testing.T) (bool) {

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	stmt, err := conn.PrepareContext(ctx, "drop table test_failover")
	if err != nil {
		rollback(conn, t)
		return false
	}
	//noTable := false
	_,err = stmt.Exec()
	if err != nil {
		rollback(conn, t)
		//noTable = true
	}
	// ignore errors since table might not exist

	stmt, err = conn.PrepareContext(ctx, "create table test_failover ( id int, note varchar(55) )")
	if err != nil {
		rollback(conn, t)
		return false
	}
	_,err = stmt.Exec()
	if err != nil {
		rollback(conn, t)
		t.Fatalf("create table had issue %s",err.Error())
	}
	commit(conn, t)
	return true
}

