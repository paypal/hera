package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
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
	appcfg["rac_sql_interval"] = "2"
	appcfg["db_heartbeat_interval"] = "3"

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

var ip1 string
var ip2 string
var dbName = "failovertestdb"

func TestMain(m *testing.M) {
	// startup two mysql DBs
	ip1 = testutil.MakeDB("mysql33", dbName, testutil.MySQL)
	ip2 = testutil.MakeDB("mysql44", dbName, testutil.MySQL)
	os.Setenv("TWO_TASK", "tcp("+ip1+":3306)/"+dbName+"?timeout=11s||tcp("+ip2+":3306)/"+dbName+"?timeout=11s")

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

	hostname := testutil.GetHostname()
	fmt.Println("Hostname: ", hostname)
	db, err := sql.Open("hera", hostname+":31002")
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
	/*
		conn2, err := db.Conn(ctx)
		if err != nil {
			logger.GetLogger().Log(logger.Debug, "reacq conn "+err.Error())
		}
		doCrud(conn2, 1, t)
		conn2.Close() //*/

	logger.GetLogger().Log(logger.Debug, "TestFailover taking out first db +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	cleanCmd := exec.Command("docker", "stop", "mysql33")
	cleanCmd.Run()
	logger.GetLogger().Log(logger.Debug, "TestFailover taken out first db +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	time.Sleep(8 * time.Second)
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
	cleanCmd = exec.Command("docker", "start", "mysql33")
	cleanCmd.Run()

}

func doCrud(conn *sql.Conn, id int, t *testing.T) bool {

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	//note := time.Now().Format("test note 2006-01-02j15:04:05.000 failover")
	stmt, err := conn.PrepareContext(ctx, "drop table test_failover")
	if err != nil {
		return false
	}
	//noTable := false
	_, err = stmt.Exec()
	if err != nil {
		//noTable = true
	}
	// ignore errors since table might not exist

	stmt, err = conn.PrepareContext(ctx, "create table test_failover ( id int, note varchar(55) )")
	if err != nil {
		return false
	}
	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("create table had issue %s", err.Error())
	}
	// ignore errors since table may already exist

	/*
			// not using txn since mysql
			stmt, err = conn.PrepareContext(ctx, "insert into test_failover ( id , note ) values ( ?, ? )")
			if err != nil {
				t.Fatalf("Error preparing test (insert table) %s\n", err.Error())
			}
			_, err = stmt.Exec(id, note)
			if err != nil {
				t.Fatalf("Error exec test (insert table) %s\n", err.Error())
			}

			stmt, err = conn.PrepareContext(ctx, "insert into test_failover (id , note ) values ( ?, ? )")
			if err != nil {
				t.Fatalf("Error prep test (insert neg-id table) %s\n", err.Error())
			}
			_, err = stmt.Exec(-id, note)
			if err != nil {
				t.Fatalf("Error exec test (insert neg-id table) %s\n", err.Error())
			}

			/*
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
			// */

	/*
		stmt, err := conn.PrepareContext(ctx, "delete from test_failover where id = ?")
		if err != nil {
			t.Fatalf("Error preparing test (del table) %s\n", err.Error())
		}
		_, err = stmt.Exec(id)
		if err != nil {
			t.Fatalf("Error exec test (del table) %s\n", err.Error())
		}
		// */
	return true
}
