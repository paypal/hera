package main 
import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
	//"github.com/paypal/hera/client/gosqldriver"
        _"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*
To run the test
export username=clocapp
export password=clocappstg
export DB_USER=$username
export DB_PASSWORD=password
export TWO_TASK='tcp(127.0.0.1:3306)/world?timeout=10s'
export TWO_TASK_READ='tcp(127.0.0.1:3306)/world?timeout=10s'
export DB_DATASOURCE=$TWO_TASK

$GOROOT/bin/go install  .../worker/{mysql,oracle}worker
ln -s $GOPATH/bin/{mysql,oracle}worker .
*/

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

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "1"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_2")
        return testutil.RunDML("CREATE TABLE test_simple_table_2 (accountID VARCHAR(64) PRIMARY KEY, NAME VARCHAR(64), STATUS VARCHAR(64), CONDN VARCHAR(64))")
}

func killworker () {
        fmt.Println ("Termintating worker")
        testutil.BashCmd("killall -9 oracleworker");
        testutil.BashCmd("killall -9 mysqlworker");
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

func TestFreeWorkerCrashes(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestFreeWorkerCrashes begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname,_ := os.Hostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
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

        fmt.Println ("Set autocommit to false")
        tx, _ := conn.BeginTx(ctx, nil)

        fmt.Println ("Inserting a row and sleep, not commit");
        stmt, _ := tx.PrepareContext(ctx, "/*cmd*/insert into test_simple_table_2 (accountID, Name, Status) VALUES(:AccountID, :Name, :Status)")
         _, err = stmt.Exec(sql.Named("AccountID", "12346"), sql.Named("Name", "Steve"), sql.Named("Status", "done"))
        if err != nil {
                t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
        }

        stmt.Close()
        go killworker ()
        time.Sleep(10 * time.Second);

        cancel()
        conn.Close()

	/*if ( testutil.RegexCount("worker.*received signal. transits from state  2  to terminated'") < 1) {
           t.Fatalf ("Error: should get log regarding worker getting killed");
        }*/

	if ( testutil.RegexCountFile("WARNING.*unexpected_eof.*closed connection on coordinator", "cal.log") < 1) {
           t.Fatalf ("Error: should see unexpected_eof from CAL");
        }

	fmt.Print ("Verify after worker gets restarted, it serves requests successfully");
        ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
        // cleanup and insert one row in the table
        conn1, err := db.Conn(ctx1)
        if err != nil {
                t.Fatalf("Error getting connection %s\n", err.Error())
        }
        stmt1, _ := conn1.PrepareContext(ctx1, "/*cmd*/Select accountID, name from test_simple_table_2 where accountID=?")
        rows, _ := stmt1.Query("12346")
        if !rows.Next() {
                t.Fatalf("Expected 1 row")
        }

	rows.Close()	
        stmt1.Close()
        cancel1()
        conn1.Close()
	logger.GetLogger().Log(logger.Debug, "TestFreeWorkerCrashes done  -------------------------------------------------------------")
}

