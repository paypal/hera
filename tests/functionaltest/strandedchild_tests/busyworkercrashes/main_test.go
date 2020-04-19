package main 
import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
        _"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)


/*

The test will start Mysql server docker and Hera connects to this Mysql DB docker
No setup needed

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
	appcfg["database_type"] = "mysql"

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

/************
 ** Test to check if busy worker behaves properly when it crashes 
 ************/

func TestBusyWorkerCrashes(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestBusyWorkerCrashes begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	//Insert a row
 	testutil.RunDML1("insert into test_simple_table_2 (accountID, Name, Status) VALUES (12345, 'Jack', 'In Progress')")

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

        tx, _ := conn.BeginTx(ctx, nil)
        fmt.Println ("Inserting a row and sleep, not commit");
        stmt, _ := tx.PrepareContext(ctx, "/*cmd*/insert into test_simple_table_2 (accountID, Name, Status) VALUES(:AccountID, :Name, :Status)")
         _, err = stmt.Exec(sql.Named("AccountID", "12346"), sql.Named("Name", "Steve"), sql.Named("Status", "done"))
        if err != nil {
                t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
        }

        stmt.Close()
        go killworker ()
	//Give it enough time to recover worker
        time.Sleep(25 * time.Second);

        cancel()
        conn.Close()

	if ( testutil.RegexCountFile ("WARNING.*unexpected_eof", "cal.log") < 1) {
           t.Fatalf ("Error: should get CAL log before busy worker is killed");
        }
	if ( testutil.RegexCount ("worker closed, exiting") < 1) {
           t.Fatalf ("Error: should see log for  worker closed, exiting");
        }

	fmt.Print ("Verify after worker gets restarted, it serves requests successfully");
        ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)

        // Verify worker is up and serve the request successfully` 
        conn1, err := db.Conn(ctx1)
        if err != nil {
                t.Fatalf("Error getting connection %s\n", err.Error())
        }
        stmt1, _ := conn1.PrepareContext(ctx1, "/*cmd*/Select accountID, name from test_simple_table_2 where accountID=?")
        rows, _ := stmt1.Query("12345")
        if !rows.Next() {
                t.Fatalf("Expected 1 row")
        }

	rows.Close()	
        stmt1.Close()
        cancel1()
        conn1.Close()

	//if ( testutil.RegexCount ("poolsize(ready) 1  type  0  instance  0") < 2) {
        //   t.Fatalf ("Error: should see poolsize ready log");
        //}
	logger.GetLogger().Log(logger.Debug, "TestBusyWorkerCrashes done  -------------------------------------------------------------")
}

