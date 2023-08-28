package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
	_ "github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*

The test will start Mysql server docker and Hera server connects to this Mysql DB docker
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
	appcfg["rac_sql_interval"] = "0"
        appcfg["opscfg.default.server.max_lifespan_per_child"] = "5"
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


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*******************
 **  Validate workers are restared when  max_lifespan_per_child is reached
 *******************/

func TestMaxLifespanDML(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestMaxLifespanDML begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname := testutil.GetHostname()
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
        stmt, _ := tx.PrepareContext(ctx, "set autocommit=0")
        _, err = stmt.Exec()
        if err != nil {
                t.Fatalf("Error setting autocommit to false %s\n", err.Error())
        }

        fmt.Println ("Inserting a row and wait for long time for max_lifespan to kick in");
        stmt, _ = tx.PrepareContext(ctx, "/*cmd*/insert into test_simple_table_2 (accountID, Name, Status) VALUES(:AccountID, :Name, :Status)")
         _, err = stmt.Exec(sql.Named("AccountID", "12346"), sql.Named("Name", "Steve"), sql.Named("Status", "done"))
        if err != nil {
                t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
        }

	time.Sleep(8 * time.Second);
        fmt.Println ("Verify worker is not recycled");
        if ( testutil.RegexCount("'PROXY.*Lifespan exceeded, terminate worker'") > 0) {
	   t.Fatalf ("Error: should not have worker recycle");
	}
        time.Sleep(3 * time.Second)
        err = tx.Commit()
        if err != nil {
		fmt.Printf ("Error in commit %s\n", err);
        }
	

        stmt.Close()
        cancel()
        conn.Close()

	fmt.Println ("Verify worker is recycled due to max_lifespan setting");
        if ( testutil.RegexCount("PROXY.*Lifespan exceeded, terminate worker") < 1) {
	   t.Fatalf ("Error: should have worker recycle");
	}

        time.Sleep(10 * time.Second)
        fmt.Println ("Check CAL log for worker restarted event, 1 event from the beginning and 1 due to max_lifespan");
        count := testutil.RegexCountFile ("E.*MUX.*new_worker_child_0", "cal.log");
	if (count < 2) {
	    t.Fatalf ("Error: expected new_worker_child event");
	}
        count = testutil.RegexCountFile ("E.*SERVER_INFO.*worker-go-start", "cal.log");
	if (count < 2) {
	    t.Fatalf ("Error: expected occworker-go-start event");
	}
	
	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestMaxLifespanDML done  -------------------------------------------------------------")
}
