package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*

The test will start Mysql server docker. Hera sever connects to this Mysql DB docker
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
        appcfg["opscfg.default.server.max_requests_per_child"] = "4"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["database_type"] = "mysql"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "2"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
        testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
        return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*******************
 ** Validate max_requests_per_child set for the Hera take effect when we send nonDML requests
 *******************/

func TestMaxRequestsNonDML(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestMaxRequestsNonDML begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname := testutil.GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	fmt.Println ("Check Root Transaction Logging in CAL log - max_connections=2");
        l1 := testutil.RegexCountFile ("A.*URL.*INITDB.*0", "cal.log");
        if (l1 < 2) {
            t.Fatalf ("Error: should see 2 Root Transaction logging lines, but get %d ", l1);
        }
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// insert one row in the table
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*cmd*/insert into test_simple_table_1 (id, Name, Status) VALUES(?, ?, ?)")
	_, err = stmt.Exec(1, time.Now().Unix(), 1*10)
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	//Send 80 select requests, max_requests_per_child = 4, verify workers are terminated a total of 20 times
	for i := 1; i < 80; i++ {
		stmt, err = conn.PrepareContext(ctx, "/*cmd*/Select id, name, status from test_simple_table_1 where id=?")
		if err != nil { 
			t.Fatalf("could not prepare statement %s", err.Error()) 
		}
		rows, _ := stmt.Query(1)
		if !rows.Next() {
			t.Fatalf("Expected 1 row")
		}

		rows.Close()
		stmt.Close()
	}
	time.Sleep(7 * time.Second)
        fmt.Println ("Verify worker is recycled due to max_request_per_child setting");
	//Count how many times worker recycle encounters error (due to other workers not complete recycling)
	err_count :=  testutil.RegexCountFile ("E.*ERROR.*RECYCLE_WORKER", "cal.log")
        if ( testutil.RegexCount("PROXY.*Max requests exceeded, terminate worker.*cnt 4 max 4") < (20 - err_count)) {
           t.Fatalf ("Error: should have worker recycle at least 20 times");
        }

        time.Sleep(5 * time.Second)
        fmt.Println ("Check CAL log for worker restarted events");
        count := testutil.RegexCountFile ("E.*MUX.*new_worker_child_0", "cal.log");
        if (count < 20) {
            t.Fatalf ("Error: expected 20 new_worker_child events");
        }
        count = testutil.RegexCountFile ("E.*SERVER_INFO.*worker-go-start", "cal.log");
        if (count < 20) {
            t.Fatalf ("Error: expected 20 occworker-go-start events");
        }
	
	time.Sleep(1 * time.Second)
	fmt.Println ("Verify Root Transaction Logging in CAL log after workers are restarted");
        l2 := testutil.RegexCountFile ("A.*URL.*INITDB.*0", "cal.log");
        if (l2-l1 < 2) {
            t.Fatalf ("Error: should see 2 more Root Transaction logging lines, but get %d ", l2);
        }

	cancel()
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestMaxRequestsNonDML done  -------------------------------------------------------------")
}
