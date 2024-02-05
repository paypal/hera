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
 ** Validate opscfig max_requests_per_child change take effect at run time
 *******************/

func TestMaxRequestsOpscfgChange(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestMaxRequestsOpscfgChange begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	//Insert a row
	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (1, 'Jack', 100)")

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
        // insert one row in the table
        conn, err := db.Conn(ctx)
        if err != nil {
                t.Fatalf("Error getting connection %s\n", err.Error())
        }
	//Send 10 select requests, max_requests_per_child = 4, worker restarting is seen twince 
	for i := 1; i < 10; i++ {
		stmt, _ := conn.PrepareContext(ctx, "/*cmd*/Select id, name, status from test_simple_table_1 where id=?")
		rows, _ := stmt.Query(1)
		if !rows.Next() {
			t.Fatalf("Expected 1 row")
		}

		rows.Close()
		stmt.Close()
	}
	time.Sleep(5 * time.Second)
        fmt.Println ("Verify worker is recycled due to max_request_per_child setting");
        if ( testutil.RegexCount("PROXY.*Max requests exceeded, terminate worker.*cnt 4 max 4") < 2) {
           t.Fatalf ("Error: should have worker recycled");
        }

        time.Sleep(4 * time.Second)
	//2 events from the beginning and 2 events now
        fmt.Println ("Check CAL log for worker restarted events");
        count1 := testutil.RegexCountFile ("E.*MUX.*new_worker_child_0", "cal.log");
        if (count1 < 4) {
            t.Fatalf ("Error: expected 4 new_worker_child events");
        }
        count2 := testutil.RegexCountFile ("E.*SERVER_INFO.*worker-go-start", "cal.log");
        if (count2 < 4) {
            t.Fatalf ("Error: expected 4 occworker-go-start events");
        }

	cancel()
	conn.Close()

        fmt.Println ("Opscfg change for max_requests_per_child");
	testutil.ModifyOpscfgParam (t, "hera.txt", "max_requests_per_child", "8")
        time.Sleep(50 * time.Second) //for opscfg change to take effect
	//Send 25 select requests, max_requests_per_child = 8,  worker restart should be seen 3 times 
        for i := 0; i < 25; i++ {
        	time.Sleep(100 * time.Millisecond)
		testutil.Fetch ("/*cmd*/Select id, name, status from test_simple_table_1 where id=1")
        }
        time.Sleep(5 * time.Second)
        fmt.Println ("Check CAL log for worker restarted events");
        count := testutil.RegexCountFile ("E.*MUX.*new_worker_child_0", "cal.log");
        if (count - count1 != 3) {
            t.Fatalf ("Error: expected 3 new_worker_child events");
        }
        count = testutil.RegexCountFile ("E.*SERVER_INFO.*worker-go-start", "cal.log");
        if (count -count2 != 3) {
            t.Fatalf ("Error: expected 3 occworker-go-start events");
        }
		
	testutil.DoDefaultValidation(t);

	logger.GetLogger().Log(logger.Debug, "TestMaxRequestsOpscfgChange done  -------------------------------------------------------------")
}
