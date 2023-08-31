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
        appcfg["opscfg.default.server.max_requests_per_child"] = "10"
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

/*****
 *
 *  Verify workers are restarted when max_requests_per_child is reached
 *
 ****/

func TestMaxRequestPerChildDML(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestMaxRequestPerChildDML begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	//shard := 0
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
	// cancel must be called before conn.Close()
        defer cancel()
        // cleanup and insert one row in the table
        conn, err := db.Conn(ctx)
        if err != nil {
                t.Fatalf("Error getting connection %s\n", err.Error())
        }

	for i:=0; i < 8; i++ {
                tx, _ := conn.BeginTx(ctx, nil)

                fmt.Println ("Inserting 8 rows for max_requests_per_child to kick in");
                stmt, _ := tx.PrepareContext(ctx, "/*cmd*/insert into test_simple_table_1 (ID, Name, Status) VALUES(:ID, :Name, :Status)")
		defer stmt.Close()
                 _, err = stmt.Exec(sql.Named("ID", i+1), sql.Named("Name", "Lee"), sql.Named("Status", i+1))
                if err != nil {
                        t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
                }
		err = tx.Commit()
                if err != nil {
                        t.Fatalf("Error commit %s\n", err.Error())
                }

                stmt.Close()
        }
        cancel()
        conn.Close()

	time.Sleep(25 * time.Second)
	fmt.Println ("Verify worker is recycled due to max_request_per_child setting");
        if ( testutil.RegexCount("PROXY.*Max requests exceeded, terminate worker") < 1) {
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
	
	logger.GetLogger().Log(logger.Debug, "TestMaxRequestPerChildDML done  -------------------------------------------------------------")
}
