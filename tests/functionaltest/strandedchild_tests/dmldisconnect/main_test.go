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
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
        appcfg["opscfg.default.server.idle_timeout_ms"] = "3000"
        appcfg["opscfg.default.server.transaction_idle_timeout_ms"] = "5000"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["database_type"] = "mysql"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "4"
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


/* ##########################################################################################
   # Perform an insert without commit 
   # While the query is in transaction, close connection 
   # Verify worker get stranded and recovered
   ##########################################################################################
*/

func TestDmlDisconnect(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestDmlDisconnect begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname := testutil.GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                t.Fatal("Error starting Mux:", err)
                return
        }

	db.SetMaxIdleConns(0)
	defer db.Close()

        fmt.Println ("Open new connection");
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                t.Fatalf("Error getting connection %s\n", err.Error())
        }

	fmt.Println ("Perform an insert without commit");
        stmt, _ := conn.PrepareContext(ctx, "/*TestBasic*/ insert into test_simple_table_2 (accountID, Name, Status) VALUES (12345, 'Linda Smith' , '111')")
	_, err = stmt.Exec()
	if err != nil {
                t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
        }
        stmt.Close()
        cancel()
	fmt.Println ("Close connection while insert query is in transaction");
        conn.Close()

	time.Sleep(1 * time.Second);
	fmt.Println ("Verify worker get stranded and recovered");
	if ( testutil.RegexCount("begin recover worker:") < 1) {
           t.Fatalf ("Error: should have worker recovered");
        }

	if ( testutil.RegexCount("stranded conn recovered") < 1) {
           t.Fatalf ("Error: should have stranded conn recovered");
        }

	fmt.Println ("Verify worker recovery is seen in CALlog")
        count := testutil.RegexCountFile ("RECOVER.*dedicated.*0", "cal.log")
	if (count < 1) {
	    t.Fatalf ("Error: should see worker recovery event");
	}
        count = testutil.RegexCountFile ("STRANDED.*RECOVERED.*0", "cal.log")
	if (count < 1) {
	    t.Fatalf ("Error: should see worker recovery event");
	}

	logger.GetLogger().Log(logger.Debug, "TestDmlDisconnect done  -------------------------------------------------------------")
}

