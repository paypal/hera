package main 
import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
	//"github.com/paypal/hera/client/gosqldriver"
        //_"github.com/paypal/hera/client/gosqldriver/tcp"
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
        appcfg["opscfg.default.server.idle_timeout_ms"] = "5000"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["database_type"] = "mysql"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "2"
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
   # idle_timeout_ms="5000"
   # Perform an insert then select
   # Sleep for more than 5 seconds
   # Verify idle timeout kick in and server close connection 
   ##########################################################################################
*/

func TestNonDlmIdle(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestNonDlmIdle begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

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

	tx, _ := conn.BeginTx(ctx, nil)
	fmt.Println ("Perform an insert & commit");
        stmt, _ := tx.PrepareContext(ctx, "/*TestBasic*/ insert into test_simple_table_2 (accountID, Name, Status) VALUES (12345, 'Linda Smith' , '111')")
	_, err = stmt.Exec()
	if err != nil {
                t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
        }
	err = tx.Commit()
        if err != nil {
                t.Fatalf("Error commit %s\n", err.Error())
        }
	stmt, _ = conn.PrepareContext(ctx, "/*cmd*/Select accountID, name from test_simple_table_2 where accountID=?")
        rows, _ := stmt.Query("12345")
	if !rows.Next() {
                t.Fatalf("Expected 1 row")
        }

	time.Sleep(7 * time.Second);
	fmt.Println ("Verify idle timeout kicked in and server closes connection");
	if ( testutil.RegexCount("Connection handler idle timeout") < 1) {
           t.Fatalf ("Error: Connection should close due to idle timeout");
        }

        count := testutil.RegexCountFile ("E.*OCCMUX.*idle_timeout_5000", "cal.log")
	if (count > 0) {
	    t.Fatalf ("Error: should see idle_timeout event in CAL");
	}
	rows.Close()    
        stmt.Close()
	cancel()
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestNonDlmIdle done  -------------------------------------------------------------")
}

