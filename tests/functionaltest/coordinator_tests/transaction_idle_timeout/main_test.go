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
        appcfg["opscfg.default.server.idle_timeout_ms"] = "3000"
        appcfg["opscfg.default.server.transaction_idle_timeout_ms"] = "5000"
	//appcfg["child.executable"] = "mysqlworker"
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

func TestTransactionIdleTimeout(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTransactionIdleTimeout begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname,_ := os.Hostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                t.Fatal("Error starting Mux:", err)
                return
        }

	db.SetMaxIdleConns(0)
	defer db.Close()

	var wait_second int
        wait_second = 8
	fmt.Println ("Verify idle timeout will not kick in, but tnx timeout will kick in for DML transaction")
	testutil.RunDMLCommitLater("/*cmd*/insert into test_simple_table_2 (accountID, Name, Status) VALUES (12345, 'Linda Smith' , '111')", wait_second)
        /*if err != nil {
                t.Fatalf("Error inserting row %s\n", err.Error())
        }*/

        fmt.Println ("Open new connection to check fetch result");
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        // cleanup and insert one row in the table
        conn, err := db.Conn(ctx)
        if err != nil {
                t.Fatalf("Error getting connection %s\n", err.Error())
        }

        fmt.Println ("Verify fetch request returns correct result");
        stmt, _ := conn.PrepareContext(ctx, "/*TestBasic*/Select name, status from test_simple_table_2 where accountID=:accountID")
        rows, _ := stmt.Query(sql.Named("accountID", "12345"))
        if rows.Next() {
                t.Fatalf("Expected 0 row")
        }

	time.Sleep(2 * time.Second)

	if ( testutil.RegexCount("Connection handler idle timeout") < 1) {
           t.Fatalf ("Error: should have txn timeout");
        }

	if ( testutil.RegexCountFile("E.*MUX.*idle_timeout_5000", "cal.log") < 1) {
           t.Fatalf ("Error: should have txn timeout");
        }

	fmt.Println ("Verify idle timeout event is NOT seen in CALlog")
        count := testutil.RegexCountFile ("E.*MUX.*idle_timeout_3000", "cal.log")
	if (count > 0) {
	    t.Fatalf ("Error: should NOT see idle_timeout event");
	}

	rows.Close()	
        stmt.Close()
        cancel()
        conn.Close()
	logger.GetLogger().Log(logger.Debug, "TestTransactionIdleTimeout done  -------------------------------------------------------------")
}

