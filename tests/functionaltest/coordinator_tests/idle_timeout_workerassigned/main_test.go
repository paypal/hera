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

The test will start Mysql server docker and OCC connects to this Mysql DB docker
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
	appcfg["database_type"] = "mysql"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
        return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}

/*******************
 *  Validate client idle_timeout NOT kick in when worker is assigned
 *******************/

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

func TestIdleTimeoutWorkerAssigned(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestIdleTimeoutWorkerAssigned begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

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
	fmt.Println ("Verify idle timeout will not kick in for DML transaction")
	testutil.RunDMLCommitLater("/*cmd*/insert into test_simple_table_1 (ID, Name, Status) VALUES (77, 'Testing', 77)", wait_second)
        if err != nil {
                t.Fatalf("Error inserting row %s\n", err.Error())
        }

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                t.Fatalf("Error getting connection %s\n", err.Error())
        }

	fmt.Println ("Verify fetch request returns correct result")
        stmt, _ := conn.PrepareContext(ctx, "/*cmd*/Select id, Name from test_simple_table_1 where id=?")
        rows, _ := stmt.Query(77)
        if !rows.Next() {
                t.Fatalf("Expected 1 row")
        }

	time.Sleep(2 * time.Second)
	fmt.Println ("Verify idle timeout event is NOT seen in CALlog")
        count := testutil.RegexCountFile ("E.*MUX.*idle_timeout_3000", "cal.log")
	if (count > 0) {
	    t.Fatalf ("Error: should NOT see idle_timeout event");
	}

	rows.Close()	
        stmt.Close()
        cancel()
        conn.Close()
	logger.GetLogger().Log(logger.Debug, "TestIdleTimeoutWorkerAssigned done  -------------------------------------------------------------")
}

