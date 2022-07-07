package main 
import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
	"github.com/paypal/hera/tests/functionaltest/testutil"
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
        appcfg["opscfg.default.server.idle_timeout_ms"] = "4000"
	appcfg["child.executable"] = "mysqlworker"
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


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}


/*******************
 **   Validate idle timeout kicked in  when worker is not assigned
 *******************/

func TestIdleTimeoutWorkerNotAssigned(t *testing.T) {
	fmt.Println ("TestIdleTimeoutWorkerNotAssigned begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

	hostname := testutil.GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                t.Fatal("Error starting Mux:", err)
                return
        }

	db.SetMaxIdleConns(0)
	defer db.Close()

	fmt.Println ("Verify idle timeout will kick in for idle transaction");
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                t.Fatalf("Error getting connection %s\n", err.Error())
        }

        tx, _ := conn.BeginTx(ctx, nil)

        fmt.Println ("Insert a row and sleep for idle timeout to happen");
        stmt, _ := tx.PrepareContext(ctx, "/*cmd*/insert into test_simple_table_1 (ID, Name, Status) VALUES(:ID, :Name, :Status)")
        _, err = stmt.Exec(sql.Named("ID", 11), sql.Named("Name", "Lee"), sql.Named("Status", 11))
        if err != nil {
              t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
        }
        err = tx.Commit()
        if err != nil {
             t.Fatalf("Error commit %s\n", err.Error())
        }

	time.Sleep(7 * time.Second)
	fmt.Println ("Verify idle timeout event is seen in CAL and occ log");
        if ( testutil.RegexCount("PROXY.*Connection handler idle timeout") < 1) {
	   t.Fatalf ("Error: should have worker recycle");
	}

        time.Sleep(10 * time.Second)
        fmt.Println ("Check CAL log for worker restarted event, 1 event from the beginning and 1 due to max_lifespan");
        count := testutil.RegexCountFile ("E.*MUX.*idle_timeout_4000", "cal.log");
	if (count != 1) {
	    t.Fatalf ("Error: expected idle_timeout event");
	}
	
        stmt.Close()
        cancel()
        conn.Close()
	testutil.DoDefaultValidation(t)
	fmt.Println ("TestIdleTimeoutWorkerNotAssigned done  -------------------------------------------------------------")
}
