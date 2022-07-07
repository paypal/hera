package main 
import (
	"context"
	"database/sql"
//	"strconv"
	"fmt"
	"os"
	"testing"
	"time"
        _"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
//	"github.com/paypal/hera/tests/functionaltest/bind_eviction_tests/util"
	"github.com/paypal/hera/utility/logger"
)

/*
The test will start Mysql server docker and Hera connects to this Mysql DB docker
No setup needed

*/

var mx testutil.Mux

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["rac_sql_interval"] = "0"
        appcfg["request_backlog_timeout"] = "6000"
	appcfg["bind_eviction_threshold_pct"] = "40"
        appcfg["soft_eviction_effective_time"] = "500"
        appcfg["soft_eviction_probability"] = "100"
        appcfg["opscfg.default.server.saturation_recover_threshold"] = "10" 
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "80" //Killing interval = 1000/(3*80) = 416ms

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "5"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
        return testutil.RunDML("CREATE TABLE test_simple_table_1 (AccountNumber INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}

/**-----------------------------------------
   Helper function to update (inclause) a row in test_simple_table_1 with delay
--------------------------------------------*/
func UpdateInclause (account_number string, wait_second int) error {
        hostname := testutil.GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                fmt.Println("Error connecting to OCC:", err)
                return err
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                return err
        }
        defer conn.Close()
        defer cancel()
        tx, _ := conn.BeginTx(ctx, nil)
        stmt, _ := tx.PrepareContext(ctx, "update test_simple_table_1 set Name='Steve' where AccountNumber in (:AccountNumber1, :AccountNumber2, :AccountNumber3, :AccountNumber4)")
        if err != nil {
                fmt.Println("Error Pereparing context:", err)
        }
        defer stmt.Close()
        _, err = stmt.Exec(sql.Named("AccountNumber1", account_number), sql.Named("AccountNumber2", "99999999"), sql.Named("AccountNumber3", "88888888"), sql.Named("AccountNumber4","77777777" ))
        if err != nil {
                return err
        }
        time.Sleep (time.Duration(wait_second) * time.Second)
        err = tx.Commit()
        if err != nil {
                return err
        }

        return nil
}
/**-----------------------------------------
   Helper function to insert a row to test_simple_table_1 with delay
--------------------------------------------*/
func InsertBinding (account string, wait_second int) error {
        fmt.Println ("Insert a row, commit later")
        status := 9999 
        hostname := testutil.GetHostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                fmt.Println("Error connecting to OCC:", err)
                return err
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                return err
        }
        defer conn.Close()
        defer cancel()
        tx, _ := conn.BeginTx(ctx, nil)
        stmt, _ := tx.PrepareContext(ctx, "insert into test_simple_table_1 (AccountNumber, Name, Status) VALUES(:AccountNumber, :Name, :Status)")
        if err != nil {
                fmt.Println("Error Preparing context:", err)
        }
        defer stmt.Close()
        _, err = stmt.Exec(sql.Named("AccountNumber", account), sql.Named("Name", "Lee"), sql.Named("Status", status))
        if err != nil {
                return err
        }
        time.Sleep (time.Duration(wait_second) * time.Second)
        err = tx.Commit()
        if err != nil {
                fmt.Println("Error commiting row insertion:", err)
                return err
        }

        return nil
}


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*
 *  Testing Bind Eviction with InClause query
 *
 */

func TestInclauseEviction(t *testing.T) {
	fmt.Println ("TestInclauseEviction begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestInclauseEviction begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	testutil.RunDML1("insert into test_simple_table_1 (AccountNumber, Name, Status) VALUES (12345699, 'Jack', 100)")
	
        account_number0 := "01234599"
	fmt.Println ("Insert a row with account_number: ", account_number0);
	InsertBinding(account_number0, 0)

	fmt.Println ("First thread to insert a row, but commit later");
        account_number1 := "12345678"
        go  InsertBinding(account_number1, 5)


        fmt.Println ("Having 6 threads to update same row, so all workers are busy")
        account_number := "66666666"
        for i := 0; i < 6; i++ {
                time.Sleep(200 * time.Millisecond);
                go UpdateInclause(account_number, 3)
        }

        time.Sleep(8 * time.Second);
        fmt.Println ("Verify fetch requests are fine");
        row_count := testutil.Fetch ("Select Name from test_simple_table_1 where AccountNumber = " + account_number0);
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is there");
	}
	
        fmt.Println ("Verify insert query is not evicted due to evict thresohold");
        row_count = testutil.Fetch ("Select Name from test_simple_table_1 where AccountNumber = " + account_number1);
        if (row_count != 1) {
            t.Fatalf ("Error: insert row SHOULD  be in DB");
        }

	fmt.Println ("Verify BIND_EVICT events")
	hcount := testutil.RegexCountFile ("BIND_EVICT.*4182107863.*k=AccountNumber", "cal.log")
	if ( hcount < 4) {
            t.Fatalf ("Error: expected at least 4 BIND_EVICT events");
        }
	fmt.Println ("Verify BIND_THROTTLE event")
	tcount := testutil.RegexCountFile ("BIND_THROTTLE.*4182107863.*k=AccountNumber", "cal.log")
	if ( tcount < 1) {
            t.Fatalf ("Error: expected 1 BIND_THROTTLE events");
        }
	count := testutil.RegexCountFile ("STRANDED.*RECOVERING", "cal.log")
	if ( count < hcount) {
            t.Fatalf ("Error: expected %d STRANDED_RECOVERED events", hcount);
        }
        if ( testutil.RegexCountFile ("RECOVER.*dedicated", "cal.log") < hcount ) {
            t.Fatalf ("Error: expected %d recover  event", hcount);
        }
	fmt.Println ("Verify evicted query is rolled back")
        if ( testutil.RegexCountFile ("ROLLBACK", "cal.log") < hcount ) {
            t.Fatalf ("Error: expected %d ROLLBACK  event", hcount);
        }

	fmt.Println ("Verify bind eviction error is returned to client")
        if ( testutil.RegexCount("error to client.*HERA-106: bind eviction") < hcount) {
	   t.Fatalf ("Error: client should get bind eviction error");
	}
	fmt.Println ("Verify bind throttle error is returned to client")
        if ( testutil.RegexCount("Responded to client.*HERA-105: bind throttle") < tcount) {
	   t.Fatalf ("Error: client should get bind throttle error");
	}
	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestInclauseEviction done  -------------------------------------------------------------")
}
