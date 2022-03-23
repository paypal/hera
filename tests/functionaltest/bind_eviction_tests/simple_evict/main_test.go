package main 
import (
	"context"
        "database/sql"
	"fmt"
	"os"
	"testing"
	"time"
        "github.com/paypal/hera/client/gosqldriver"
        _"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*
The test will start Mysql server docker and Hera connects to this Mysql DB docker
No setup needed

*/

var mx testutil.Mux
var appname string
func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to choose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["rac_sql_interval"] = "0"
        appcfg["request_backlog_timeout"] = "6000"
        appcfg["soft_eviction_effective_time"] = "500"
        appcfg["soft_eviction_probability"] = "100"
        appcfg["opscfg.default.server.saturation_recover_threshold"] = "10" 
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "80" //Killing interval = 1000/(3*80) = 416ms

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "5"
	opscfg["opscfg.default.server.log_level"] = "5"
	appname = "simple_evict"
	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
        return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}

/**-----------------------------------------
   Helper method to insert a row with delay
--------------------------------------------*/
func insert_row_delay_commit (id string, wait_second int) error {
        fmt.Println ("Insert a row, commit later")
        status := 999
        hostname,_ := os.Hostname()
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
        mux := gosqldriver.InnerConn(conn)
        err= mux.SetClientInfo(appname, hostname)
        if err != nil {
                fmt.Println("Error sending Client Info:", err)
        }
        tx, _ := conn.BeginTx(ctx, nil)
        stmt, _ := tx.PrepareContext(ctx, "insert into test_simple_table_1 (ID, Name, Status) VALUES(:ID, :Name, :Status)")
        if err != nil {
                fmt.Println("Error Preparing context:", err)
        }
        defer stmt.Close()
	_, err = stmt.Exec(sql.Named("ID", id), sql.Named("Name", "Lee"), sql.Named("Status", status))
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
   Helper method to update a row with delay
--------------------------------------------*/
func update_row_delay_commit (id string, wait_second int) error {
        hostname,_ := os.Hostname()
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
        // cancel must be called before conn.Close()
        defer cancel()
        mux := gosqldriver.InnerConn(conn)
        err= mux.SetClientInfo(appname, hostname)
        if err != nil {
                fmt.Println("Error sending Client Info:", err)
        }

        tx, _ := conn.BeginTx(ctx, nil)
        stmt, _ := tx.PrepareContext(ctx, "update test_simple_table_1 set Name='Steve' where ID=:ID")
	if err != nil {
                fmt.Println("Error Pereparing context:", err)
        }
        defer stmt.Close()
        _, err = stmt.Exec(sql.Named("ID", id))
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


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*
 *  Testing Simple Bind Eviction
 *
 */


func TestSimpleBindEviction(t *testing.T) {
	fmt.Println ("TestSimpleBindEviction begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestSimpleBindEviction begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12345699, 'Jack', 100)")
	

	fmt.Println ("First thread to insert a row, but commit later");
        id := "12345678"
        go insert_row_delay_commit(id, 5)

        id1 := "01234599"
	fmt.Println ("Insert a row with id: ", id1);
	insert_row_delay_commit(id1, 0)

        fmt.Println ("Having 6 threads to update same row, so all workers are busy")
        id = "12345678"
        for i := 0; i < 6; i++ {
                time.Sleep(200 * time.Millisecond);
                go update_row_delay_commit(id, 3)
        }

        time.Sleep(8 * time.Second);
        fmt.Println ("Verify fetch requests are fine");
        row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = " + id1);
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is there");
	}

        fmt.Println ("Verify eviction query is not in DB");
        row_count = testutil.Fetch ("Select Name from test_simple_table_1 where ID = " + id);
        if (row_count > 0) {
            t.Fatalf ("Error: row SHOULD NOT be in DB");
	}
	fmt.Println ("Verify BIND_EVICT events")
	count := testutil.RegexCountFile ("BIND_EVICT.*4271705786.*v=12345678", "cal.log")
	if ( count < 0  || count > 1) {
            t.Fatalf ("Error: expected 1 BIND_EVICT event for insert query");
        }

	hcount := testutil.RegexCountFile ("BIND_EVICT.*3271914668.*v=12345678", "cal.log")
	if ( hcount < 4) {
            t.Fatalf ("Error: expected at least 4 BIND_EVICT events for update query");
        }
	if ( testutil.RegexCountFile ("STRANDED.*RECOVERING", "cal.log") < count + hcount) {
            t.Fatalf ("Error: expected %d STRANDED.*RECOVERING events", hcount + count);
        }
        if ( testutil.RegexCountFile ("RECOVER.*dedicated", "cal.log") < hcount+count ) {
            t.Fatalf ("Error: expected %d recover  event", hcount+count);
        }

	fmt.Println ("Verify bind eviction errors are returned to client")
        if ( testutil.RegexCount("error to client.*HERA-106") < count + hcount) {
	   t.Fatalf ("Error: client should get %d bind eviction error", hcount+count);
	}

	fmt.Println ("Verify requests got rejected by bind evviction are rolled back")
        if ( testutil.RegexCountFile("ROLLBACK.*Local.*0", "cal.log") < count + hcount) {
	   t.Fatalf ("Error: client should get %d rollback events", hcount+count);
	}

	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestSimpleBindEviction done  -------------------------------------------------------------")
}
