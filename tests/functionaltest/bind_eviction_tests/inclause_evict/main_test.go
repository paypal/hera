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
	"github.com/paypal/hera/tests/functionaltest/bind_eviction_tests/util"
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
        return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}

/**-----------------------------------------
   Helper function to update (inclause) a row in test_simple_table_1 with delay
--------------------------------------------*/
func UpdateInclause (id string, wait_second int) error {
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
        tx, _ := conn.BeginTx(ctx, nil)
        stmt, _ := tx.PrepareContext(ctx, "update test_simple_table_1 set Name='Steve' where ID in (:ID1, :ID2, :ID3, :ID4)")
        if err != nil {
                fmt.Println("Error Pereparing context:", err)
        }
        defer stmt.Close()
        _, err = stmt.Exec(sql.Named("ID1", id), sql.Named("ID2", "99999999"), sql.Named("ID3", "88888888"), sql.Named("ID4","77777777" ))
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

func TestInclauseEviction(t *testing.T) {
	fmt.Println ("TestInclauseEviction begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestInclauseEviction begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12345699, 'Jack', 100)")
	
        id0 := "01234599"
	fmt.Println ("Insert a row with id: ", id0);
	util.InsertBinding(id0, 0)

	fmt.Println ("First thread to insert a row, but commit later");
        id := "12345678"
        go  util.InsertBinding(id, 5)


        fmt.Println ("Having 6 threads to update same row, so all workers are busy")
        id = "66666666"
        for i := 0; i < 6; i++ {
                time.Sleep(200 * time.Millisecond);
                go UpdateInclause(id, 3)
        }

        time.Sleep(6 * time.Second);
        fmt.Println ("Verify fetch requests are fine");
        row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = " + id0);
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is there");
	}
	fmt.Println ("Since we have only 3 workers, saturation will be kicked in to kill long running queries")

	fmt.Println ("Verify SATURATION events")
	hcount := testutil.RegexCountFile ("HARD_EVICTION", "cal.log")
	if ( hcount < 2) {
            t.Fatalf ("Error: expected at least 2 HARD_EVICTION events");
        }
	count := testutil.RegexCountFile ("STRANDED.*RECOVERED_SATURATION_RECOVERED", "cal.log")
	if ( count < hcount) {
            t.Fatalf ("Error: expected %d RECOVERED_SATURATION_RECOVERED events", hcount);
        }
        if ( testutil.RegexCountFile ("RECOVER.*dedicated", "cal.log") < hcount ) {
            t.Fatalf ("Error: expected %d recover  event", hcount);
        }

	fmt.Println ("Verify saturation error is returned to client")
        if ( testutil.RegexCount("error to client.*saturation kill") < hcount) {
	   t.Fatalf ("Error: should get saturation kill error");
	}
	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestInclauseEviction done  -------------------------------------------------------------")
}
