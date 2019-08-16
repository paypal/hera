package main 
import (
	"context"
        "database/sql"
	"fmt"
	"os"
	"testing"
	"strconv"
	"time"
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

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["rac_sql_interval"] = "0"
	appcfg["readonly_children_pct"] = "50"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["backlog_pct"] = "100"
	appcfg["request_backlog_timeout"] = "100"
        appcfg["soft_eviction_probability"] = "80"
        appcfg["short_backlog_timeout"] = "20"
        appcfg["idle_timeout_ms"] = "20"
        appcfg["lifo_short_backlog_que"] = "true"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "4"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS DAO_LOB_TEST")
        return testutil.RunDML("CREATE TABLE DAO_LOB_TEST (ID varchar(50), PAYLOAD_R MEDIUMTEXT, PAYLOAD_C LONGTEXT, PAYLOAD_B BLOB )")
}

func insert_fetch_delay (id string, wait_second int) {
        fmt.Println ("Insert a row & commit")
        testutil.RunDML1("insert into dao_lob_test (ID) VALUES ('" + id + "')")
        fetch_delay ("select id from dao_lob_test");
}

func fetch_delay (query string) (int) {
        count := 0;
        hostname,_ := os.Hostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                fmt.Println("Error connecting to OCC:", err)
                return count
        }
        //db.SetMaxIdleConns(0)
        defer db.Close()

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                fmt.Println("Error creating context:", err)
                return count
        }
        defer conn.Close()
        // cancel must be called before conn.Close()
        defer cancel()
        stmt, _ := conn.PrepareContext(ctx, query)
        defer stmt.Close()
        rows, _ := stmt.Query()
        for rows.Next() {
                time.Sleep(200 * time.Millisecond);
                count++;
        }
        return count;
}

func TestMain(m *testing.M) {
	os.Setenv("PREFETCH_ROWS", "1")
        twoTask := os.Getenv("TWO_TASK")
        os.Setenv ("TWO_TASK_READ", twoTask)
        twoTask = os.Getenv("TWO_TASK_READ")
        fmt.Println ("TWO_TASK_READ: ", twoTask)
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*##############################################################################
# Single Target: R/W configuration, default config of request_backlog_timeout, short_backlog_timeout
# Send many requests so request goes to backlog
# Verify some requests in backlog will be processed and some of them are timed out
###############################################################################*/

func TestWriteNormalReadBklgEviction(t *testing.T) {
	fmt.Println ("TestWriteNormalReadBklgEviction begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestWriteNormalReadBklgEviction begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	var id_num int
        fmt.Println ("Having 17 threads to do insert, fetch 1 row, sleep")
        fmt.Println ("Since first thread does not commit, requests from other threads will go to backlog and some get timeout")
        for i := 0; i < 17; i++ {
                id_num = 100 + i
                go insert_fetch_delay(strconv.Itoa(id_num), 5)
                time.Sleep(80 * time.Millisecond);
        }

        time.Sleep(10 * time.Second);

	fmt.Println ("Verify BKLG & bklg_timeout events")
	if ( testutil.RegexCountFile ("WARNING.*bklg0_timeout", "cal.log") < 2) {
            t.Fatalf ("Error: expected 2 or more bklg0_timeout events");
        }
        if ( testutil.RegexCountFile ("WARNING.*bklg0_eviction", "cal.log") < 1) {
            t.Fatalf ("Error: expected bklg0_eviction event");
        }
	if (testutil.RegexCountFile ("QUEUE.*aqbklg", "cal.log") == 0) {
            t.Fatalf ("Error: expected QUEUE.*aqbklg  event");
	}
	/*fmt.Println ("Verify some requests in backlog queue get processed")
	if (testutil.RegexCountFile ("BKLG0_long", "cal.log") == 0) {
            t.Fatalf ("Error: expected BKLG0_long event");
	}*/

	fmt.Println ("Verify no bounce")
	if (testutil.RegexCountFile ("WARNING.*bouncer_activate_2", "cal.log") > 0) {
            t.Fatalf ("Error: expected no request bouncing");
        }


	fmt.Println ("Verify bklg_timeout error is returned to client")
        if ( testutil.RegexCount("error to client.*HERA-100: backlog timeout") < 1) {
	   t.Fatalf ("Error: should get backlog timeout error");
	}
        if ( testutil.RegexCount("error to client.*HERA-102: backlog eviction") < 1) {
	   t.Fatalf ("Error: should get backlog eviction error");
	}

        fmt.Println ("Verify next requests are fine");
        err := testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (999, 'Smith', 111)")
        if err != nil {
                t.Fatalf ("Error inserting row to test_simple_table_1");
        }
        row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = 999");
        if (row_count != 1) {
                t.Fatalf ("Error: should get 1 row in test_simple_table_1");
	}

	logger.GetLogger().Log(logger.Debug, "TestWriteNormalReadBklgEviction done  -------------------------------------------------------------")
}
