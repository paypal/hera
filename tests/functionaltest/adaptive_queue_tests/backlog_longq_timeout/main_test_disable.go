package main 
import (
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
	appcfg["child.executable"] = "mysqlworker"
	appcfg["backlog_pct"] = "100"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "4"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
        return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}

/**-----------------------------------------
   Helper method to insert a row with delay commit
--------------------------------------------*/
func insert_delay_commit (id string, wait_second int) {
        fmt.Println ("Insert a row, commit later")
        testutil.RunDMLCommitLater("insert into test_simple_table_1 (ID, Name, Status) VALUES (" + id + ", 'Smith', 111)", wait_second)
}

/**-----------------------------------------
   Helper method to update a row with delay
--------------------------------------------*/
func update_row_delay_commit (id string, wait_second int) {
        fmt.Println ("Update a row, commit later")
        testutil.RunDMLCommitLater("update test_simple_table_1 set Name='Steve' where ID=123", wait_second)
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*##############################################################################
# No R/W configuration, default config of request_backlog_timeout, short_backlog_timeout
# Send many requests so request goes to backlog
# Verify correct default setting
# Verify some requests will be timed out 
###############################################################################*/

func TestBklgLongQTimeout(t *testing.T) {
	fmt.Println ("TestBklgLongQTimeout begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestBklgLongQTimeout begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	var id_num int
        fmt.Println ("Having 7 threads to do insert, sleep & commit")
        fmt.Println ("Since first thread does not commit, requests from other threads will go to backlog and some get timeout")
	insert_count := 7;
        for i := 0; i < insert_count ; i++ {
                id_num = 100 + i
                go insert_delay_commit(strconv.Itoa(id_num), 5)
                time.Sleep(100 * time.Millisecond);
        }

        time.Sleep(7 * time.Second);
	fmt.Println ("Verify correct default request_backlog_timeout")
        if (testutil.RegexCount ("add to backlog. type: 0 , instance: 0  timeout: 1000 , blgsize: 0") == 0) {
            t.Fatalf ("Error: expected request_backlog_timeout=500");
        }
	fmt.Println ("Verify bklg_timeout events, no bklg eviction")
	count_bklg_timeout := testutil.RegexCountFile ("WARNING.*bklg0_timeout", "cal.log");
        if (count_bklg_timeout < 2) {
            t.Fatalf ("Error: expected 2 or more bklg0_timeout events");
        }
        count := testutil.RegexCountFile ("WARNING.*bklg0_eviction", "cal.log");
        if (count > 0) {
            t.Fatalf ("Error: NOT expected bklg0_eviction  event");
        }
	if (testutil.RegexCountFile ("QUEUE.*aqbklg", "cal.log") == 0) {
            t.Fatalf ("Error: expected QUEUE.*aqbklg  event");
	}
	fmt.Println ("Verify no bounce")
	if (testutil.RegexCountFile ("WARNING.*bouncer_activate_2", "cal.log") > 0) {
            t.Fatalf ("Error: expected no request bouncing");
        }

	fmt.Println ("Verify bklg_timeout error is returned to client")
        if ( testutil.RegexCount("error to client.*HERA-100: backlog timeout") < 1) {
	   t.Fatalf ("Error: should get backlog timeout error");
	}

	for i := 0; i < insert_count-count_bklg_timeout ; i++ {
                id_num = 100 + i
		row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = " + strconv.Itoa(id_num));
                fmt.Printf ("row_count: %d\n", row_count);
                if (row_count < 1) {
                        t.Fatalf ("Error: expected row in DB");
                }
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

	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestBklgLongQTimeout done  -------------------------------------------------------------")
}
