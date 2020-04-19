package main 
import (
	"fmt"
	"os"
	"testing"
	"time"
	"strconv"
        _"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*
The test will start Mysql server docker. Hera server connects to this Mysql DB docker
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
	appcfg["child.executable"] = "mysqlworker"
	appcfg["rac_sql_interval"] = "0"
        appcfg["request_backlog_timeout"] = "10000"
        appcfg["bouncer_enabled"] = "true"
        appcfg["backlog_pct"] = "0"
        appcfg["idle_timeout_ms"] = "60000"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "2"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
        return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}

/**-----------------------------------------
   Helper method to insert a row with delay
--------------------------------------------*/
func insert_row_delay_commit (id string, wait_second int) {
        fmt.Println ("Insert a row, commit later")
        testutil.RunDMLCommitLater("insert into test_simple_table_1 (ID, Name, Status) VALUES (" + id + ", 'Smith', 111)", wait_second)
}

/**-----------------------------------------
   Helper method to update a row with delay
--------------------------------------------*/
func update_row_delay_commit (id string, wait_second int) {
        fmt.Println ("Update a row, commit later")
        testutil.RunDMLCommitLater("update test_simple_table_1 set Name='Steve' where ID=" + id, wait_second)
}

/**-----------------------------------------
   Helper method to load a row 
--------------------------------------------*/
func load_row (id string) {
        fmt.Println ("Load the row in test_simple_table_1")
	testutil.Fetch ("Select Name from test_simple_table_1 where ID = " + id);
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}


/*********************
 ** All workers are busy and connections greater than workers are already accepted.
 ** A Worker gets freed up before the req_backlog_timeout happens.
 ** In CAL logs "BKLG *" means
 ** "BKLG 0" means the client waited in backlog queue between 0ms and (1/5)*request_backlog_timeout
 ** "BKLG 1" means the client waited in backlog queue between (1/5)*request_backlog_timeout 
 ** and (2/5)*request_backlog_timeout
 ** "BKLG 2" means the client waited in backlog queue between (2/5)*request_backlog_timeout 
 ** and (3/5)*request_backlog_timeout
 ** "BKLG 3" means the client waited in backlog queue between (3/5)*request_backlog_timeout 
 ** and (4/5)*request_backlog_timeout
 ** "BKLG 4" means the client waited in backlog queue between (4/5)*request_backlog_timeout 
 ** and request_backlog_timeout
 ** This Testcase checks the cal logs for "BKLOG 3", because the child process waits for 
 ** 7 seconds in backlog quee (the request_backlog_timeout is 10 seconds (10000 ms))
 **********************/

func TestNoFreeWorker5(t *testing.T) {
	fmt.Println ("TestNoFreeWorker5 begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestNoFreeWorker5 begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12346, 'Jack', 100)")

	fmt.Println ("Two threads insert a row, but not commit until 7 seconds later");
        id := 123;
	for i := 0; i < 2; i++ {
		id_str := strconv.Itoa (id) 
        	go insert_row_delay_commit(id_str, 7)
		id = id+1
	}

	// Load a row while row insertions is happening, this request will go to backlog
        time.Sleep(1 * time.Second);
        go load_row ("12346");

        time.Sleep(8 * time.Second);

	fmt.Println ("Verify BKLG(3) event")
	count := testutil.RegexCountFile ("BKLG0_long.*3", "cal.log")
	if ( count < 1) {
            t.Fatalf ("Error: expected BKLG0_long (3)  event");
        }
	fmt.Println ("Verify 3 inserted rows are in DB")
        id = 123;
        for i := 0; i < 2; i++ {
                id_str1 := strconv.Itoa (id) 
                row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = " + id_str1);
                fmt.Printf ("row_count: %d\n", row_count);
                if (row_count < 1) {
                        t.Fatalf ("Error: expected row in DB");
                }
                id = id + 1;
        }

	logger.GetLogger().Log(logger.Debug, "TestNoFreeWorker5 done  -------------------------------------------------------------")
}
