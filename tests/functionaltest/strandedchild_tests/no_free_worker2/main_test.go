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
        appcfg["bouncer_enabled"] = "true"
        appcfg["idle_timeout_ms"] = "60000"
        appcfg["request_backlog_timeout"] = "60000"

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
   Helper method to load a row 
--------------------------------------------*/
func load_row (id string) {
        fmt.Println ("Load the row in test_simple_table_1")
	testutil.Fetch ("Select Name from test_simple_table_1 where ID = " + id);
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*
 * Steps:
 *   Three threads perform an insert to a table but not commit
 *   Since we have 2 workers to serve 2 insert requests, the third requests will go to backlog queue. 
 *   Verify request in backlog queue is successfully processed 
 */


func TestNoFreeWorker2(t *testing.T) {
	fmt.Println ("TestNoFreeWorker2 begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestNoFreeWorker2 begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	fmt.Println ("Three threads to insert a row, but not commit");
        id := 123;
	for i := 0; i < 3; i++ {
		id_str := strconv.Itoa (id) 
        	go insert_row_delay_commit(id_str, 3)
		id = id+1
                time.Sleep(100 * time.Millisecond);
	}

        time.Sleep(7 * time.Second);
        fmt.Println ("Since there are only 2 workers,  request is sent to back log queue");
        if ( testutil.RegexCount("add to backlog. type: 0") < 1) {
           t.Fatalf ("Error: request is not sent to backlog queue");
        }
        fmt.Println ("Verify request in backlog queue gets processed");
        if ( testutil.RegexCount("exiting backlog. type: 0") < 1) {
           t.Fatalf ("Error: should see request in backlog queue get processed");
        }
        fmt.Println ("Verify bklg events in CAL for request (in queue) that get processed")
        count := testutil.RegexCountFile ("E.*BKLG0_long.*", "cal.log")
        if ( count < 1) {
            t.Fatalf ("Error: expected 1 bklg event");
        }
        fmt.Println ("Verify 3 inserted rows")
	id = 123;
        for i := 0; i < 3; i++ {
		id_str1 := strconv.Itoa (id) 
		row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = " + id_str1);
		fmt.Printf ("row_count: %d\n", row_count);
		if (row_count < 1) {
			t.Fatalf ("Error: expected row in DB");
		}
		id = id + 1;
        }

	logger.GetLogger().Log(logger.Debug, "TestNoFreeWorker2 done  -------------------------------------------------------------")
}
