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
        appcfg["idle_timeout_ms"] = "600000"

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
 *   Four threads perform an insert to a table but not commit
 *   Since we have 2 workers to serve 2 insert requests, the other 2 requests will go to backlog queue 
 *   and timeout 
 */


func TestNoFreeWorker1(t *testing.T) {
	fmt.Println ("TestNoFreeWorker1 begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestNoFreeWorker1 begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12346, 'Jack', 100)")

	fmt.Println ("Four threads to insert a row, but not commit, to send all 2 workers in busy state");
        id := 123;
	for i := 0; i < 4; i++ {
		id_str := strconv.Itoa (id) 
        	go insert_row_delay_commit(id_str, 4)
		id = id+1
	}

        time.Sleep(5 * time.Second);
        fmt.Println ("Since there are only 2 workers, 2 requests are sent to back log queue");
        if ( testutil.RegexCount("add to backlog. type: 0") < 2) {
	   t.Fatalf ("Error: request is not sent to backlog queue");
	}
        fmt.Println ("Verify requests in backlog queue are timed out");
        if ( testutil.RegexCount("no worker HERA-100: backlog timeout") < 2) {
	   t.Fatalf ("Error: should see backlog timeout");
	}

	fmt.Println ("Verify bklg_timeout events")
	count := testutil.RegexCountFile ("WARNING.*bklg0_timeout", "cal.log")
	if ( count < 2) {
            t.Fatalf ("Error: expected 2 bklg0_timeout events");
        }

	logger.GetLogger().Log(logger.Debug, "TestNoFreeWorker1 done  -------------------------------------------------------------")
}
