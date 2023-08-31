package main 
import (
	"fmt"
	"os"
	"testing"
	"time"
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
        appcfg["idle_timeout_ms"] = "60000" //Large idletimout so that the server does not close the connection
        appcfg["request_backlog_timeout"] = "60000"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "1"
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
   Helper method to update a row 
--------------------------------------------*/
func update_row (id string) {
        fmt.Println ("Update a row, set to different name")
        testutil.RunDML1 ("update test_simple_table_1 set Name='Steve' where ID=" + id)
}

/**-----------------------------------------
   Helper method to load a row 
--------------------------------------------*/
func load_row (id string) {
        fmt.Println ("Load the row in test_simple_table_1")
	testutil.Fetch ("Select Status from test_simple_table_1 where name = 'Steve' and ID = " + id);
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*
 * Steps:
 *   First client to insert row to a table but not commit
 *   second client to update the row 
 *   Third client to load the row and verify row is updated 
 */


func TestNoFreeWorker3(t *testing.T) {
	fmt.Println ("TestNoFreeWorker3 begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestNoFreeWorker3 begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

        id := "123";
	fmt.Println ("First client to insert a row, but delay commit");
	go insert_row_delay_commit(id, 5)

        time.Sleep(1 * time.Second);
	fmt.Println ("Second client to update the row, set name to 'Steve'");
	go update_row (id)

        time.Sleep(1 * time.Second);
	fmt.Println ("Third client to load the row in test_simple_table_1");
        row_count := testutil.Fetch ("Select Status from test_simple_table_1 where name = 'Steve' and ID = " + id);
	fmt.Printf ("row_count: %d", row_count);

        time.Sleep(6 * time.Second);
        fmt.Println ("Since there is 1 worker, update and select requests are sent to back log queue");
        if ( testutil.RegexCount("add to backlog. type: 0") < 2) {
           t.Fatalf ("Error: request is not sent to backlog queue");
        }
        fmt.Println ("Verify requests in backlog queue get processed");
        if ( testutil.RegexCount("exiting backlog. type: 0") < 2) {
           t.Fatalf ("Error: should see request in backlog queue get processed");
        }
        fmt.Println ("Verify bklg events")
        count := testutil.RegexCountFile ("E.*BKLG0_long.*", "cal.log")
        if ( count < 2) {
            t.Fatalf ("Error: expected 2 bklg event");
        }

	logger.GetLogger().Log(logger.Debug, "TestNoFreeWorker3 done  -------------------------------------------------------------")
}
