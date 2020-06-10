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
        appcfg["request_backlog_timeout"] = "60000"
        appcfg["bouncer_enabled"] = "true"
	// Bouncer will be activated after 10 x 4000ms
        appcfg["num_polls_before_activation"] = "10"
        appcfg["bouncer_poll_interval_ms"] = "4000"
        appcfg["idle_timeout_ms"] = "20000"

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

/***********************************
 *  This test case tests the scenario when the bouncer is not activated at the startup
 *  The bouncer wont be active for (num_polls_before_activation) * (bouncer_poll_interval)
 *  In this test case the bouncer wont be active for 4000ms * 10 = 40 seconds
 ***********************************/

func TestNumPollsBeforeBouncerActive(t *testing.T) {
	fmt.Println ("TestNumPollsBeforeBouncerActive begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        time.Sleep(10 * time.Second);
	fmt.Println ("First thread to insert a row, but not commit, so all 4 workers are busy");
        id := 123;
	for i := 0; i < 4; i++ {
		id_str := strconv.Itoa (id) 
        	go insert_row_delay_commit(id_str, 5)
		id = id+1
	}

        time.Sleep(2 * time.Millisecond);
        fmt.Println ("Having 50 threads to load row.")
        fmt.Println ("All workers are busy, bouncing must be triggerd 4 times before it really bounce connections")
        for i := 0; i < 50; i++ {
                time.Sleep(200 * time.Millisecond);
                go load_row ("123");
        }

        time.Sleep(6 * time.Second); //wait for 4 rows to commit successfully
	fmt.Println ("Verify requests are executed successfully after workers become avail ");
        id = 123;
        for i := 0; i < 4; i++ {
           id_str := strconv.Itoa (id) 
           row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = " + id_str);
           if (row_count != 1) {
                t.Fatalf ("Error: expected row is NOT there %d", id);
           }
           id = id+1
        }

        fmt.Println ("Verify none of the requests are bounced");
        if ( testutil.RegexCountFile ("WARNING.*Bounce", "cal.log") > 0 ) {
            t.Fatalf ("Error: expected No bouncing event");
        }

	testutil.DoDefaultValidation(t)

	logger.GetLogger().Log(logger.Debug, "TestNumPollsBeforeBouncerActive done  -------------------------------------------------------------")
}
