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
        appcfg["bouncer_startup_delay"] = "2"
        appcfg["backlog_pct"] = "50"   //backlog queue size = 50*4/100 = 2
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

/****************
 Bouncer will be activated either when (max connections are reached) or (when the bklg queue size is reached
 in ALL of the proxies)
 backlog queuesize = (backlog_pct * max_connections/100)

 ****************/
func TestBouncerBacklogPct(t *testing.T) {
	fmt.Println ("TestBouncerBacklogPct begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        time.Sleep(3 * time.Second); //Wait for bouncer_startup_delay for bouncer to kickin
	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12346, 'Jack', 100)")

	fmt.Println ("Six threads to do insert with commit delay, so all 4 workers are busy, 2 requests go to backlog");
        id := 123;
	for i := 0; i < 6; i++ {
		id_str := strconv.Itoa (id) 
        	go insert_row_delay_commit(id_str, 5)
                time.Sleep(200 * time.Millisecond);
		id = id+1
	}

        time.Sleep(2 * time.Millisecond);
        fmt.Println ("Having 4 threads to load row.")
        fmt.Println ("All workers are busy, bouncing must be triggerd 4 times before it really bounce connections")
        for i := 0; i < 4; i++ {
                time.Sleep(200 * time.Millisecond);
                go load_row ("123");
        }

        time.Sleep(1 * time.Second);
        fmt.Println ("Verify next request is bounced");
        testutil.Fetch ("Select Name from test_simple_table_1 where ID = 12346");
	
        time.Sleep(8 * time.Second); //wait for 4 rows to commit successfully
	id = 123;
        for i := 0; i < 4; i++ {
           id_str := strconv.Itoa (id) 
           row_count := testutil.Fetch ("Select Name from test_simple_table_1 where ID = " + id_str);
           if (row_count != 1) {
                t.Fatalf ("Error: expected row is NOT there for id = %d", id);
           }
           id = id+1
        }

        fmt.Println ("Verify correct bouncing message in log");
        if ( testutil.RegexCount (" bouncer checking checktime.*checkcount= 3  activated= false  msg= out of worker capacity") < 1 ) {
            t.Fatalf ("Error: expected bouncing logging");
        }

        fmt.Println ("Verify out of worker event in CAL");
        if ( testutil.RegexCountFile ("WARNING.*bouncer_activate_2.*reason=out of worker capacity", "cal.log") < 1 ) {
            t.Fatalf ("Error: expected bouncing event");
        }

        if ( testutil.RegexCountFile ("WARNING.*Bounce.*3.SERVER.BOUNCE.-1", "cal.log") < 1 ) {
            t.Fatalf ("Error: expected bouncing event");
        }
        fmt.Println ("Verify BKLG events in CAL");
        if ( testutil.RegexCountFile ("BKLG0_long", "cal.log") < 4 ) {
            t.Fatalf ("Error: expected bklg events");
        }
	testutil.DoDefaultValidation(t)

	fmt.Println ("TestBouncerBacklogPct Done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestBouncerBacklogPct done  -------------------------------------------------------------")
}
