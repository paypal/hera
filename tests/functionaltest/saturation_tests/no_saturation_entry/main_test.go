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

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
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


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*
 *  Verify saturation is NOT triggered when saturation entry is not in opscfg 
 */


func NoSaturationEntry(t *testing.T) {
	fmt.Println ("NoSaturationEntry begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "NoSaturationEntry begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	fmt.Println ("First thread to insert a row, but not commit");
        id := "123"
        go insert_row_delay_commit(id, 5)

        fmt.Println ("Having 5 threads to update same row.")
        fmt.Println ("Since first thread does not commit, they will have long query running")
        for i := 0; i < 5; i++ {
                time.Sleep(200 * time.Millisecond);
                go update_row_delay_commit(id, 3)
        }

        time.Sleep(7 * time.Second);

        fmt.Println ("Verify update requests work as expected");
        row_count := testutil.Fetch ("Select Name from test_simple_table_1 where Name = 'Steve' and ID = " + id);
        if (row_count != 1) {
            t.Fatalf ("Error: expected row is in DB");
	}

	fmt.Println ("Verify no SATURATION events is seen")
	hcount := testutil.RegexCountFile ("HARD_EVICTION", "cal.log")
	if ( hcount > 0) {
            t.Fatalf ("Error: expected no HARD_EVICTION events");
        }
	count := testutil.RegexCountFile ("STRANDED.*RECOVERED_SATURATION_RECOVERED", "cal.log")
	if ( count > 0) {
            t.Fatalf ("Error: should not get %d RECOVERED_SATURATION_RECOVERED events", count);
        }
        if ( testutil.RegexCountFile ("SATURATION_RECYCLED", "cal.log") > 0 ) {
            t.Fatalf ("Error: NOT expected to have SATURATION_RECYCLED  event" );
        }
        if ( testutil.RegexCountFile ("RECOVER.*dedicated", "cal.log") > 0 ) {
            t.Fatalf ("Error: NOT expected to have recover  event" );
        }
        if ( testutil.RegexCountFile ("WARNING.*bklg0_timeout", "cal.log") < 0 ) {
            t.Fatalf ("Error: should see backlog timeout event" );
        }

	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "NoSaturationEntry done  -------------------------------------------------------------")
}
