package main 
import (
	"strconv"
	"fmt"
	"os"
	"testing"
	"time"
        _"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/bind_eviction_tests/util"
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
        appcfg["soft_eviction_effective_time"] = "500"
        appcfg["soft_eviction_probability"] = "100"
        appcfg["bind_eviction_max_throttle"] = "10"
        appcfg["opscfg.default.server.saturation_recover_threshold"] = "10" 
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "80" //Killing interval = 1000/(3*80) = 416ms

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "10"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
        return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}

func TestMain(m *testing.M) {
        os.Exit(testutil.UtilMain(m, cfg, setupDb))
}


/*
 * Dynamic Throttle Testing, Throttle decreased 
 *
 */


func TestBindThrottle(t *testing.T) {
	fmt.Println ("TestBindThrottle begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestBindThrottle begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	testutil.RunDML1("insert into test_simple_table_1 (ID, Name, Status) VALUES (12345699, 'Jack', 100)")
	

	fmt.Println ("First thread to insert a row, but commit later");
        id := "12345688"
        go util.InsertBinding(id, 5)

        id1 := 22345699
	fmt.Println ("Insert a row with id: ", id1);
	util.InsertBinding(strconv.Itoa(id1), 0)

        fmt.Println ("Having threads to select...for update on same row, all workers are busy")
        for i := 0; i < 15; i++ {
                time.Sleep(200 * time.Millisecond);
                go util.FetchBinding(id, "FOR UPDATE");
        }
        time.Sleep(20 * time.Second);
	for i := 0; i < 5; i++ {
                go util.FetchBinding(strconv.Itoa(id1), "FOR UPDATE");
        }
        for i := 0; i < 5; i++ {
                time.Sleep(200 * time.Millisecond);
                go util.FetchBinding(id, "FOR UPDATE");
        }

	time.Sleep(10 * time.Second);

	fmt.Println ("Verify BIND_EVICT events")
	count := testutil.RegexCountFile ("BIND_EVICT", "cal.log")
	if ( count < 8) {
            t.Fatalf ("Error: expected at least 8 BIND_EVICT events");
        }
	fmt.Println ("Verify BIND_THROTTLE events")
	hcount := testutil.RegexCountFile ("BIND_THROTTLE", "cal.log")
	if ( hcount < 5) {
            t.Fatalf ("Error: expected BIND_THROTTLE events");
        }
	fmt.Println ("Verify bind throttle decr")
	cnt := testutil.RegexCount ("bind throttle decr hash:3378653297.*allowEveryX:28-20" )
	if ( cnt < 1) {
            t.Fatalf ("Error: expected bind throttle decr - allowEveryX:28-20");
        }
	cnt = testutil.RegexCount ("bind throttle decr hash:3378653297.*allowEveryX:8-2" )
	if ( cnt < 1) {
            t.Fatalf ("Error: expected bind throttle decr - allowEveryX:28-20");
        }
	/*count := testutil.RegexCountFile ("STRANDED.*RECOVERED_SATURATION_RECOVERED", "cal.log")
	if ( count < hcount) {
            t.Fatalf ("Error: expected %d RECOVERED_SATURATION_RECOVERED events", hcount);
        }*/
        if ( testutil.RegexCountFile ("RECOVER.*dedicated", "cal.log") < count ) {
            t.Fatalf ("Error: expected %d recover  event", count);
        }

	fmt.Println ("Verify bind eviction error is returned to client")
        if ( testutil.RegexCount("Responded to client.*HERA-105: bind throttle") < hcount) {
	   t.Fatalf ("Error: should get correct # of HERA-105: bind throttle error: %d", hcount);
	}
	fmt.Println ("Verify ROLLBACK for BIND_EVICTION event")
        if ( testutil.RegexCountFile ("ROLLBACK", "cal.log") < count ) {
            t.Fatalf ("Error: expected %d ROLLBACK  event", count);
        }

	
	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestBindThrottle done  -------------------------------------------------------------")
}
