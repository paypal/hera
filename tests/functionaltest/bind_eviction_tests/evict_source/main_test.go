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
        appcfg["soft_eviction_probability"] = "0" // disable soft evict
        appcfg["bind_eviction_max_throttle"] = "10"
	appcfg["bind_eviction_threshold_pct"] = "20" // 10*20/100 = 2
        appcfg["bind_eviction_target_conn_pct"] = "30"
        appcfg["opscfg.default.server.saturation_recover_threshold"] = "10" 
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "80"
        appcfg["backlog_pct"] = "0" // disable backlog
        appcfg["skip_eviction_host_prefix"] = "^heraa[0-9]+" 
        appcfg["eviction_host_prefix"] = "^herab[0-9]+"

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


func TestEvictSource(t *testing.T) {
	fmt.Println ("TestEvictSource begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestEvictSource begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	//Send 6 insert queries, same sqlhash but different bind KV, source from heraa so it can be ignored.
	testutil.GetClientInfo().Appname = "userapp1"
	testutil.GetClientInfo().Host = "heraa99userapp1"
	fmt.Println ("First 6 threads in wait to insert a row, holding txn for 25 seconds");
        id := 1000000
	idStr := strconv.Itoa(id)
	go util.InsertBinding(idStr, 25)
	go util.InsertBinding(strconv.Itoa(id+1), 25)
	go util.InsertBinding(strconv.Itoa(id+2), 25)
	go util.InsertBinding(strconv.Itoa(id+3), 25)
	go util.InsertBinding(strconv.Itoa(id+4), 25)
	go util.InsertBinding(strconv.Itoa(id+5), 25)
	time.Sleep(2* time.Second);

//	trigger the pool depletion and trigger bind eviction as same sqlhash and client info, but different bind KV
//	Each select for update will hold 10 seconds before closing txn.
//	This should trigger bind eviction, bind_eviction_threshold_pct(20) is 2 workers. 3 should work
        fmt.Println ("Having threads to select...for update on same row, all workers are busy")
	testutil.GetClientInfo().Appname = "userapp2"
	testutil.GetClientInfo().Host = "herab333userapp2"
        for i := 0; i < 6; i++ {
                time.Sleep(200 * time.Millisecond);
		tmp_id := id + i%6 // make each id repeat twice
		if i > 2 {
			testutil.GetClientInfo().Appname = "userapp3"
			testutil.GetClientInfo().Host = "herab555userapp3"
		}
		go util.FetchBindingWithDelay(strconv.Itoa(tmp_id), "FOR UPDATE", 8);
        }
//	id 1000000,1000001,1000002 are evicted. free worker becomes 3
//	now send more queries to saturate the the pool and get throttled based on clientinfo
//	this should trigger at least two heavy usage case 
	fmt.Println("Pause for 1 seconds and trigger the next batch for throttle") 
	time.Sleep(5 * time.Second);
	testutil.GetClientInfo().Appname = "userapp2"
	testutil.GetClientInfo().Host = "herab333userapp2"

        for i := 0; i < 12; i++ {
                time.Sleep(1000* time.Millisecond);
		tmp_id := id + i%6 // make each id repeat twice
		go util.FetchBindingWithDelay(strconv.Itoa(tmp_id), "FOR UPDATE", 0);
        }

        time.Sleep(15 * time.Second);
	fmt.Println ("Verify BIND_EVICT events")
	count := testutil.RegexCountFile ("BIND_EVICT", "cal.log")
	if ( count < 3) {
            t.Fatalf ("Error: expected at least 3 BIND_EVICT events");
        }
	fmt.Println ("Verify BIND_THROTTLE events")
	hcount := testutil.RegexCountFile ("BIND_THROTTLE", "cal.log")
	if ( hcount < 8) {
            t.Fatalf ("Error: expected BIND_THROTTLE events");
        }
	fmt.Println ("Verify bind throttle decr")
	cnt := testutil.RegexCount ("decr hash:3378653297 bindName:srcPrefixApp# val:herab333&userapp2 allowEveryX:31-6" )
	if ( cnt < 1) {
            t.Fatalf ("Error: expected bind throttle decr - allowEveryX:31-6")
        }
	cnt = testutil.RegexCount ("bind throttle decr hash:3378653297 bindName:srcPrefixApp# val:herab333&userapp2 allowEveryX:25-2" )
	if ( cnt < 1) {
            t.Fatalf ("Error: expected bind throttle decr - allowEveryX:25-2");
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
	logger.GetLogger().Log(logger.Debug, "TestEvictSource done  -------------------------------------------------------------")
}
