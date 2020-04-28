package main 
import (
	"fmt"
	"os"
	"testing"
        _"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
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
        appcfg["soft_eviction_probability"] = "80"
        appcfg["opscfg.default.server.saturation_recover_threshold"] = "0" 
        appcfg["opscfg.default.server.saturation_recover_throttle_rate"] = "-1" 

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "2"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	return  nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*
 * Test when saturation_recover_throttle_rate is negative 
 * It will be set to big number, so same as saturation disabled 
 */

func TestNegativeThrottleRate(t *testing.T) {
	str := "saturation_recover check.*9223372036854775807"
        if ( testutil.RegexCount(str) < 1) {
           t.Fatalf ("Error: should see %s in log", str)
        }
	testutil.DoDefaultValidation(t)
	fmt.Println ("TestNegativeThrottleRate is done")
}
