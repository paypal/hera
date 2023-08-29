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
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["rac_sql_interval"] = "10"
	appcfg["rac_restart_window"] = "120"
	appcfg["child.executable"] = "mysqlworker"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS hera_maint")
        err := testutil.RunDML("CREATE TABLE hera_maint (MACHINE varchar(512) not null, INST_ID int, MODULE VARCHAR(128), STATUS VARCHAR(1), STATUS_TIME INT, REMARKS varchar(64))")
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_2")
        err1 := testutil.RunDML("CREATE TABLE test_simple_table_2 (accountID VARCHAR(64) PRIMARY KEY, NAME VARCHAR(64), STATUS VARCHAR(64), CONDN VARCHAR(64))")
        if (err != nil || err1 != nil) {
            return err
        }
        return nil
}


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/* ##########################################################################################
# This test case verifies the implemenation of 'rac_restart_window' parameter implementation
# If 'rac_restart_window'=120 then all the workers in 'R' state should recycle within 120 seconds
# The proxy spreads out the restart time of the workers within this 120 seconds so that they dont all
# restart at the same time. 
# (1)Start the Hera Server, with max_connections = 3
# (2)Update the hera_maint table in DB to R state
# (3)Verify Only one worker restarted after 15 seconds, 2 have restarted after 50 seconds and 3 after 115 seconds
############################################################################################*/
func TestRacRestartWindowR(t *testing.T) {
	fmt.Println ("TestRacRestartWindowR begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestRacRestartWindowR begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

        err := testutil.SetRacNodeStatus ("R", "hera-test",  1)
        if err != nil {
                t.Fatalf("Error inserting RAC maint row  %s\n", err.Error())
        }
        time.Sleep(22 * time.Second)

	fmt.Println ("Verify mux detects RAC status change")
        if ( testutil.RegexCount ("Rac maint activating, worker") != 3) {
		 t.Fatalf ("Error: should have 3 Rac maint activating");
        }
	if ( testutil.RegexCount ("Lifespan exceeded, terminate") != 1) {
                 t.Fatalf ("Error: should have 1 'Lifespan exceeded, terminate' in log");
        }
        time.Sleep(50 * time.Second)
	if ( testutil.RegexCount ("Lifespan exceeded, terminate") != 2) {
                 t.Fatalf ("Error: should have 2 'Lifespan exceeded, terminate' in log");
        }
        time.Sleep(50 * time.Second)
	if ( testutil.RegexCount ("Lifespan exceeded, terminate") != 3) {
                 t.Fatalf ("Error: should have 3 'Lifespan exceeded, terminate' in log");
        }
	if ( testutil.RegexCountFile ("E.*RACMAINT_INFO_CHANGE.*0.*inst:0 status:R.*module:HERA-TEST", "cal.log") != 1) {
                 t.Fatalf ("Error: should have RACMAINT_INFO_CHANGE in CAL");
        }
	testutil.DoDefaultValidation(t);

	logger.GetLogger().Log(logger.Debug, "TestRacRestartWindowR done  -------------------------------------------------------------")
}

