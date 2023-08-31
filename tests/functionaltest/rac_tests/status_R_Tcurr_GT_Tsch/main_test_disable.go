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

The test will start Mysql docker and Hera connects to this Mysql DB docker
No setup needed

*/
var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to choose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["rac_sql_interval"] = "5"
	appcfg["child.executable"] = "mysqlworker"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "1"
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

/* #####################################################################################
 #  Verify mux will not detect status change 'R' when rac maint time < current time
 #######################################################################################*/

func TestStatusU_to_R_Tcurr_GT_Tsch(t *testing.T) {
	fmt.Println ("TestStatusU_to_R_Tcurr_GT_Tsch begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestStatusU_to_R_Tcurr_GT_Tsch begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	fmt.Println ("Set Rac time 60 seconds before current time")
        err := testutil.SetRacNodeStatus ("R", "hera-test",  -60)
        if err != nil {
                t.Fatalf("Error inserting RAC maint row  %s\n", err.Error())
        }

        time.Sleep(20 * time.Second)

	fmt.Println ("Verify mux does not detect RAC status change")
        if ( testutil.RegexCount ("Rac maint activating, worker 0") != 0) {
		 t.Fatalf ("Error: should NOT have Rac maint activating");
        }

        fmt.Println ("Verify CAL log still have RACMAINT_INFO_CHANGE events");
	count := testutil.RegexCountFile ( "E.*RACMAINT_INFO_CHANGE.*0.*inst:0 status:R.*module:HERA-TEST", "cal.log");
        if ( count != 1 ) {
		t.Fatalf ("Error: should have RACMAINT_INFO_CHANGE event");
        }
        fmt.Printf ("Verify worker does not retart")
        if ( testutil.RegexCount ("Lifespan exceeded, terminate") != 0) {
		 t.Fatalf ("Error: should NOT have 'Lifespan exceeded, terminate' in log");
        }
	fmt.Printf ("Verify no RAC_ID and DB_UNAME cal event")
        if ( testutil.RegexCountFile("E.*RAC_ID.*0.*0", "cal.log") > 0) {
           t.Fatalf ("Error: should NOT have RAC_ID event");
        }

        if ( testutil.RegexCountFile ("E.*DB_UNAME.*MyDB.*0", "cal.log") > 0) {
            t.Fatalf ("Error: should NOT see DB_UNAME event");
        }

        fmt.Printf ("Verify RACMAINT_INFO is logged")
        if ( testutil.RegexCountFile("E.*RACMAINT_INFO[[:space:]].*0", "cal.log") !=  1) {
           t.Fatalf ("Error: should have 1 RACMAINT_INFO events");
        }
	testutil.DoDefaultValidation(t);
	logger.GetLogger().Log(logger.Debug, "TestStatusU_to_R_Tcurr_GT_Tsch done  -------------------------------------------------------------")
}

