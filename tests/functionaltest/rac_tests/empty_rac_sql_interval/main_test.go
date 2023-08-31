package main 
import (
	"fmt"
	"os"
	"testing"
	"time"
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
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["rac_sql_interval"] = ""
	appcfg["child.executable"] = "mysqlworker"
	appcfg["database_type"] = "mysql"

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
 #  This test case tests rac feature is enabled when rac_sql_interval = ""
 #
 #######################################################################################*/

func TestEmptyRacInterval(t *testing.T) {
	fmt.Println ("TestEmptyRacInterval begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestEmptyRacInterval begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	fmt.Println ("Insert a row to table")
        testutil.RunDML("DELETE from test_simple_table_2")
        err := testutil.RunDML("insert into test_simple_table_2 (accountID, Name, Status) VALUES('12345', 'Linda Plump', 'Good')")
        if err != nil {
                t.Fatalf("Error inserting row to table %s\n", err.Error())

        }

        err = testutil.SetRacNodeStatus ("R", "hera-test",  1)
        if err != nil {
                t.Fatalf("Error inserting RAC maint row  %s\n", err.Error())
        }
        time.Sleep(10000 * time.Millisecond)

	fmt.Println ("Verify mux will NOT detect RAC status change")
        if ( testutil.RegexCount ("Rac maint activating") < 0) {
		 t.Fatalf ("Error: should have Rac maint activating");
        }

        fmt.Println ("Verify CAL log for RAC change events");
	count := testutil.RegexCountFile (  "E.*RACMAINT_INFO_CHANGE.*0.*inst:0 status:R.*module:HERA-TEST", "cal.log")
        if ( count < 0 ) {
		t.Fatalf ("Error: should have Rac maint event");
        }

        time.Sleep(6500 * time.Millisecond)
        fmt.Printf ("Verify worker retarted")
        if ( testutil.RegexCount ("Lifespan exceeded, terminate") < 0) {
		 t.Fatalf ("Error: should have 'Lifespan exceeded, terminate' in log");
        }
	row_count := testutil.Fetch ("Select Name from test_simple_table_2 where accountID = 12345");
        if (row_count != 1) {
                t.Fatalf ("Error: expected row is NOT there %d", row_count);
        }

	testutil.DoDefaultValidation(t);
	logger.GetLogger().Log(logger.Debug, "TestEmptyRacInterval done  -------------------------------------------------------------")
}

