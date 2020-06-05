package main 
import (
	"context"
	"database/sql"
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
	//appcfg["rac_sql_interval"] = "2"
        appcfg["rac_restart_window"] = "10"
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
 #  Testing RAC change to status 'R'
 #  Verify mux detects status change 'R' in hera_maint table and restarts workers
 #  Run a non-dml query and expect to run without any exceptions
 #######################################################################################*/

func TestStatusU_to_R_Tcurr_GT_Tsch(t *testing.T) {
	fmt.Println ("TestStatusU_to_R_Tcurr_GT_Tsch begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestStatusU_to_R_Tcurr_GT_Tsch begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname,_ := os.Hostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                t.Fatal("Error starting Mux:", err)
                return
        }

	db.SetMaxIdleConns(0)
	defer db.Close()

	fmt.Println ("Insert a row to table")
        testutil.RunDML("DELETE from test_simple_table_2")
        err = testutil.RunDML("insert into test_simple_table_2 (accountID, Name, Status) VALUES('12345', 'Linda Plump', 'Good')")
        if err != nil {
                t.Fatalf("Error inserting row to table %s\n", err.Error())

        }

        err = testutil.SetRacNodeStatus ("R", "hera-test",  2)
        if err != nil {
                t.Fatalf("Error inserting RAC maint row  %s\n", err.Error())
        }

        time.Sleep(1 * time.Second)
	fmt.Println ("Verify RAC status change is not detected yet")
        if ( testutil.RegexCount ("Rac maint activating") > 0) {
		 t.Fatalf ("Error: should NOT have Rac maint activating");
        }
        time.Sleep(17 * time.Second)

	fmt.Println ("Verify mux detects RAC status change")
        if ( testutil.RegexCount ("Rac maint activating, worker 0") < 1) {
		 t.Fatalf ("Error: should have Rac maint activating");
        }

        fmt.Println ("Verify CAL log for RAC events");
	count := testutil.RegexCountFile ( "E.*RACMAINT_INFO_CHANGE.*0.*inst:0 status:R.*module:HERA-TEST", "cal.log");
        if ( count == 0 ) {
		t.Fatalf ("Error: should have Rac maint event");
        }

        time.Sleep(2500 * time.Millisecond)
        fmt.Printf ("Verify worker retarted")
        if ( testutil.RegexCount ("Lifespan exceeded, terminate") < 1) {
		 t.Fatalf ("Error: should have 'Lifespan exceeded, terminate' in log");
        }

        fmt.Println ("Verify request works fine after restarting")
        time.Sleep(2000 * time.Millisecond)
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                t.Fatalf ("Error getting connection %s\n", err.Error())
        }

	fmt.Println ("Send a fetch request, verify row is returned successfully ")
        stmt, _ := conn.PrepareContext(ctx, "/*cmd*/Select accountID, status from test_simple_table_2 where Name=?")
        rows, _ := stmt.Query("Linda Plump")
        if !rows.Next() {
                t.Fatalf("Expected 1 row")
        }

 	fmt.Println ("Verify no more  RACMAINT event after worker restart");
	if ( testutil.RegexCountFile("E.*RACMAINT_INFO_CHANGE.*F.*0", "cal.log") > count) {
           t.Fatalf ("Error: should NOT have RACMAINT event after worker restarted");
        }

 	fmt.Printf ("Verify RAC_ID and DB_UNAME cal event")
	if ( testutil.RegexCountFile("E.*RAC_ID.*0.*0", "cal.log") < 1) {
           t.Fatalf ("Error: should have RAC_ID event");
        }

        if ( testutil.RegexCountFile ("E.*DB_UNAME.*MyDB.*0", "cal.log") < 1) {
	    t.Fatalf ("Error: should see DB_UNAME event");
	}

        time.Sleep(40 * time.Second)
 	fmt.Printf ("Verify RACMAINT_INFO is logged every 60 seconds")
	if ( testutil.RegexCountFile("E.*RACMAINT_INFO[[:space:]].*0", "cal.log") !=  1) {
           t.Fatalf ("Error: should have 1 RACMAINT_INFO events");
        }

	rows.Close()	
        stmt.Close()
        cancel()
        conn.Close()
	testutil.DoDefaultValidation(t);
	logger.GetLogger().Log(logger.Debug, "TestStatusU_to_R_Tcurr_GT_Tsch done  -------------------------------------------------------------")
}

