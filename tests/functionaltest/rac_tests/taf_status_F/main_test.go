package main 
import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
	//"github.com/paypal/hera/client/gosqldriver"
        _"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*

The test will start Mysql docker and OCC connects to this Mysql DB docker
No setup needed

*/

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {
	twoTask := os.Getenv("TWO_TASK")
        fmt.Println ("TWO_TASK: ", twoTask)
        os.Setenv ("TWO_TASK_STANDBY0", twoTask)
        twoTask = os.Getenv("TWO_TASK_STANDBY0")
        fmt.Println ("TWO_TASK_STANDBY0: ", twoTask)

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["enable_taf"] = "true"
        appcfg["taf_timeout_ms"] = "1"
        appcfg["rac_sql_interval"] = "1"
        appcfg["rac_restart_window"] = "5"
        appcfg["lifespan_check_interval"] = "1"
        appcfg["testing_enable_dml_taf"] = "1"

	appcfg["child.executable"] = "mysqlworker"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "1"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
 	testutil.RunDML("DROP TABLE IF EXISTS hera_maint")
        err := testutil.RunDML("CREATE TABLE hera_maint (MACHINE varchar(512) not null, INST_ID int, MODULE VARCHAR(128), STATUS VARCHAR(1), STATUS_TIME INT, REMARKS varchar(64))")
        if err != nil {
            return err
        }
        return nil
}


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/* #####################################################################################
 #  Testing RAC change to status 'F' for TAF workers
 #  Verify mux detects status change and restart TAF workers
 #  Run a non-dml query for it to fail over to TAF worker and expect it to run 
 #  without any exception
 #######################################################################################*/

func TestRacStatusF_TAF(t *testing.T) {
	fmt.Println ("TestRacStatusF_TAF begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestRacStatusF_TAF begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

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

        err = testutil.SetRacNodeStatus ("F", "hera-test-taf",  1)
        if err != nil {
                t.Fatalf("Error inserting RAC maint row  %s\n", err.Error())
        }
        time.Sleep(2500 * time.Millisecond)

	fmt.Println ("Verify mux detects RAC status change")
        if ( testutil.RegexCount ("Rac maint activating, worker 0") < 1) {
		 t.Fatalf ("Error: should have Rac maint activating");
        }

        fmt.Println ("Verify CAL log for RAC events");
        if (testutil.RegexCountFile ( "E.*RACMAINT.*F.*0", "cal.log") == 0 ) {
		t.Fatalf ("Error: should have Rac maint event");
        }

	if ( testutil.RegexCount ("Rac maint row.*status = F.*OCC_TAF") < 1) {
                 t.Fatalf ("Error: should have status F");
        }


        time.Sleep(2000 * time.Millisecond)
        fmt.Printf ("Verify TAF worker retarted")
        if ( testutil.RegexCount ("Lifespan exceeded, terminate.*pool_type = 2") < 1) {
		 t.Fatalf ("Error: should have 'Lifespan exceeded, terminate' in log");
        }

        fmt.Println ("Verify request works fine after restarting")
        time.Sleep(5000 * time.Millisecond)
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
	if ( testutil.RegexCountFile("E.*RACMAINT.*F.*0", "cal.log") < 1) {
           t.Fatalf ("Error: should have RACMAINT event");
        }

 	fmt.Println ("Verify RAC_ID and DB_UNAME cal event")
	if ( testutil.RegexCountFile("E.*RAC_ID.*0.*0", "cal.log") < 1) {
           t.Fatalf ("Error: should have RAC_ID event");
        }

        count := testutil.RegexCountFile ("E.*DB_UNAME.*MyDB.*0", "cal.log")
	if (count > 0) {
	    t.Fatalf ("Error: should see DB_UNAME event");
	}

 	fmt.Println ("Verify fetch request is failed over")
	if ( testutil.RegexCount("S0-WORKER.*Preparing.*Select accountID, status from test_simple_table_2") < 0) {
           t.Fatalf ("Error: request should fail over");
        }

	rows.Close()	
        stmt.Close()
        cancel()
        conn.Close()
	logger.GetLogger().Log(logger.Debug, "TestRacStatusF_TAF done  -------------------------------------------------------------")
}

