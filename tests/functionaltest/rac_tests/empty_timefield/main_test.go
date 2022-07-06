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
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "2"
        appcfg["lifespan_check_interval"] = "1"
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
 #  In hera_maint table, insert first row with empty time and second row with time 
 #  Verify Hera mux ignores the first row and processes successfully the second row in hera_main 
 #######################################################################################*/

func TestEmptyTime(t *testing.T) {
	fmt.Println ("TestEmptyTime begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestEmptyTime begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname := testutil.GetHostname()
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

        err = testutil.InsertRacEmptyTime ("F", "hera-test",  1)
        if err != nil {
                t.Fatalf("Error inserting RAC maint row  %s\n", err.Error())
        }
        time.Sleep(2500 * time.Millisecond)

	fmt.Println ("Verify mux detects RAC status change")
        if ( testutil.RegexCount ("Rac maint activating, worker 0") < 1) {
		 t.Fatalf ("Error: should have Rac maint activating");
        }

        fmt.Println ("Verify CAL log for RAC events");
	count := testutil.RegexCountFile ( "E.*RACMAINT_INFO_CHANGE.*0.*inst:0 status:F.*module:HERA-TEST", "cal.log");
        if ( count == 0 ) {
		t.Fatalf ("Error: should have Rac maint event");
        }

        time.Sleep(1500 * time.Millisecond)
        fmt.Println ("Verify worker retarted")
        if ( testutil.RegexCount ("Lifespan exceeded, terminate") < 1) {
		 t.Fatalf ("Error: should have 'Lifespan exceeded, terminate' in log");
        }

        fmt.Println ("Verify request works fine after restarting")
        time.Sleep(5000 * time.Millisecond)
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                t.Fatalf ("Error getting connection %s\n", err.Error())
        }
	fmt.Println ("Verify no more  RACMAINT event after worker restart");
	count2  := testutil.RegexCountFile("E.*RACMAINT_INFO_CHANGE.*F.*0", "cal.log")
        if ( count2 > count) {
           t.Fatalf ("Error: should NOT have extra %d RACMAINT event after worker restarted", count2 - count);
        }

	fmt.Println ("Send a fetch request, verify row is returned successfully ")
        stmt, _ := conn.PrepareContext(ctx, "/*cmd*/Select accountID, status from test_simple_table_2 where Name=?")
        rows, _ := stmt.Query("Linda Plump")
        if !rows.Next() {
                t.Fatalf("Expected 1 row")
        }

 	fmt.Printf ("Verify RAC_ID and DB_UNAME cal event")
	if ( testutil.RegexCountFile("E.*RAC_ID.*0.*0", "cal.log") != 1) {
           t.Fatalf ("Error: should have RAC_ID event");
        }

        if ( testutil.RegexCountFile ("E.*DB_UNAME.*MyDB.*0", "cal.log") != 1) {
	    t.Fatalf ("Error: should see DB_UNAME event");
	}

	rows.Close()	
        stmt.Close()
        cancel()
        conn.Close()
	testutil.DoDefaultValidation(t);
	logger.GetLogger().Log(logger.Debug, "TestEmptyTime done  -------------------------------------------------------------")
}

