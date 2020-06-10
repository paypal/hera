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
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "1"
        appcfg["lifespan_check_interval"] = "1"
	appcfg["child.executable"] = "mysqlworker"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "2"
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
 #  Testing RAC change to status 'F'
 # (1)Run a dml query without commit (so the one occworker remains dedicated)
 # (2)Update the hera_maint table in DB to F state
 # (3)Check that the occworker should not restart  
 # (4)Commit the changes
 # (5)Check that the occworker gets restarted
 # (6)Run a non-dml query and expect it to run successfully
 #######################################################################################*/

func TestStatusFDedicatedWorker(t *testing.T) {
	fmt.Println ("TestStatusFDedicatedWorker begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestStatusFDedicatedWorker begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        conn, err := db.Conn(ctx)
        if err != nil {
                t.Fatalf ("Error getting connection %s\n", err.Error())
        }

	tx, _ := conn.BeginTx(ctx, nil)
	fmt.Println ("Set autocommit to false")
        stmt, _ := tx.PrepareContext(ctx, "set autocommit=0")
        defer stmt.Close()
        _, err = stmt.Exec()
        if err != nil {
                t.Fatalf ("Error: Set autocommit to false: %s", err.Error())
        }

        stmt, _ = tx.PrepareContext (ctx, "insert into test_simple_table_2 (accountID, Name, Status) VALUES('12345', 'Linda Plump', 'Good')")
        _, err = stmt.Exec()
        if err != nil {
                t.Fatalf("Error inserting row to table %s\n", err.Error())

        }

        err = testutil.SetRacNodeStatus ("F", "hera-test",  1)
        if err != nil {
                t.Fatalf("Error inserting RAC maint row  %s\n", err.Error())
        }
        time.Sleep(2500 * time.Millisecond)

	fmt.Println ("Verify mux detects RAC status change")
        if ( testutil.RegexCount ("Rac maint activating, worker 0") < 1) {
		 t.Fatalf ("Error: should have Rac maint activating");
        }
        if ( testutil.RegexCount ("Rac maint activating, worker 1") < 1) {
		 t.Fatalf ("Error: should have Rac maint activating");
        }

        fmt.Println ("Verify CAL log for RACMAINT events when F command is detected");
	count := testutil.RegexCountFile ( "E.*RACMAINT_INFO_CHANGE.*0.*inst:0 status:F.*module:HERA-TEST", "cal.log")
        if (count != 1 ) {
		t.Fatalf ("Error: should have Rac maint event");
        }

	fmt.Println ("Since the transaction is not completed, only 1 worker is restarted")
	if ( testutil.RegexCount ("Lifespan exceeded, terminate") != 1) {
                 t.Fatalf ("Error: should have 1 'Lifespan exceeded, terminate' in log");
        }


        fmt.Println ("Now commit the changes, expected 2 workers to be restarted");
	err = tx.Commit()
        time.Sleep(2000 * time.Millisecond)
        if err != nil {
                t.Fatalf("Error commit %s\n", err.Error())
        }

        time.Sleep(2000 * time.Millisecond)
        fmt.Println ("Verify worker retarted")
        if ( testutil.RegexCount ("Lifespan exceeded, terminate") != 2) {
		 t.Fatalf ("Error: should have 2 'Lifespan exceeded, terminate' in log");
        }

        fmt.Println ("Verify RAC_ID and DB_UNAME cal event")
        if ( testutil.RegexCountFile("E.*RAC_ID.*0.*0", "cal.log") != 2) {
           t.Fatalf ("Error: should have 2 RAC_ID event");
        }

        fmt.Println ("Verify request works fine after restarting")
	fmt.Println ("Send a fetch request, verify row is returned successfully ")
        stmt, _ = conn.PrepareContext(ctx, "/*cmd*/Select accountID, status from test_simple_table_2 where Name=?")
        rows, _ := stmt.Query("Linda Plump")
        if !rows.Next() {
                t.Fatalf("Expected 1 row")
        }

	rows.Close()	
        stmt.Close()
        cancel()
        conn.Close()
	logger.GetLogger().Log(logger.Debug, "TestStatusFDedicatedWorker done  -------------------------------------------------------------")
}

