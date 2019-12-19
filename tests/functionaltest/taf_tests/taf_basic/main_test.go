package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
 	_"github.com/paypal/hera/client/gosqldriver/tcp"
	"os"
	"testing"
	"time"
)
type cfgFunc func() (map[string]string, map[string]string, testutil.WorkerType)
var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
        appcfg["enable_taf"] = "true"
        appcfg["taf_timeout_ms"] = "1"
        appcfg["testing_enable_dml_taf"] = "1"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "6"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}


func setupDb() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_mux_test"
	}

	testutil.RunDML("DROP TABLE " + tableName)
	return testutil.RunDML("CREATE TABLE " + tableName + " ( id bigint, int_val bigint, str_val varchar(128) )")
}


func TestMain (m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}


func TestTafSimple (t *testing.T) {
        fmt.Println ("TestTafSimple begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

	hostname,_ := os.Hostname()
        fmt.Println ("Hostname: ", hostname);
        db, err := sql.Open("hera", hostname + ":31002")
        if err != nil {
                t.Fatal("Error starting Mux:", err)
                return
        }
        db.SetMaxIdleConns(0)
        defer db.Close()

        fmt.Println ("Verify in same txn, read worker can switch to write worker")
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        // cleanup and insert one row in the table
        conn, err := db.Conn(ctx)
        if err != nil {
                t.Fatalf("Error getting connection %s\n", err.Error())
        }
       	stmt, _ := conn.PrepareContext(ctx, "/*cmd*/Select item_id, comments from dalcert_details order by item_id")
       	rows, _ := stmt.Query()
	var item_id int
	var comment string
	rows.Next()
       	rows.Scan(&item_id, &comment)
        if (item_id < 1) {
	     t.Fatalf ("Error: expected 1 row fetched");
        }
	rows.Close()
       	stmt.Close()

	cancel()
        conn.Close()

	fmt.Println ("Verify TAF events and worker recycle in CAL")
	count := testutil.RegexCountFile ("TAF.*TMO.*pct=109&sqlhash=1472831036&timeout_ms=1&used_ms='", "cal.log");
        if (count < 1) {
	     t.Fatalf ("Error: expected TAF TMO event");
        }
	count = testutil.RegexCountFile ("E.*STRANDED.*RECOVER.*", "cal.log");
        if (count < 2) {
	     t.Fatalf ("Error: expected worker recover event");
        }

	count = testutil.RegexCountFile ("E.*OCCWORKER.*recoverworker", "cal.log");
        if (count < 1) {
	     t.Fatalf ("Error: expected worker recover event");
        }

	count = testutil.RegexCountFile ("T.*API.*CLIENT_SESSION_TAF", "cal.log");
        if (count < 1) {
	     t.Fatalf ("Error: expected TAF worker execute the query");
        }

        logger.GetLogger().Log(logger.Debug, "TestTaf done  -------------------------------------------------------------")

}
