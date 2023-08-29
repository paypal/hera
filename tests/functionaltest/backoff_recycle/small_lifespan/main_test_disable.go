package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"
	"testing"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to choose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["rac_sql_interval"] = "2"
	appcfg["db_heartbeat_interval"] = "3"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "100"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"]="5"

	appcfg["child.executable"] = "mysqlworker"

	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_hera_test"
	}
	return nil
}


func TestMain(m *testing.M) {
	// startup mysql DBs
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestBackOffWithMaxLifespan(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestBackOffWithMaxLifespan begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname := testutil.GetHostname()
        fmt.Println ("Hostname: ", hostname);
        _, err := sql.Open("hera", hostname + ":31002")
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	time.Sleep(20*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err", "cal.log") > 1 {
		t.Fatalf("Error: should NOT have INITDB as error ")
	}
	//twotask :=os.Getenv("TWO_TASK")
	pwd :=os.Getenv("password")
	
	fmt.Printf ("Incorrect Password")
	os.Setenv("password","badbad")
        time.Sleep(30*time.Second)
	if testutil.RegexCount("warn.*could not get connection.*Error 1045: Access denied for user") < 1 {
                t.Fatalf("Error: should have error for bad password ")
        }
	if testutil.RegexCountFile("A.*INITDB.*1.*m_err=READONLY_CONN&m_errtype=CONNECT.*", "cal.log") < 3 {
                t.Fatalf("Error: should have INITDB as error for bad password ")
        }
        init, _ := testutil.StatelogGetField(1)
        acpt, _ := testutil.StatelogGetField(2)
	fmt.Printf ("Init Worker: %d\n",  init)
	fmt.Printf ("Accept Worker: %d\n",  acpt)
        if( init >  (acpt + init) * 30/100) {
                t.Fatalf("Error: should not have more than 30 percent workers in INIT")
        }
        os.Setenv("password", pwd)
        time.Sleep(10*time.Second)
	fmt.Println ("TestShardBasic begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
}
