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
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "2"
	appcfg["db_heartbeat_interval"] = "3"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"]="4"

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

func TestInitDBError(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestInitDBError begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	//shard := 0
	//_, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	hostname,_ := os.Hostname()
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
	twotask :=os.Getenv("TWO_TASK")
	pwd :=os.Getenv("password")
	
	fmt.Printf ("Incorrect TWO_TASK")
	os.Setenv("TWO_TASK", "tcp(dummy:3306)/"+"test"+"?timeout=11s")
	time.Sleep(30*time.Second)
	if testutil.RegexCountFile("A.*INITDB.*1.*m_err=READONLY_CONN&m_errtype=CONNECT.*", "cal.log") < 1 {
		t.Fatalf("Error: should have INITDB as error ")
	}
	os.Setenv("TWO_TASK", twotask)
	os.Truncate("cal.log",0)
	time.Sleep(10*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err.*", "cal.log") > 1 {
		t.Fatalf("Error: should NOT have INITDB as error ")
	}

	fmt.Printf ("Incorrect Password")
	os.Setenv("password","badbad")
        time.Sleep(20*time.Second)
	if testutil.RegexCount("warn.*could not get connection.*Error 1045: Access denied for user") < 1 {
                t.Fatalf("Error: should have error for bad password ")
        }
	if testutil.RegexCountFile("A.*INITDB.*1.*m_err=READONLY_CONN&m_errtype=CONNECT.*", "cal.log") < 3 {
                t.Fatalf("Error: should have INITDB as error for bad password ")
        }
	os.Setenv("password", pwd)
	os.Truncate("cal.log",0)
	os.Truncate("hera.log",0)
	time.Sleep(10*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err.*", "cal.log") > 1 {
                t.Fatalf("Error: should not have INITDB as error ")
        }

	fmt.Printf ("Incorrect Username")
	os.Setenv("username","app")
        time.Sleep(20*time.Second)
	if testutil.RegexCountFile("A.*INITDB.*1.*m_err=READONLY_CONN&m_errtype=CONNECT.*", "cal.log") < 3 {
                t.Fatalf("Error: should have INITDB as error for bad username ")
        }
	os.Setenv("username", "root")
	os.Truncate("cal.log",0)
	time.Sleep(10*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err.*", "cal.log") > 1 {
                t.Fatalf("Error: should not have INITDB as error ")
        }
        logger.GetLogger().Log(logger.Debug, "TestInitDBError done  -------------------------------------------------------------")
}
