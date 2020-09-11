package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"
	"testing"

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*
To run the test
export DB_USER=x
export DB_PASSWORD=x
export DB_DATASOURCE=x
export username=realU
export password=realU-pwd
export TWO_TASK='tcp(mysql.example.com:3306)/someSchema?timeout=60s&tls=preferred||tcp(failover.example.com:3306)/someSchema'
export TWO_TASK_READ='tcp(mysqlr.example.com:3306)/someSchema?timeout=6s&tls=preferred||tcp(failover.example.com:3306)/someSchema'
$GOROOT/bin/go install  .../worker/{mysql,oracle}worker
ln -s $GOPATH/bin/{mysql,oracle}worker .
$GOROOT/bin/go test -c .../tests/unittest/coordinator_basic && ./coordinator_basic.test
*/

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	appcfg["x-mysql"] = "manual" // disable test framework spawning mysql server
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "2"
	appcfg["db_heartbeat_interval"] = "3"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
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

var ip1 string
var dbName = "failovertestdb"

func TestMain(m *testing.M) {
	// startup mysql DBs
	ip1 := testutil.MakeMysql("mysql33", dbName)
	os.Setenv("TWO_TASK", "tcp("+ip1+":3306)/"+dbName+"?timeout=11s")
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestCalTransInitDB(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestCalTransInitDB begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	_, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	os.Setenv("TWO_TASK", "tcp("+ip1+":3306)/"+"test"+"?timeout=11s")
	time.Sleep(20*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err", "cal.log") < 1 {
		t.Fatalf("Error: should have INITDB as error ")
	}
	os.Setenv("TWO_TASK", "tcp("+ip1+":3306)/"+dbName+"?timeout=11s")
	os.Setenv("username","")
	time.Sleep(10*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err.*USERNAME_NOT_FOUND.*", "cal.log") < 1 {
		t.Fatalf("Error: should have INITDB as error ")
	}
	os.Setenv("username","root")
	os.Setenv("password","")
	time.Sleep(10*time.Second)
	if testutil.RegexCountFile("[A|T].*INITDB.*1.*m_err.*PASSWORD_NOT_FOUND.*", "cal.log") < 1 {
		t.Fatalf("Error: should have INITDB as error ")
	}
}
