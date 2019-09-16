package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
	"regexp"
	"strings"
	"strconv"
	"bufio"

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
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestCalClientSessionDur(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestCalClientSessionDur begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	stmt, err := conn.PrepareContext(ctx, "select 'foo' from dual")
	if err != nil {
		t.Fatalf("Error with the prepared statement")
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		t.Fatalf("Error with the QueryContext")
	}
	defer rows.Close()
	stmt.Close()

	cancel()
	conn.Close()
	clientSessionDurLogScan(t)
	logger.GetLogger().Log(logger.Debug, "TestCalClientSessionDur done  -------------------------------------------------------------")
}

func clientSessionDurLogScan(t *testing.T){
	file, err := os.Open("cal.log")
	defer file.Close()
	if err != nil {
		t.Fatalf("Error in opening cal.log")
	}
	re := regexp.MustCompile("[ |\t][0-9]+\\.[0-9]")
	cliSession_re := regexp.MustCompile("CLIENT_SESSION.*corr_id_")
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if(cliSession_re.MatchString(line)){
			_, err := strconv.ParseFloat(strings.TrimSpace(re.FindAllString(line, -1)[0]),32)
			if(err != nil){
				t.Fatalf("Num error for CLIENT_SESSION duration")
			}
		}
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("cal.log read error")
	}
}
