package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

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
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_hera_test"
	}
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") { // mysql
		// with testutil.RunDML, extra log line throws off test
		testutil.DBDirect("create table jdbc_hera_test ( ID BIGINT, INT_VAL BIGINT, STR_VAL VARCHAR(500))", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)
	}
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestCoordinatorRqId(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestCoordinatorRqId begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// cleanup and insert one row in the table
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*TestCoordinatorRqId*/delete from "+tableName)
	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("Error preparing test (delete table) %s\n", err.Error())
	}
	stmt, _ = tx.PrepareContext(ctx, "/*TestCoordinatorRqId*/insert into "+tableName+" (id, int_val, str_val) VALUES(?, ?, ?)")
	_, err = stmt.Exec(1, time.Now().Unix(), "val 1")
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	conn, err = db.Conn(ctx)
	stmt, _ = conn.PrepareContext(ctx, "/*TestCoordinatorRqId*/Select id, int_val from "+tableName+" where id=?")
	rows, _ := stmt.Query(1)
	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}

	rows.Close()
	stmt.Close()

	cancel()
	conn.Close()

	out, err := testutil.BashCmd("grep 'EOR code: 0' hera.log | wc -l")
	if (err != nil) || (len(out) == 0) || (out[0] != '2') {
		err = nil
		t.Fatalf("Expected 2 'EOR 0'")
	}

	out, err = testutil.BashCmd("grep 'wrqId: 13 ): EOR code: 0 , rqId:  13' hera.log | wc -l")
	if (err != nil) || (len(out) == 0) || (out[0] != '1') {
		err = nil
		t.Fatalf("Expected 'wrqId: 13 ): EOR code: 0 , rqId:  13'")
	}

	out, err = testutil.BashCmd("grep 'wrqId: 20 ): EOR code: 0 , rqId:  20' hera.log | wc -l")
	if (err != nil) || (len(out) == 0) || (out[0] != '1') {
		err = nil
		t.Fatalf("Expected 'wrqId: 21 ): EOR code: 0 , rqId:  21'")
	}

	logger.GetLogger().Log(logger.Debug, "TestCoordinatorRqId done  -------------------------------------------------------------")
}
