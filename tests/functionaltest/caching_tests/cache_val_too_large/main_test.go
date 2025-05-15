package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/paypal/hera/client/gosqldriver"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

var tableName string

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randSeq(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	cacheHost, ok := os.LookupEnv("CACHE_HOST")
	if !ok {
		cacheHost = "localhost"
	}
	cacheCertsPath, ok := os.LookupEnv("CACHE_CERTS_PATH")
	if !ok {
		cacheCertsPath, _ = os.Getwd()
	}
	appcfg["cache_cert_file_path"] = cacheCertsPath
	appcfg["cache_endpoint"] = fmt.Sprintf("%s:5080", cacheHost)
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "2"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["db_heartbeat_interval"] = "10"
	appcfg["enable_caching"] = "true"
	appcfg["caching_cfg_reload_interval"] = "60"
	appcfg["cache_response_timeout_ms"] = "10000"
	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "2"
	opscfg["opscfg.default.server.max_lifespan_per_child"] = "5"

	appcfg["child.executable"] = "mysqlworker"

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}

	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "hera_caching"
	}
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		err := testutil.DBDirect(
			"create table hera_caching(query_id varchar(30),sqlhash varchar(40),sqltext varchar(4000),"+
				"bind_variables varchar(1000),TTL_sec BIGINT,enable_shadow_test varchar(1),tableName varchar(30),"+
				"invalidation_clause varchar(1000),caching_enabled varchar(1),cache_by_corrid varchar(1), caching_enabled_apps varchar(4000),remarks varchar(4000),hera_module varchar(100))",
			os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL,
		)
		if err != nil {
			return err
		}
	}
	err := testutil.DBDirect("drop table if exists testValTooLargeForJuno", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)
	if err != nil {
		return err
	}
	err = testutil.DBDirect(
		"create table testValTooLargeForJuno (id INT, val MEDIUMTEXT)", os.Getenv("MYSQL_IP"), "heratestdb",
		testutil.MySQL,
	)
	if err != nil {
		return err
	}
	return nil
}

func populateTable() error {
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", 0))
	if err != nil {
		return err
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// cancel must be called before conn.Close()
	defer cancel()
	// cleanup and insert one row in the table
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "INSERT INTO testValTooLargeForJuno (id,val) VALUES (:id,:val)")
	if err != nil {
		fmt.Println("Error Preparing context:", err)
	}
	defer stmt.Close()
	val := randSeq(250 * 1024)
	_, err = stmt.Exec(sql.Named("id", 1), sql.Named("val", val))
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println("Error commiting row insertion:", err)
		return err
	}

	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

// GET (Cache MISS), SET (FAILED), Response from DB
func TestTTLCacheValTooLargeForJuno(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheValTooLargeForJuno begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML("DELETE from hera_caching")
	testutil.RunDML("INSERT into hera_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, cache_by_corrid, caching_enabled_apps, remarks, hera_module) VALUES  ('1', '3520330359', 'SelectQueryWithLargeResultSet', 'abc=123', 30, 'N', 'MyTestTable', '', 'Y', 'Y', 'all','', 'hera-test')")

	time.Sleep(1 * time.Second)
	e := populateTable()

	if e != nil {
		t.Fatal("Error populating table:", e)
	}

	time.Sleep(5 * time.Second)

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	mux := gosqldriver.InnerConn(conn)
	mux.SetCalCorrID("5af5e4a2758e")

	rows, err := conn.QueryContext(ctx, "select val from testValTooLargeForJuno")

	if err != nil {
		t.Fatalf("expected 1 row, but received error :%v", err)
	}

	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	rows.Close()

	time.Sleep(2 * time.Second)

	if testutil.RegexCountFile(".*GET\t3520330359\t2.*error: no key", "cal.log") < 1 {
		t.Fatalf("Error: GET should fail with key not found")
	}

	if testutil.RegexCountFile(".*EXEC\t3520330359\t0", "cal.log") < 1 {
		t.Fatalf("Error: query should be sent to the database")
	}

	if testutil.RegexCountFile(".*SET\t3520330359\t2.*error: bad parameter", "cal.log") < 1 {
		t.Fatalf("Error: SET should fail with bad param due to max payload size")
	}

	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheValTooLargeForJuno done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}

func TestTTLCacheGetAfterValTooLargeForJuno(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheGetAfterValTooLargeForJuno begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	mux := gosqldriver.InnerConn(conn)
	mux.SetCalCorrID("5af5e4a2758e")

	rows, err := conn.QueryContext(ctx, "select val from testValTooLargeForJuno")

	if err != nil {
		t.Fatalf("expected 1 row but received an unexpected error %v", err)
	}

	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	rows.Close()

	time.Sleep(5 * time.Second)

	// GET should fail with key not found
	if testutil.RegexCountFile(".*GET\t3520330359\t2.*error: no key", "cal.log") < 2 {
		t.Fatalf("Error: GET should fail with key not found")
	}

	if testutil.RegexCountFile(".*EXEC\t3520330359\t0", "cal.log") < 2 {
		t.Fatalf("Error: query should be sent to the database")
	}

	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheGetAfterValTooLargeForJuno done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

}
