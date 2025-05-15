package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

var tableName string

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXY0Z123456789 "

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
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["db_heartbeat_interval"] = "10"
	appcfg["enable_caching"] = "true"
	appcfg["caching_cfg_reload_interval"] = "5"
	appcfg["cache_response_timeout_ms"] = "3000"
	appcfg["cache_enable_compression"] = "true"
	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "5"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"] = "5"

	appcfg["child.executable"] = "mysqlworker"

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}

	return appcfg, opscfg, testutil.MySQLWorker
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func randSeq(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_hera_cache_compress_test"
	}
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		err := testutil.DBDirect(
			"create table hera_caching(query_id varchar(30),sqlhash varchar(40),sqltext varchar(4000),"+
				"bind_variables varchar(1000),TTL_sec BIGINT,enable_shadow_test varchar(1),tableName varchar(30),"+
				"invalidation_clause varchar(1000),caching_enabled varchar(1),cache_by_corrid varchar(1), caching_enabled_apps varchar(4000), remarks varchar(4000),hera_module varchar(100))",
			os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL,
		)
		if err != nil {
			return err
		}
		err = testutil.DBDirect("create table jdbc_hera_cache_compress_test ( ID BIGINT, INT_VAL BIGINT, STR_VAL MEDIUMTEXT)", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)
		if err != nil {
			return err
		}
	}
	return nil
}

// GET (Cache MISS) + SET
func TestTTLCacheHappyPath(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheHappyPathWithCompression begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML("INSERT into hera_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, cache_by_corrid, caching_enabled_apps, remarks, hera_module) VALUES  ('1', '102358690', '/*cmd*/select id, int_val, str_val from jdbc_hera_cache_compress_test where id=?', 'id=1', 30, 'N', 'MyTestTable', '', 'Y', 'N', 'all', '', 'hera-test')")
	testutil.RunDML("/*cmd*/insert into " + tableName + "(id, int_val, str_val) VALUES(1," + fmt.Sprint(time.Now().Unix()) + ",\"" + randSeq(20*1024) + "\")")
	time.Sleep(10 * time.Second)

	if testutil.RegexCountFile("Loaded 1 sqlhashes, 1 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should have loaded the cacheCfg entry...")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 1", "hera.log") < 1 {
		t.Fatalf("Error: should have loaded the cacheCfg entry...")
	}

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
	defer conn.Close()
	if err != nil {
		t.Fatalf("error in preparing test %v", err)
	}
	stmt, _ := conn.PrepareContext(ctx, "/*cmd*/select id, int_val, str_val from "+tableName+" where id=?")
	rows, err := stmt.QueryContext(ctx, 1)

	if err != nil {
		t.Errorf("extected one row but received error %v", err)
	}
	if !rows.Next() {
		t.Errorf("Expected 1 row")
	}
	rows.Close()
	stmt.Close()
	time.Sleep(15 * time.Second)
	if testutil.RegexCountFile("102358690 CachingEnabled for  GET : true", "hera.log") < 1 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("coordinator doCacheRequest: starting", "hera.log") < 1 {
		t.Fatalf("Error: should have entered doCacheRequest when caching is enabled")
	}

	if testutil.RegexCountFile("Trying GET with key", "hera.log") < 1 {
		t.Fatalf("Error: should have entered getRecordFromCache when caching is enabled")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for GET returned: error: no key", "hera.log") < 1 {
		t.Fatalf("Error: should be a cache miss for the first read")
	}

	if testutil.RegexCountFile(".*\tGET\t.*", "cal.log") < 1 {
		t.Fatalf("Error: should see GET when cacheCfgRecord is enabled for caching")
	}

	if testutil.RegexCountFile("coordinator dispatchrequest", "hera.log") < 4 {
		t.Fatalf("Error: should have dispatched the request to database")
	}

	if testutil.RegexCountFile("Trying SET with key", "hera.log") < 1 {
		t.Fatalf("Error: should have entered setRecordToCache when caching is enabled")
	}

	// INSERT + UPDATE + CacheCfg query and the select query should be sent to the database)
	if testutil.RegexCountFile("T.*CLIENT_SESSION.*", "cal.log") < 4 {
		t.Fatalf("Error: both the requests should be sent to the database")
	}

	if testutil.RegexCountFile(".*GET.*102358690", "cal.log") < 1 {
		t.Fatalf("Error: should see GET when cacheCfgRecord is enabled for caching")
	}

	if testutil.RegexCountFile(".*SET.*102358690", "cal.log") < 1 {
		t.Fatalf("Error: should see SET when cacheCfgRecord is enabled for caching")
	}

	if testutil.RegexCountFile("Set: Compression enabled", "hera.log") < 1 {
		t.Fatalf("Error: should set the compressed value when compression is enabled")
	}

	if testutil.RegexCountFile("Encode.*SET.*compressedSize.*", "cal.log") < 1 {
		t.Fatalf("Error: should see the event when compression is enabled")
	}

	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheHappyPathWithCompression done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}

func TestTTLCacheHit(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheHitWithCompression begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	time.Sleep(2 * time.Second)

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
	defer conn.Close()
	if err != nil {
		t.Fatalf("error in preparing test %v", err)
	}
	stmt, _ := conn.PrepareContext(ctx, "/*cmd*/select id, int_val, str_val from "+tableName+" where id=?")
	rows, err := stmt.QueryContext(ctx, 1)

	if err != nil {
		t.Errorf("extected one row but received error %v", err)
	}
	if !rows.Next() {
		t.Errorf("Expected 1 row")
	}
	rows.Close()
	stmt.Close()
	time.Sleep(10 * time.Second)
	if testutil.RegexCountFile("102358690 CachingEnabled for  GET : true", "hera.log") < 2 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("coordinator doCacheRequest: starting", "hera.log") < 2 {
		t.Fatalf("Error: should have entered doCacheRequest when caching is enabled")
	}

	if testutil.RegexCountFile("Trying GET with key", "hera.log") < 2 {
		t.Fatalf("Error: should have entered getRecordFromCache when caching is enabled")
	}

	if testutil.RegexCountFile("Get: Compression enabled", "hera.log") < 1 {
		t.Fatalf("Error: should return the decompressed response")
	}

	if testutil.RegexCountFile("Decode.*GET.*respSize.*", "cal.log") < 1 {
		t.Fatalf("Error: should see the event when compression is enabled")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for GET returned: error: no key", "hera.log") == 2 {
		t.Fatalf("Error: should be a cache HIT for the second read")
	}

	if testutil.RegexCountFile(".*GET\t102358690\t0.*", "cal.log") < 1 {
		t.Fatalf("Error: should be a cache HIT")
	}

	if testutil.RegexCountFile("coordinator dispatchrequest", "hera.log") < 4 {
		t.Fatalf("Error: should have dispatched the request to database")
	}
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheHitWithCompression done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

}
