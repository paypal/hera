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
	"sync"
	"testing"
	"time"
)

var tableName string

const letters = "abcdefghijklmnopqrstuvwxyz0123456789"

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
	appcfg["caching_cfg_reload_interval"] = "300"
	appcfg["cache_response_timeout_ms"] = "3000"
	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "10"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"] = "500"

	appcfg["child.executable"] = "mysqlworker"

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}

	return appcfg, opscfg, testutil.MySQLWorker
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func before() error {
	tableName = "jdbc_hera_cache_txn_test"

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
		err = testutil.DBDirect(
			"create table jdbc_hera_cache_txn_test ( ID BIGINT, INT_VAL BIGINT, STR_VAL VARCHAR(500))",
			os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL,
		)
		if err != nil {
			return err
		}

		err = testutil.DBDirect(
			"INSERT into hera_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, cache_by_corrid, caching_enabled_apps, remarks, hera_module) VALUES  ('1', '2733177372', 'select id, int_val from jdbc_hera_cache_txn_test where id=?', 'id=1', 30, 'N', 'jdbc_hera_cache_taf_test', '', 'Y', 'Y', 'all','', 'hera-test')",
			os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func generateRandomCorrId() string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, 11)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// GET (Cache MISS) + SET
func TestTTLCacheWithDifferentCorrIdAndCacheByCorrId(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheWithDifferentCorrIdAndCacheByCorrId begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML("/*cmd*/insert into " + tableName + "(id, int_val, str_val) VALUES(1," + fmt.Sprint(time.Now().Unix()) + ", \"val 1\")")
	time.Sleep(5 * time.Second)

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	waitGroup := sync.WaitGroup{}

	for index := 0; index < 10; index++ {
		waitGroup.Add(1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			conn, err := db.Conn(ctx)
			mux := gosqldriver.InnerConn(conn)
			mux.SetCalCorrID(fmt.Sprintf("%s%d", generateRandomCorrId(), index))
			defer conn.Close()
			if err != nil {
				t.Fatalf("error in preparing test %v", err)
			}
			stmt, _ := conn.PrepareContext(ctx, "/*cmd*/select id, int_val from "+tableName+" where id=?")
			rows, err := stmt.QueryContext(ctx, 1)

			if err != nil {
				t.Errorf("expected one row but received error %v", err)
			}
			if !rows.Next() {
				t.Errorf("Expected 1 row")
			}
			rows.Close()
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()

	//stmt.Close()
	time.Sleep(10 * time.Second)
	if testutil.RegexCountFile("coordinator doCacheRequest: starting", "hera.log") < 1 {
		t.Fatalf("Error: should have entered doCacheRequest when caching is enabled")
	}

	if testutil.RegexCountFile("Trying SET with key", "hera.log") < 10 {
		t.Fatalf("Error: should have entered setRecordToCache when caching is enabled for every unique corrId")
	}

	if testutil.RegexCountFile(".*SET.*2733177372[\\s]*0.*", "cal.log") < 10 {
		t.Fatalf("Error: should see successful SET when cacheCfgRecord is enabled for caching")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for SET returned: error: unique key violation", "hera.log") > 0 {
		t.Fatalf("Error:should not see error: unique key violation error because each request coming with different corrId")
	}

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheWithDifferentCorrIdAndCacheByCorrId done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}
