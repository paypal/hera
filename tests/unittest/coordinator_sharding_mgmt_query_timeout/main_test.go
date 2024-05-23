package main

import (
	"context"
	"database/sql"

	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31003"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["enable_sharding"] = "true"
	appcfg["num_shards"] = "3"
	appcfg["max_scuttle"] = "9"
	appcfg["shard_key_name"] = "id"
	pfx := os.Getenv("MGMT_TABLE_PREFIX")
	if pfx != "" {
		appcfg["management_table_prefix"] = pfx
	}
	appcfg["sharding_cfg_reload_interval"] = "2"
	appcfg["rac_sql_interval"] = "0"
	appcfg["management_queries_timeout_us"] = "400"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupShardMap() {
	twoTask := os.Getenv("TWO_TASK")
	if !strings.HasPrefix(twoTask, "tcp") {
		// not mysql
		return
	}
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		testutil.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		testutil.Fatalf("Error getting connection %s\n", err.Error())
	}
	defer conn.Close()

	testutil.DBDirect("create table hera_shard_map ( scuttle_id smallint not null, shard_id tinyint not null, status char(1) , read_status char(1), write_status char(1), remarks varchar(500))", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)

	for i := 0; i < 9; i++ {
		shard := 0
		if i >= 3 {
			shard = i % 3
		}
		testutil.DBDirect(fmt.Sprintf("insert into hera_shard_map ( scuttle_id, shard_id, status, read_status, write_status ) values ( %d, %d, 'Y', 'Y', 'Y' )", i, shard), os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)
	}
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_hera_test2"
	}
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		// mysql
		testutil.DBDirect("create table jdbc_hera_test2 ( ID BIGINT, INT_VAL BIGINT, STR_VAL VARCHAR(500))", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)
	}
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestShardingWithContextTimeout(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestShardingWithContextTimeout setup")
	setupShardMap()
	logger.GetLogger().Log(logger.Debug, "TestShardingWithContextTimeout begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	time.Sleep(25 * time.Second)
	hostname, _ := os.Hostname()
	db, err := sql.Open("hera", hostname+":31003")
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	out := testutil.RegexCountFile("loading shard map: context deadline exceeded", "cal.log")
	if out < 2 {
		err = nil
		t.Fatalf("sharding management query should fail with context timeout")
	}

	logger.GetLogger().Log(logger.Debug, "TestShardingWithContextTimeout done  -------------------------------------------------------------")
}
