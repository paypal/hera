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
	appcfg["management_queries_timeout_ms"] = "2"

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

	testutil.RunDML("create table hera_shard_map ( scuttle_id smallint not null, shard_id tinyint not null, status char(1) , read_status char(1), write_status char(1), remarks varchar(500))")

	for i := 0; i < 1024; i++ {
		shard := 0
		if i <= 8 {
			shard = i % 3
		}
		testutil.RunDML(fmt.Sprintf("insert into hera_shard_map ( scuttle_id, shard_id, status, read_status, write_status ) values ( %d, %d, 'Y', 'Y', 'Y' )", i, shard))
	}
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_hera_test2"
	}
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		// mysql
		testutil.RunDML("create table jdbc_hera_test2 ( ID BIGINT, INT_VAL BIGINT, STR_VAL VARCHAR(500))")
	}
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func cleanup(ctx context.Context, conn *sql.Conn) error {
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*Cleanup*/delete from "+tableName+" where id != :id")
	_, err := stmt.Exec(sql.Named("id", -123))
	if err != nil {
		return err
	}
	err = tx.Commit()
	return nil
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	cleanup(ctx, conn)
	// insert one row in the table
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*TestShardingWithContextTimeout*/insert into "+tableName+" (id, int_val, str_val) VALUES(:id, :int_val, :str_val)")
	_, err = stmt.Exec(sql.Named("id", 1), sql.Named("int_val", time.Now().Unix()), sql.Named("str_val", "val 1"))
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	stmt, _ = conn.PrepareContext(ctx, "/*TestShardingWithContextTimeout*/Select id, int_val, str_val from "+tableName+" where id=:id")
	rows, _ := stmt.Query(sql.Named("id", 1))
	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	var id, int_val uint64
	var str_val sql.NullString
	err = rows.Scan(&id, &int_val, &str_val)
	if err != nil {
		t.Fatalf("Expected values %s", err.Error())
	}
	if str_val.String != "val 1" {
		t.Fatalf("Expected val 1 , got: %s", str_val.String)
	}

	rows.Close()
	stmt.Close()

	cancel()
	conn.Close()

	out, err := testutil.BashCmd("grep 'Preparing: /\\*TestShardingWithContextTimeout\\*/' hera.log | grep 'WORKER shd2' | wc -l")
	if (err != nil) || (len(out) == 0) {
		err = nil
		t.Fatalf("Request did not run on shard 2. err = %v, len(out) = %d", err, len(out))
	}
	if out[0] != '2' {
		t.Fatalf("Expected 2 excutions on shard 2, instead got %d", int(out[0]-'0'))
	}

	logger.GetLogger().Log(logger.Debug, "TestShardingWithContextTimeout done  -------------------------------------------------------------")
}
