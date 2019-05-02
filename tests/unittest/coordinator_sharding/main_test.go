package main

import (
	"context"
	"database/sql"
//	"fmt"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
	_ "github.com/paypal/hera/gomuxdriver/muxtcp"
	"github.com/paypal/hera/gomuxdriver"
	"os"
	"testing"
	"time"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31003"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "occ.log"
	appcfg["enable_sharding"] = "true"
	appcfg["num_shards"] = "3"
	appcfg["max_scuttle"] = "9"
	appcfg["shard_key_name"] = "id"
	pfx := os.Getenv("MGMT_TABLE_PREFIX")
	if pfx != "" {
		appcfg["management_table_prefix"] = pfx
	}
	appcfg["sharding_cfg_reload_interval"] = "3600"
	appcfg["rac_sql_interval"] = "0"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.OracleWorker
}

func before() error {
	tableName = os.Getenv("TABLE_NAME") 
	if tableName == "" {
		tableName = "jdbc_mux_test"
	}
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}


func cleanup(ctx context.Context, conn *sql.Conn) error {
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*Cleanup*/delete " + tableName + " where id != :id")
	_, err := stmt.Exec(sql.Named("id", -123))
	if err != nil {
		return err
	}
	err = tx.Commit()
	return nil
}

func TestShardingBasic(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestShardingBasic begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname,_ := os.Hostname()
	db, err := sql.Open("occ", hostname + ":31003")
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
	stmt, _ := tx.PrepareContext(ctx, "/*TestShardingBasic*/insert into "+tableName+" (id, int_val, str_val) VALUES(:id, :int_val, :str_val)")
	_, err = stmt.Exec(sql.Named("id", 1), sql.Named("int_val", time.Now().Unix()), sql.Named("str_val", "val 1"))
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	stmt, _ = conn.PrepareContext(ctx, "/*TestShardingBasic*/Select id, int_val, str_val from "+tableName+" where id=:id")
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
	
	
	out, err := testutil.BashCmd("grep 'Preparing: /\\*TestShardingBasic\\*/' occ.log | grep 'WORKER shd2' | wc -l")
	if (err != nil) || (len(out) == 0){
		err = nil
		t.Fatalf("Request did not run on shard 2. err = %v, len(out) = %d", err, len(out))
	}
	if out[0] != '2' {
		t.Fatalf("Expected 2 excutions on shard 2, instead got %d", int(out[0] - '0'))
	}

	logger.GetLogger().Log(logger.Debug, "TestShardingBasic done  -------------------------------------------------------------")
}

func TestShardingSetShard(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestShardingSetShard begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	hostname,_ := os.Hostname()
	db, err := sql.Open("occ", hostname + ":31003")
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
	
	mux := gomuxdriver.InnerConn(conn)
	mux.SetShardID(1)
	stmt, _ := conn.PrepareContext(ctx, "/*TestShardingSetShard*/Select id, int_val, str_val from " + tableName + " where id=1")
	rows, _ := stmt.Query()
	rows.Close()
	stmt.Close()
	out, err := testutil.BashCmd("grep 'Preparing: /\\*TestShardingSetShard\\*/' occ.log | grep 'WORKER shd1' | wc -l")
	if (err != nil) || (len(out) == 0){
		err = nil
		t.Fatalf("Request did not run on shard 1. err = %v, len(out) = %d", err, len(out))
	}
	if out[0] != '1' {
		t.Fatalf("Expected 1 excution on shard 1, instead got %d", int(out[0] - '0'))
	}

	mux.SetShardID(2)
	stmt, _ = conn.PrepareContext(ctx, "/*TestShardingSetShard*/Select id, int_val, str_val from " + tableName + " where id=1")
	rows, _ = stmt.Query()
	rows.Close()
	stmt.Close()
	out, err = testutil.BashCmd("grep 'Preparing: /\\*TestShardingSetShard\\*/' occ.log | grep 'WORKER shd2' | wc -l")
	if (err != nil) || (len(out) == 0){
		err = nil
		t.Fatalf("Request did not run on shard 2. err = %v, len(out) = %d", err, len(out))
	}
	if out[0] != '1' {
		t.Fatalf("Expected 1 excution on shard 2, instead got %d", int(out[0] - '0'))
	}

	mux.ResetShardID()
	cnt, err := mux.GetNumShards()
	if (err != nil){
		t.Fatalf("GetNumShards failed: %v", err)
	}
	if (cnt != 3){
		t.Fatalf("Expected 3 shards, instead got %v", cnt)
	}

	stmt, _ = conn.PrepareContext(ctx, "/*TestShardingSetShard 2*/insert into " + tableName + " (id) VALUES(123)")
	res, err := stmt.ExecContext(ctx)
	if err == nil {
		t.Fatal("Expected to fail because no shard key")
	}
	if err.Error() != "Internal occ error: OCC-373: no shard key or more than one or bad logical db" {
		t.Fatal("Expected error OCC-373")
	}

	mux.SetShardID(1)
	stmt, _ = conn.PrepareContext(ctx, "/*TestShardingSetShard 3*/insert into " + tableName + " (id) VALUES(1)")
	res, err = stmt.ExecContext(ctx)
	if err != nil {
		t.Fatal("Expected to succeed")
	}
	cnt2, err := res.RowsAffected()
	if err != nil {
		t.Fatal("Expected to succeed")
	}
	if cnt2 != 1 {
		t.Fatal("Expected 1 row inserted")
	}
	err = mux.SetShardID(2)
	if err == nil {
		t.Fatalf("Change shard in TXN should fail")
	}
	if err.Error() != "OCC-203: changing shard_id while in txn" {
		t.Fatalf("Expected error OCC-203")
	}
	
	err = mux.SetShardID(3)
	if err == nil {
		t.Fatalf("Change shard in TXN should fail")
	}
	if err.Error() != "Failed to read response" {
		t.Fatalf("Expected error 'Failed to read response', instead got %s", err.Error())
	}

	conn.Close()

	cancel()
	
	logger.GetLogger().Log(logger.Debug, "TestShardingSetShard done  -------------------------------------------------------------")
}

func TestShardingSetShard2(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestShardingSetShard2 begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	hostname,_ := os.Hostname()
	db, err := sql.Open("occ", hostname + ":31003")
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
	
	mux := gomuxdriver.InnerConn(conn)
	mux.SetShardID(1)
	
	stmt, _ := conn.PrepareContext(ctx, "/*TestShardingSetShard 3*/insert into " + tableName + " (id) VALUES(1)")
	res, err := stmt.ExecContext(ctx)
	if err != nil {
		t.Fatal("Expected to succeed")
	}
	cnt2, err := res.RowsAffected()
	if err != nil {
		t.Fatal("Expected to succeed")
	}
	if cnt2 != 1 {
		t.Fatal("Expected 1 row inserted")
	}
	err = mux.ResetShardID()
	if err == nil {
		t.Fatalf("Change shard in TXN should fail")
	}
	if err.Error() != "OCC-203: changing shard_id while in txn" {
		t.Fatalf("Expected error OCC-203")
	}
	
	err = mux.SetShardID(3)
	if err == nil {
		t.Fatalf("Change shard in TXN should fail")
	}
	if err.Error() != "Failed to read response" {
		t.Fatalf("Expected error 'Failed to read response', instead got %s", err.Error())
	}
	
	cancel()
	conn.Close()
	
	logger.GetLogger().Log(logger.Debug, "TestShardingSetShard2 done  -------------------------------------------------------------")
}

func TestShardingSetShardTx(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestShardingSetShardTx begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	hostname,_ := os.Hostname()
	db, err := sql.Open("occ", hostname + ":31003")
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
	
	mux := gomuxdriver.InnerConn(conn)
	mux.SetShardID(1)
	
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*TestShardingSetShardTx*/insert into " + tableName + " (id) VALUES(:id_val)")
	// bind var is not shard key
	res, err := stmt.ExecContext(ctx, sql.Named("id_val", 1))
	if err != nil {
		t.Fatal("Expected to succeed")
	}
	cnt2, err := res.RowsAffected()
	if err != nil {
		t.Fatal("Expected to succeed")
	}
	if cnt2 != 1 {
		t.Fatal("Expected 1 row inserted")
	}
	stmt.Close()
	tx.Commit()
	
	// check the logs that in fact shard 1 was used
	out, err := testutil.BashCmd("grep 'Preparing: /\\*TestShardingSetShardTx\\*/' occ.log | grep 'WORKER shd1' | wc -l")
	if (err != nil) || (len(out) == 0){
		err = nil
		t.Fatalf("Request did not run on shard 1. err = %v, len(out) = %d", err, len(out))
	}
	if out[0] != '1' {
		t.Fatalf("Expected 1 excution on shard 1, instead got %d", int(out[0] - '0'))
	}

	err = mux.SetShardID(2)
	if err != nil {
		t.Fatalf("Expected to succeed, instead %s", err.Error())
	}

	tx, _ = conn.BeginTx(ctx, nil)
	// bind var is shard key
	stmt, _ = tx.PrepareContext(ctx, "/*TestShardingSetShardTx 1*/insert into " + tableName + " (id) VALUES(:id)")
	res, err = stmt.ExecContext(ctx, sql.Named("id", 2))
	if err == nil {
		t.Fatalf("Expected to fail with Internal occ error: OCC-374: autodiscover while set shard id")
	}
	if err.Error() != "Internal occ error: OCC-374: autodiscover while set shard id" {
		t.Fatalf("Expected to fail with Internal occ error: OCC-374: autodiscover while set shard id, instead failed with '%s'", err.Error())
	}
	stmt.Close()
	tx.Rollback()
	
	// check the logs that in fact shard 1 was used
	out, err = testutil.BashCmd("grep 'autodiscover_while_set_shard_id' cal.log | grep 'sql=3408809937' | wc -l")
	if (err != nil) || (len(out) == 0){
		err = nil
		t.Fatalf("Expected autodiscover_while_set_shard_id in cal.log. err = %v, len(out) = %d", err, len(out))
	}
	if out[0] != '1' {
		t.Fatalf("Expected 1 autodiscover_while_set_shard_id in cal.log, instead got %d", int(out[0] - '0'))
	}

	cancel()
	conn.Close()
	
	logger.GetLogger().Log(logger.Debug, "TestShardingSetShardTx done  -------------------------------------------------------------")
}

func TestShardingSetShardKey(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestShardingSetShardKey begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	hostname,_ := os.Hostname()
	db, err := sql.Open("occ", hostname + ":31003")
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
	
	mux := gomuxdriver.InnerConn(conn)
	mux.SetShardID(1)
	
	// check the logs that no 'Unsupported both OCC_SET_SHARD_ID and ShardKey true'
	out, err := testutil.BashCmd("grep 'Unsupported both OCC_SET_SHARD_ID and ShardKey true' occ.log | wc -l")
	if (err != nil) || (len(out) == 0){
		err = nil
		t.Fatalf("Expected no Unsupported both OCC_SET_SHARD_ID and ShardKey true, %v %v", err, len(out))
	}
	if out[0] != '0' {
		t.Fatalf("Expected no occurrence of 'Unsupported both OCC_SET_SHARD_ID and ShardKey', instead got %d", int(out[0] - '0'))
	}
	
	mux.SetShardKeyPayload("id=1")
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*TestShardingSetShardKey*/insert into " + tableName + " (id) VALUES(:id_val)")
	// bind var is not shard key
	_, err = stmt.ExecContext(ctx, sql.Named("id_val", 1))
	if err == nil {
		t.Fatal("Expected to fail")
	}
	if err.Error() != "Failed to read response" {
		t.Fatalf("Expected 'Failed to read response', instead got '%s'", err.Error())
	}
	stmt.Close()
	tx.Rollback()

	// check the logs for one 'Unsupported both OCC_SET_SHARD_ID and ShardKey true'
	out, err = testutil.BashCmd("grep 'Unsupported both OCC_SET_SHARD_ID and ShardKey true' occ.log | wc -l")
	if (err != nil) || (len(out) == 0){
		err = nil
		t.Fatalf("Expected 1 Unsupported both OCC_SET_SHARD_ID and ShardKey true, %v %v", err, len(out))
	}
	if out[0] != '1' {
		t.Fatalf("Expected 1 occurence of 'Unsupported both OCC_SET_SHARD_ID and ShardKey', instead got %d", int(out[0] - '0'))
	}
	

	conn, err = db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	cleanup(ctx, conn)
	
	mux = gomuxdriver.InnerConn(conn)
	// key 1223: hash = 3648679963 , bucket = 7 , shardID = 1
	mux.SetShardKeyPayload("id=1223")
	
	tx, _ = conn.BeginTx(ctx, nil)
	stmt, _ = tx.PrepareContext(ctx, "/*TestShardingSetShardKey 2*/insert into " + tableName + " (id) VALUES(:id)")
	// bind var is shard key
	// key 1: hash = 2599271225 , bucket = 8 , shardID = 2
	_, err = stmt.ExecContext(ctx, sql.Named("id", 1))
	if err != nil {
		t.Fatalf("Unexpected err: %s", err.Error())
	}
	stmt.Close()

	// check the logs running on shard 1
	out, err = testutil.BashCmd("grep 'Preparing: /\\*TestShardingSetShardKey 2\\*/' occ.log | grep 'WORKER shd1' | wc -l")
	if (err != nil) || (len(out) == 0){
		err = nil
		t.Fatalf("Expected no error, instead %v %v", err, len(out))
	}
	if out[0] != '1' {
		t.Fatalf("Expected 1 occurence of 'Preparing: /\\*TestShardingSetShardKey 2\\*/', instead got %d", int(out[0] - '0'))
	}
	
	stmt, _ = tx.PrepareContext(ctx, "/*TestShardingSetShardKey 3*/insert into " + tableName + " (id) VALUES(:id)")
	// bind var is shard key
	// key 6: hash = 2979871947 , bucket = 0 , shardID = 0
	_, err = stmt.ExecContext(ctx, sql.Named("id", 3))
	if err != nil {
		t.Fatalf("Unexpected err: %s", err.Error())
	}
	stmt.Close()

	// check the logs running on shard 1
	out, err = testutil.BashCmd("grep 'Preparing: /\\*TestShardingSetShardKey 3\\*/' occ.log | grep 'WORKER shd1' | wc -l")
	if (err != nil) || (len(out) == 0){
		err = nil
		t.Fatalf("Expected no error, instead %v %v", err, len(out))
	}
	if out[0] != '1' {
		t.Fatalf("Expected 1 occurence of 'Preparing: /\\*TestShardingSetShardKey 3\\*/', instead got %d", int(out[0] - '0'))
	}
	
	tx.Rollback()
	
	// reset shard key payload to use a different api
	mux.ResetShardKeyPayload()
	tx, _ = conn.BeginTx(ctx, nil)
	stmt, _ = tx.PrepareContext(ctx, "/*TestShardingSetShardKey 4*/insert into " + tableName + " (id) VALUES(:id)")
	// bind var is shard key
	// key 1: hash = 2599271225 , bucket = 8 , shardID = 2
	_, err = stmt.ExecContext(ctx, sql.Named("id", 1))
	if err != nil {
		t.Fatalf("Unexpected err: %s", err.Error())
	}
	stmt.Close()

	// check the logs running on shard 2
	out, err = testutil.BashCmd("grep 'Preparing: /\\*TestShardingSetShardKey 4\\*/' occ.log | grep 'WORKER shd2' | wc -l")
	if (err != nil) || (len(out) == 0){
		err = nil
		t.Fatalf("Expected no error, instead %v %v", err, len(out))
	}
	if out[0] != '1' {
		t.Fatalf("Expected 1 occurence of 'Preparing: /\\*TestShardingSetShardKey 4\\*/', instead got %d", int(out[0] - '0'))
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Unexpected err: %s", err.Error())
	}
	
	conn.Close()
	cancel()
	
	logger.GetLogger().Log(logger.Debug, "TestShardingSetShardKey done  -------------------------------------------------------------")
}
