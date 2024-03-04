package main

import (
	"context"
	"database/sql"

	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/paypal/hera/client/gosqldriver"
	_ "github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

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
	appcfg["scuttle_col_name"] = "scuttle_id"
	appcfg["shard_key_name"] = "id"
	appcfg["error_code_prefix"] = "HERA"
	pfx := os.Getenv("MGMT_TABLE_PREFIX")
	if pfx != "" {
		appcfg["management_table_prefix"] = pfx
	}
	appcfg["sharding_cfg_reload_interval"] = "3600"
	appcfg["rac_sql_interval"] = "0"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func setupShardMap(t *testing.T) {
	twoTask := os.Getenv("TWO_TASK")
	if !strings.HasPrefix(twoTask, "tcp") {
		// not mysql
		return
	}
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer releaseDB(db)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	defer func(conn *sql.Conn) {
		err := conn.Close()
		if err != nil {
			logger.GetLogger().Log(logger.Info, "Error while closing Conn: ", err)
		}
	}(conn)

	testutil.RunDML("create table hera_shard_map ( scuttle_id smallint not null, shard_id tinyint not null, status char(1) , read_status char(1), write_status char(1), remarks varchar(500))")

	for i := 0; i < 1024; i++ {
		shard := 0
		if i <= 9 {
			shard = i % 3
		}
		testutil.RunDML(fmt.Sprintf("insert into hera_shard_map ( scuttle_id, shard_id, status, read_status, write_status ) values ( %d, %d, 'Y', 'Y', 'Y' )", i, shard))
	}
}

func before() error {
	tableName = "jdbc_hera_scuttle_id_test"
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		// mysql
		err := testutil.RunDML("create table " + tableName + " ( SCUTTLE_ID smallint not null, ID BIGINT, INT_VAL BIGINT, STR_VAL VARCHAR(500))")
		if err != nil {
			logger.GetLogger().Log(logger.Info, "Error while creating table")
		}
	}
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestShardingWithScuttleIDBasic(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestShardingBasicWithScuttleID setup")
	setupShardMap(t)
	logger.GetLogger().Log(logger.Debug, "TestShardingBasicWithScuttleID begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	hostname, _ := os.Hostname()
	appCfg, _, _ := cfg()
	db, err := sql.Open("hera", hostname+":31003")
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer releaseDB(db)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	// insert one row in the table
	tx, _ := conn.BeginTx(ctx, nil)
	shardKey := 1
	scuttleID, err := testutil.ComputeScuttleId(shardKey, appCfg["max_scuttle"])
	if err != nil {
		t.Fatalf("Error generating scuttle ID %s\n", err.Error())
	}
	stmt, _ := tx.PrepareContext(ctx, "/*TestShardingBasicWithScuttleID*/insert into "+tableName+" (scuttle_id, id, int_val, str_val) VALUES(:scuttle_id, :id, :int_val, :str_val)")
	_, err = stmt.Exec(sql.Named("scuttle_id", scuttleID), sql.Named("id", shardKey), sql.Named("int_val", time.Now().Unix()), sql.Named("str_val", "val 1"))
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	stmt, _ = conn.PrepareContext(ctx, "/*TestShardingBasicWithScuttleID*/Select id, int_val, str_val from "+tableName+" where id=:id and scuttle_id=:scuttle_id")
	rows, err := stmt.Query(sql.Named("id", 1), sql.Named("scuttle_id", scuttleID))

	if err != nil {
		t.Fatalf("Error Selecting results %s\n", err.Error())
	}

	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	var id, intVal uint64
	var strVal sql.NullString
	err = rows.Scan(&id, &intVal, &strVal)
	if err != nil {
		t.Fatalf("Expected values %s", err.Error())
	}
	if strVal.String != "val 1" {
		t.Fatalf("Expected val 1 , got: %s", strVal.String)
	}
	err = rows.Close()
	if err != nil {
		logger.GetLogger().Log(logger.Info, "Error while closing rows: ", err)
	}

	//Change Scuttle ID value to in correct scuttle ID
	shardKey = 2
	scuttleID = 3
	// insert one row in the table
	tx, _ = conn.BeginTx(ctx, nil)
	stmt, _ = tx.PrepareContext(ctx, "/*TestShardingBasicWithScuttleIDIncorrectVal*/insert into "+tableName+" (scuttle_id, id, int_val, str_val) VALUES(:scuttle_id, :id, :int_val, :str_val)")
	_, err = stmt.Exec(sql.Named("scuttle_id", scuttleID), sql.Named("id", shardKey), sql.Named("int_val", time.Now().Unix()), sql.Named("str_val", "val 2"))
	if err == nil {
		t.Fatal("Expected to fail because, mismatch between computed bucket and scuttleId.")
	}
	if !strings.Contains(err.Error(), "HERA-208: scuttle_id mismatch") {
		t.Fatal("Expected error HERA-208: scuttle_id mismatch")
	}
	err = tx.Commit()

	//Add record2
	// insert one row in the table
	conn, err = db.Conn(ctx)
	tx, _ = conn.BeginTx(ctx, nil)
	shardKey = 2
	scuttleID, err = testutil.ComputeScuttleId(shardKey, appCfg["max_scuttle"])
	if err != nil {
		t.Fatalf("Error generating scuttle ID %s\n", err.Error())
	}
	stmt, _ = tx.PrepareContext(ctx, "/*TestShardingBasicWithScuttleID*/insert into "+tableName+" (scuttle_id, id, int_val, str_val) VALUES(:scuttle_id, :id, :int_val, :str_val)")
	_, err = stmt.Exec(sql.Named("scuttle_id", scuttleID), sql.Named("id", shardKey), sql.Named("int_val", time.Now().Unix()), sql.Named("str_val", "val 2"))

	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	err = stmt.Close()
	if err != nil {
		logger.GetLogger().Log(logger.Info, "Error while closing statement: ", err)
	}
	err = conn.Close()
	if err != nil {
		logger.GetLogger().Log(logger.Info, "Error while closing connection: ", err)
	}
	cancel()
	logger.GetLogger().Log(logger.Debug, "TestShardingBasicWithScuttleID done  -------------------------------------------------------------")
}

func TestShardingWithScuttleIDAndSetShard(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestShardingWithScuttleIDAndSetShard begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	hostname, _ := os.Hostname()
	db, err := sql.Open("hera", hostname+":31003")
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer releaseDB(db)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	mux := gosqldriver.InnerConn(conn)
	err = mux.SetShardID(1)
	if err != nil {
		logger.GetLogger().Log(logger.Info, "Failed to set shardId: ", err)
	}
	stmt, _ := conn.PrepareContext(ctx, "/*TestShardingWithScuttleIDAndSetShard*/Select scuttle_id, id, int_val, str_val from "+tableName+" where id=1 and scuttle_id=:scuttle_id")
	rows, _ := stmt.Query(sql.Named("scuttle_id", 2))
	if rows != nil {
		err = rows.Close()
		if err != nil {
			logger.GetLogger().Log(logger.Info, "Error while closing rows: ", err)
		}
	}
	if stmt != nil {
		err = stmt.Close()
		if err != nil {
			logger.GetLogger().Log(logger.Info, "Error while closing statement: ", err)
		}
	}
	out, err := testutil.BashCmd("grep 'Preparing: /\\*TestShardingWithScuttleIDAndSetShard\\*/' hera.log | grep 'WORKER shd1' | wc -l")
	if (err != nil) || (len(out) == 0) {
		err = nil
		t.Fatalf("Request did not run on shard 1. err = %v, len(out) = %d", err, len(out))
	}

	err = mux.SetShardID(2)
	if err != nil {
		logger.GetLogger().Log(logger.Info, "Failed to set shardId: ", err)
	}
	stmt, _ = conn.PrepareContext(ctx, "/*TestShardingWithScuttleIDAndSetShard*/Select scuttle_id, id, int_val, str_val from "+tableName+" where id=2 and scuttle_id=:scuttle_id")
	rows, _ = stmt.Query(sql.Named("scuttle_id", 1))
	if rows != nil {
		err = rows.Close()
		if err != nil {
			logger.GetLogger().Log(logger.Info, "Error while closing rows: ", err)
		}
	}
	err = stmt.Close()
	if err != nil {
		logger.GetLogger().Log(logger.Info, "Error while closing Statement: ", err)
	}
	out, err = testutil.BashCmd("grep 'Preparing: /\\*TestShardingWithScuttleIDAndSetShard\\*/' hera.log | grep 'WORKER shd2' | wc -l")
	if (err != nil) || (len(out) == 0) {
		err = nil
		t.Fatalf("Request did not run on shard 2. err = %v, len(out) = %d", err, len(out))
	}
	err = conn.Close()
	if err != nil {
		logger.GetLogger().Log(logger.Info, "Error while closing connection: ", err)
	}
	cancel()
	logger.GetLogger().Log(logger.Debug, "TestShardingWithScuttleIDAndSetShard done  -------------------------------------------------------------")
}

func TestShardingWithScuttleIDAndInvalidBindValue(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestShardingWithScuttleIDAndInvalidBindValue begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	hostname, _ := os.Hostname()
	db, err := sql.Open("hera", hostname+":31003")
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer releaseDB(db)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	//Test 1 provide scuttle_id as empty or nil
	tx, _ := conn.BeginTx(ctx, nil)
	shardKey := 2
	if err != nil {
		t.Fatalf("Error generating scuttle ID %s\n", err.Error())
	}
	stmt, _ := tx.PrepareContext(ctx, "/*TestShardingWithScuttleIDAndWithoutBindValue*/insert into "+tableName+" (scuttle_id, id, int_val, str_val) VALUES(:scuttle_id, :id, :int_val, :str_val)")
	_, err = stmt.Exec(sql.Named("scuttle_id", ""), sql.Named("id", shardKey), sql.Named("int_val", time.Now().Unix()), sql.Named("str_val", "val 2"))
	if err == nil {
		t.Fatal("Expected to fail because, mismatch between computed bucket and scuttleId.")
	}
	if !strings.Contains(err.Error(), "HERA-208") {
		t.Fatal("Expected error HERA-208: scuttle_id mismatch")
	}
	tx.Commit()
	err = conn.Close()
	if err != nil {
		logger.GetLogger().Log(logger.Info, "Error while closing connection: ", err)
	}

	conn, err = db.Conn(ctx)
	//Test 2 select with no bind value scuttle_id
	stmt, _ = conn.PrepareContext(ctx, "/*TestShardingWithScuttleIDAndWithoutBindValue*/Select id, int_val, str_val from "+tableName+" where id=:id and scuttle_id=:scuttle_id")
	rows, err := stmt.Query(sql.Named("scuttle_id", ""), sql.Named("id", shardKey))

	if err == nil {
		t.Fatal("Expected to fail because, mismatch between computed bucket and scuttleId.")
	}
	if !strings.Contains(err.Error(), "HERA-208") {
		t.Fatal("Expected error HERA-208: scuttle_id mismatch")
	}
	if rows != nil {
		err := rows.Close()
		if err != nil {
			logger.GetLogger().Log(logger.Info, "Error while closing rows object: ", err)
		}
	}
	if stmt != nil {
		err = stmt.Close()
		if err != nil {
			logger.GetLogger().Log(logger.Info, "Error while closing statement: ", err)
		}
	}
	err = conn.Close()
	if err != nil {
		logger.GetLogger().Log(logger.Info, "Error while closing connection: ", err)
	}
	cancel()
	logger.GetLogger().Log(logger.Debug, "TestShardingWithScuttleIDAndWithoutBindValue done  -------------------------------------------------------------")
}

func releaseDB(db *sql.DB) {
	err := db.Close()
	if err != nil {
		logger.GetLogger().Log(logger.Info, "Error while closing DB: ", err)
	}
}
