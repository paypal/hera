package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
	"github.com/paypal/hera/tests/functionaltest/testutil"
        "github.com/paypal/hera/utility/logger"
        "github.com/paypal/hera/client/gosqldriver"
        _ "github.com/paypal/hera/client/gosqldriver/tcp"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {
        appcfg := make(map[string]string)
        appcfg["bind_port"] = "31002"
        appcfg["log_level"] = "5"
        appcfg["log_file"] = "hera.log"
        appcfg["rac_sql_interval"] = "0"

	//For sharding
        appcfg["enable_sharding"] = "true"
        appcfg["num_shards"] = "5"
        appcfg["shard_key_name"] = "ACCOUNTID"
        appcfg["enable_whitelist_test"] = "true"
        appcfg["whitelist_children"] = "2"
	appcfg["max_scuttle"] = "128"
        appcfg["sharding_algo"] = "mod" 

        opscfg := make(map[string]string)
        opscfg["opscfg.default.server.max_connections"] = "3"
        opscfg["opscfg.default.server.log_level"] = "5"

	if os.Getenv("WORKER") == "postgres" {
                return appcfg, opscfg, testutil.PostgresWorker
        } 

	return appcfg, opscfg, testutil.MySQLWorker
}


func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
	testutil.RunDML("DROP TABLE IF EXISTS hera_shard_map")
	if os.Getenv("WORKER") == "postgres" {
                testutil.RunDML("CREATE TABLE test_simple_table_1 (ID BIGINT PRIMARY KEY, NAME VARCHAR(128), STATUS BIGINT, PYPL_TIME_TOUCHED BIGINT)")
                testutil.RunDML("CREATE TABLE hera_shard_map (SCUTTLE_ID BIGINT, SHARD_ID BIGINT, STATUS CHAR(1), READ_STATUS CHAR(1), WRITE_STATUS CHAR(1), REMARKS VARCHAR(500))");
        } else { 
                testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
                testutil.RunDML("CREATE TABLE hera_shard_map (SCUTTLE_ID INT, SHARD_ID INT, STATUS CHAR(1), READ_STATUS CHAR(1), WRITE_STATUS CHAR(1), REMARKS VARCHAR(500))");
        }
        max_scuttle := 128;
        err2  := testutil.PopulateShardMap(max_scuttle);
	if err2 != nil {
	    return err2
	}
	err3  := testutil.PopulateWhilelistShardMap();
        if err3 != nil {
            return err3
        }
	return err3
}


func TestMain (m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/* ##########################################################################################
 # Sharding enabled with num_shards > 0, white list enabled
 # Set shard id to shard #4
 # Sending insert, update, select request in same connection
 # Veriy all requests go to same shard #4
 # Verify app Log and CAL log
 #
 #############################################################################################*/

func TestSetShardID(t *testing.T) {
	fmt.Println ("TestSetShardID begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestSetShardID begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname := testutil.GetHostname()
        fmt.Println ("Hostname: ", hostname);
	db, err := sql.Open("hera", hostname + ":31002")
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// cleanup and insert one row in the table
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	fmt.Println ("Set Shard ID to shard 4")
	mux := gosqldriver.InnerConn(conn)
        shards, err:= mux.GetNumShards()
	if err != nil {
		t.Fatalf("GetNumShards failed: %s", err.Error())
	}
	if shards != 5 {
		t.Fatalf("Expected 5 shards")
	}
	
	fmt.Println ("Insert, update, select using same shard 4")
	mux.SetShardID(4)
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/* TestSetShardID */insert into test_simple_table_1 (ID, Name, Status) VALUES(:ID, :Name, :Status)")
	_, err = stmt.Exec(sql.Named("ID", 12346), sql.Named("Name", "Steve"), sql.Named("Status", 999))
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	
	fmt.Println ("Send an update request without shard key")
	stmt, _ = conn.PrepareContext(ctx, "/*cmd*/Update test_simple_table_1 set Status = 100 where Name=:Name")
	_, err = stmt.Exec(sql.Named("Name", "Steve"))
	if err != nil {
                t.Fatalf("Error updating row in table %s\n", err.Error())
        }
        err = tx.Commit()
        if err != nil {
                t.Fatalf("Error commit %s\n", err.Error())
        }

 	fmt.Println ("Verify insert, update requests are sent to shard 4")	
        count := testutil.RegexCount ("WORKER shd4.*Preparing.*TestSetShardID.*insert into test_simple_table_1")
	if (count < 1) {
            t.Fatalf ("Error: Insert Query does NOT go to shd4");
        }

        count = testutil.RegexCount ("WORKER shd4.*Preparing.*Update test_simple_table_1")
	if (count < 1) {
            t.Fatalf ("Error: Update Query does NOT go to shd4");
        }

        count = testutil.RegexCountFile ("T.*API.*CLIENT_SESSION_4", "cal.log")
	if (count < 1) {
            t.Fatalf ("Error: should see API.*CLIENT_SESSION_4 in CAL");
        }

	stmt, _ = conn.PrepareContext(ctx, "/* TestSetShardID */Select name, status from test_simple_table_1 where ID=:ID")
	rows, _ := stmt.Query(sql.Named("ID", 12346))
        if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	var name string
	var status int
	err = rows.Scan(&name, &status)
	if err != nil {
		t.Fatalf("Expected values %s", err.Error())
	}
	rows.Close()
	stmt.Close()

	cancel()
	conn.Close()

	fmt.Println ("Verify select request is sent to shard 4")
        if ( testutil.RegexCount ("WORKER shd4.*Preparing.*TestSetShardID.*Select name, status from test_simple_table_1") < 1) {
	      t.Fatalf ("Error: select query should go to shard4")
        }
	count = testutil.RegexCountFile ("T.*API.*CLIENT_SESSION_4", "cal.log")
        if (count < 2) {
            t.Fatalf ("Error: should see API.*CLIENT_SESSION_4 in CAL for fetch")
        }

	testutil.DoDefaultValidation(t)

	time.Sleep (time.Duration(2) * time.Second)
	logger.GetLogger().Log(logger.Debug, "TestSetShardID done  -------------------------------------------------------------")
}

