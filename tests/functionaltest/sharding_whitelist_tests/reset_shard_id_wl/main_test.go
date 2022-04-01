package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/paypal/hera/tests/functionaltest/testutil"
        "github.com/paypal/hera/utility/logger"
        "github.com/paypal/hera/client/gosqldriver"
        _ "github.com/paypal/hera/client/gosqldriver/tcp"
	"os"
	"testing"
	"time"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {
        appcfg := make(map[string]string)
        appcfg["bind_port"] = "31002"
        appcfg["log_level"] = "5"
        appcfg["log_file"] = "hera.log"
        appcfg["sharding_cfg_reload_interval"] = "0"
        appcfg["rac_sql_interval"] = "0"

	//For sharding
        appcfg["enable_sharding"] = "true"
        appcfg["num_shards"] = "5"
        appcfg["shard_key_name"] = "accountID"
        appcfg["enable_whitelist_test"] = "true"
        appcfg["whitelist_children"] = "2"
	appcfg["max_scuttle"] = "128"
	appcfg["sharding_algo"] = "mod" 

        opscfg := make(map[string]string)
        opscfg["opscfg.default.server.max_connections"] = "4"
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
 # Sharding enabled with num_shards = 5, white list enabled
 # Set Shard ID to shard 3
 # Sending Insert request without shard key  
 # Verify insert request go to shard 3
 # Reset Shard ID
 # Send fetch request without shard key
 # Verify fetch request goes to shard 0
 # Verify Log & CAL log
 #
 #############################################################################################*/

func TestSetResetShardID(t *testing.T) {
        twoTask := os.Getenv("TWO_TASK_READ_4")
        fmt.Println ("TWO_TASK_4: ", twoTask)
	fmt.Println ("TestSetResetShardID begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestSetResetShardID begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	hostname,_ := os.Hostname()
        fmt.Println ("Hostname: ", hostname);
	db, err := sql.Open("hera", hostname + ":31002")
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

	fmt.Println ("Set Shard ID to shard 3")
	mux :=  gosqldriver.InnerConn(conn)
        shards, err:= mux.GetNumShards()
	if err != nil {
		t.Fatalf("GetNumShards failed: %s", err.Error())
	}
	if shards != 5 {
		t.Fatalf("Expected 5 shards")
	}
	
	fmt.Println ("Insert using shard 3")
	mux.SetShardID(3)
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/* TestSetResetShardID */insert into test_simple_table_1 (ID, Name, Status) VALUES(:ID, :Name, :Status)")
	_, err = stmt.Exec(sql.Named("ID", 12346), sql.Named("Name", "Steve"), sql.Named("Status", 999))
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}

        err = tx.Commit()
        if err != nil {
                t.Fatalf("Error commit %s\n", err.Error())
        }

	time.Sleep (time.Duration(2) * time.Second)
	fmt.Println ("Verify insert request is sent to shard 3")
	count := testutil.RegexCount ("WORKER shd3.*Preparing.*TestSetResetShardID.*insert into test_simple_table_1")
        if (count < 1) {
            t.Fatalf ("Error: Insert Query does NOT go to shd3");
        }
	cal_count := testutil.RegexCountFile ("T.*API.*CLIENT_SESSION_3", "cal.log")
        if (cal_count < 1) {
            t.Fatalf ("Error: No API.*CLIENT_SESSION_3 in CAL");
        }

	fmt.Println ("Calling resetShardID, Verify next query without shard key will be processed using shard 0")
	mux.SetShardID (-1)
	stmt, _ = conn.PrepareContext(ctx, "/* TestSetResetShardID */Select name, status from test_simple_table_1 where ID=:ID")
	rows, _ := stmt.Query(sql.Named("ID", 12346))
	if !rows.Next() {
                t.Fatalf("Expected 1 row")
        }
        rows.Close()
	stmt.Close()

	cancel()
	conn.Close()

	count = testutil.RegexCount ("WORKER shd0.*Preparing.*TestSetResetShardID.*Select name, status from test_simple_table_1")
        if (count < 1) {
            t.Fatalf ("Error: Insert Query does NOT go to shd1");
        }
	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestSetResetShardID done  -------------------------------------------------------------")
}

