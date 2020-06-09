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
        fmt.Println ("setup() begin")
        appcfg := make(map[string]string)
        appcfg["bind_port"] = "31002"
        appcfg["log_level"] = "5"
        appcfg["log_file"] = "hera.log"
        appcfg["sharding_cfg_reload_interval"] = "0"
        appcfg["rac_sql_interval"] = "0"

	//For sharding
        appcfg["enable_sharding"] = "true"
        appcfg["num_shards"] = "5"
        appcfg["max_scuttle"] = "128"
        appcfg["shard_key_name"] = "accountID"
	appcfg["sharding_algo"] = "mod" 

        opscfg := make(map[string]string)
        opscfg["opscfg.default.server.max_connections"] = "3"
        opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}


func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
	err1 := testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
	testutil.RunDML("DROP TABLE IF EXISTS hera_shard_map")
	testutil.RunDML("CREATE TABLE hera_shard_map (SCUTTLE_ID INT, SHARD_ID INT, STATUS CHAR(1), READ_STATUS CHAR(1), WRITE_STATUS CHAR(1), REMARKS VARCHAR(500))");
	max_scuttle := 128;
	err3  := testutil.PopulateShardMap(max_scuttle);
	if err1 != nil { 
	    return err1
	}
	if err3 != nil {
            return err3
        }
	return err1
}


func TestMain (m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/* ##########################################################################################
 # Sharding enabled with num_shards > 0
 # Set shard id to 3 and perform an insert
 # Verify insert request is sent to shard 3
 # Reset shard ID
 # Verify next query without shard key will get error
 # Verify Log
 #
 #############################################################################################*/

func TestSetResetShardID(t *testing.T) {
        twoTask := os.Getenv("TWO_TASK_2")
        fmt.Println ("TWO_TASK_2: ", twoTask)
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

	fmt.Println ("Insert using shard 4")
        mux.SetShardID(4)
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/* TestSetResetShardID */insert into test_simple_table_1 (ID, Name, Status) VALUES(:ID, :Name, :Status)")
	_, err = stmt.Exec(sql.Named("ID", "12346"), sql.Named("Name", "Steve"), sql.Named("Status", 999))
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}
	

 	fmt.Println ("Verify insert request is sent to shard 4")	
        count := testutil.RegexCount ("WORKER shd4.*Preparing.*TestSetResetShardID.*insert into test_simple_table_1")
	if (count < 1) {
            t.Fatalf ("Error: Insert Query does NOT go to shd4");
        }

	fmt.Println ("Calling resetShardID, Verify next query without shard key will get error")        
        mux.SetShardID (-1)
        stmt, _ = conn.PrepareContext(ctx, "/* TestSetResetShardID */Select name, status from test_simple_table_1 where ID=:ID")
        stmt.Query(sql.Named("ID", 12346))

	fmt.Println ("Verify select request is rejected and server closes connection")
        count = testutil.RegexCount ("Error preprocessing sharding, hangup: HERA-373: no shard key or more than one or bad logical")
	if (count < 1) {
            t.Fatalf ("Error: No Shard key error should be thrown for fetch request");
        }
        cal_count := testutil.RegexCountFile ("SHARDING.*shard_key_not_found.*0", "cal.log")
	if (cal_count < 1) {
            t.Fatalf ("Error: No Shard key event for fetch request in CAL");
        }
	
	stmt.Close()

        cancel()
        conn.Close()

	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestSetResetShardID done  -------------------------------------------------------------")
}

