package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/paypal/hera/tests/functionaltest/testutil"
        "github.com/paypal/hera/utility/logger"
	"os"
	"testing"
	"time"
	_ "github.com/paypal/hera/client/gosqldriver/tcp"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {
        fmt.Println ("setup() begin")
        appcfg := make(map[string]string)
        appcfg["bind_port"] = "31002"
        appcfg["log_level"] = "5"
        appcfg["log_file"] = "hera.log"
        appcfg["rac_sql_interval"] = "0"

	//For sharding
        appcfg["enable_sharding"] = "true"
        appcfg["num_shards"] = "5"
        appcfg["shard_key_name"] = "ACCOUNTID"
	appcfg["enable_whitelist_test"] ="true";
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
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_2")
	testutil.RunDML("DROP TABLE IF EXISTS hera_shard_map")
	err1 := testutil.RunDML("CREATE TABLE test_simple_table_2 (accountID VARCHAR(64) PRIMARY KEY, NAME VARCHAR(64), STATUS VARCHAR(64), CONDN VARCHAR(64))")
	if err1 != nil { 
	    return err1
	}
	if os.Getenv("WORKER") == "postgres" {
                testutil.RunDML("CREATE TABLE hera_shard_map (SCUTTLE_ID BIGINT, SHARD_ID BIGINT, STATUS CHAR(1), READ_STATUS CHAR(1), WRITE_STATUS CHAR(1), REMARKS VARCHAR(500))");
        } else { 
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
        return err1
}


func TestMain (m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/* ##########################################################################################
 # Sharding enabled with num_shards = 5, white list enabled
 # Sending first insert DML using auto discovery 
 # Verify insert request sends to correct shard
 # Sending a fetch with no shard key
 # Verify fetch request is sent to shard 0 
 # Verify Log, CAL events
 #
 #############################################################################################*/
func TestNoShardNoError(t *testing.T) {
	fmt.Println ("TestNoShardNoError begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestNoShardNoError begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

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
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*cmd*/insert into test_simple_table_2 (accountID, Name, Status) VALUES(:AccountID, :Name, :Status)")
	_, err = stmt.Exec(sql.Named("AccountID", "12345"), sql.Named("Name", "Steve"), sql.Named("Status", "done"))
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}
	
	fmt.Println ("Send a fetch request without shard key, verify request goes to shard 0")
	stmt, _ = conn.PrepareContext(ctx, "/*cmd*/Select status from test_simple_table_2 where Name=?")
	rows, _ := stmt.Query("Steve")
	if !rows.Next() {
                t.Fatalf("Expected 1 row")
        }
	rows.Close()
	stmt.Close()

	cancel()
	conn.Close()

	time.Sleep (time.Duration(2) * time.Second)
 	fmt.Println ("Verify insert request is sent to shard 2")	
        count := testutil.RegexCount ("WORKER shd2.*Preparing.*insert into test_simple_table_2")
	if (count < 1) {
            t.Fatalf ("Error: Insert query does NOT go to shd2");
        }

	fmt.Println ("Verify there is no shard key error for fetch request")
        count = testutil.RegexCount ("Error preprocessing sharding, hangup: OCC-373: no shard key or more than one or bad logical db false")
	if (count > 0) {
            t.Fatalf ("Error: should NOT get no shard key error");
        }
	fmt.Println ("Check CAL log for correct events");
        cal_count := testutil.RegexCountFile ("SHARDING.*shard_key_auto_discovery.*0.*shardkey=accountid|12345&shardid=3&scuttleid=428", "cal.log")
	if (cal_count < 1) {
            t.Fatalf ("Error: Did NOT get shard_key_auto_discovery in CAL log");
        }
	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestNoShardNoError done  -------------------------------------------------------------")
}

