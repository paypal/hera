package main

import (
	"bytes"
        "os/exec"
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
	"github.com/paypal/hera/utility/logger"
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

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	} 
	return appcfg, opscfg, testutil.MySQLWorker
}

//Helper function to delete and populate shard map with 128 scuttles
func populate_cam_shard_map() (string,error) {
	cmd := exec.Command("mysql","-h",os.Getenv("mysql_ip"),"-p1-testDb","-uroot", "heratestdb", " < populate_cam_shard_map.sql")
        //cmd.Stdin = strings.NewReader(sql)
        var cmdOutBuf bytes.Buffer
        cmd.Stdout = &cmdOutBuf
        cmd.Run()
        return cmdOutBuf.String(), nil
}


func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_2")
	testutil.RunDML("CREATE TABLE test_simple_table_2 (accountID VARCHAR(64) PRIMARY KEY, NAME VARCHAR(64), STATUS VARCHAR(64), CONDN VARCHAR(64))")
	testutil.RunMysql("DROP TABLE IF EXISTS hera_shard_map;")
	testutil.RunMysql("CREATE TABLE hera_shard_map (SCUTTLE_ID INT, SHARD_ID INT, STATUS CHAR(1), READ_STATUS CHAR(1), WRITE_STATUS CHAR(1), REMARKS VARCHAR(500));");
	out,err2 := testutil.RunMysql (`DELIMITER $$
 DROP PROCEDURE IF EXISTS populate_shard_map$$
 CREATE PROCEDURE populate_shard_map(IN num INT)
BEGIN
   DECLARE x INT;
   SET x = 0;

   While x < num DO
      INSERT INTO hera_shard_map VALUES (x, mod(x,5),'Y','Y','Y','Initial');
      SET x = x + 1;
   END WHILE;
   COMMIT;
END$$
DELIMITER ;`);
	if err2 != nil {
		fmt.Printf (out);
		fmt.Printf("err after creating procedure "+err2.Error())
		return err2
	}
	out3, err3 := populate_cam_shard_map()
	if err3 != nil {
		fmt.Printf (out3);
		fmt.Printf("err after run populate_cam_shard_map"+err3.Error())
		return err3
	}

	max_scuttle := 128;
	err3  = testutil.PopulateShardMap(max_scuttle);
	if err2 != nil {
	    return err2
	}
	if err3 != nil {
            return err3
        }
	return err2
}


func TestMain (m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/* ##########################################################################################
 # Sharding enabled with num_shards > 0
 # Sending two DMLs, first DML with shard key pass and 2nd one no shard key
 # Verify 2nd request fails due to no shard key
 # Verify Log, CAL events
 # Send update, fetch requests with auto discovery
 # Veriy update is sent to correct shard
 # Veriy fetch is sent to correct shard and fields are updated correctly
 # Verify Log
 #
 #############################################################################################*/

func TestShardBasic(t *testing.T) {
	fmt.Println ("TestShardBasic begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	logger.GetLogger().Log(logger.Debug, "TestShardBasic begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	time.Sleep(8 * time.Second);
	
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
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := tx.PrepareContext(ctx, "/*cmd*/insert into test_simple_table_2 (accountID, Name, Status) VALUES(:accountID, :Name, :Status)")
	_, err = stmt.Exec(sql.Named("accountID", "12346"), sql.Named("Name", "Steve"), sql.Named("Status", "done"))
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}
	
	fmt.Println ("Send an update request without shard key")
	stmt, _ = conn.PrepareContext(ctx, "/*cmd*/Update test_simple_table_2 set Status = 'progess' where Name=?")
	stmt.Exec("Steve")

	stmt.Close()

	cancel()
	conn.Close() 

 	fmt.Println ("Verify insert request is sent to shard 3")	
        count := testutil.RegexCount ("WORKER shd3.*Preparing.*insert into test_simple_table_2")
	if (count < 1) {
            t.Fatalf ("Error: Insert Query does NOT go to shd3");
        }

 	fmt.Println ("Verify no shard key error is thrown for fetch request")	
        count = testutil.RegexCount ("Error preprocessing sharding, hangup: HERA-373: no shard key or more than one or bad logical")
	if (count < 1) {
            t.Fatalf ("Error: No Shard key error should be thrown for fetch request");
        }
        cal_count := testutil.RegexCountFile ("SHARDING.*shard_key_not_found.*0.*sql=1093137600", "cal.log")
	if (cal_count < 1) {
            t.Fatalf ("Error: No Shard key event for fetch request in CAL");
        }
	
 	fmt.Println ("Check CAL log for correct events");
        cal_count = testutil.RegexCountFile ("SHARDING.*shard_key_auto_discovery.*0.*shardkey=accountid|12346&shardid=3&scuttleid=", "cal.log")
	if (cal_count < 1) {
            t.Fatalf ("Error: No shard_key_auto_discovery event seen in CAL");
        }
        cal_count = testutil.RegexCountFile ("T.*API.*CLIENT_SESSION_3", "cal.log")
	if (cal_count < 1) {
            t.Fatalf ("Error: Request is not executed by shard 3 as expected");
        }

	fmt.Println ("Open new connection as previous connection is already closed");
	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	conn1, err := db.Conn(ctx1)
        if err != nil {
                t.Fatalf("Error getting connection %s\n", err.Error())
        }
        tx1, _ := conn1.BeginTx(ctx1, nil)
	fmt.Println ("Update table with shard key passed");
        stmt1, _ := tx1.PrepareContext(ctx1, "/*cmd*/ update test_simple_table_2 set Status = 'In Progress' where accountID in (:accountID)")
        _, err = stmt1.Exec(sql.Named("accountID", "12346"))
        if err != nil {
                t.Fatalf("Error updating row in table %s\n", err.Error())
        }
        err = tx1.Commit()
        if err != nil {
                t.Fatalf("Error commit %s\n", err.Error())
        }
	stmt1, _ = conn1.PrepareContext(ctx1, "/*TestShardingBasic*/Select name, status from test_simple_table_2 where accountID=:accountID")
	rows1, _ := stmt1.Query(sql.Named("accountID", "12346"))
        if !rows1.Next() {
		t.Fatalf("Expected 1 row")
	}
	var name, status string
	err = rows1.Scan(&name, &status)
	if err != nil {
		t.Fatalf("Expected values %s", err.Error())
	}
	if  (name != "Steve" || status != "In Progress") {
		t.Fatalf("***Error: name= %s, status=%s", name, status)
	}
	rows1.Close()
	stmt1.Close()

	cancel1()
	conn1.Close()

	fmt.Println ("Verify update request is sent to shard 3")
        count1 := testutil.RegexCount ("WORKER shd3.*Preparing.*update test_simple_table_2")
	if (count1 < 1) {
            t.Fatalf ("Error: Update Query does NOT go to shd3");
        }

	fmt.Println ("Verify select request is sent to shard 3")
        count1 = testutil.RegexCount ("WORKER shd3.*Preparing.*TestShardingBasic.*Select name, status from test_simple_table_2")
	if (count1 < 1) {
            t.Fatalf ("Error: Select Query does NOT go to shd3");
        }
	testutil.DoDefaultValidation(t)
	logger.GetLogger().Log(logger.Debug, "TestShardBasic done  -------------------------------------------------------------")
}

