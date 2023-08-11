package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
//var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {
	fmt.Println ("setup() begin")
	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["enable_sharding"] = "true"
	appcfg["shard_key_name"] = "email_addr"
	appcfg["shard_key_value_type_is_string"] = "true"
	appcfg["num_shards"] = "2"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_requests_per_child"] = "333"
	opscfg["opscfg.default.server.max_lifespan_per_child"] = "555"
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	} 
	return appcfg, opscfg, testutil.MySQLWorker
}

func setupDb() error {
	// TODO make utility functions that bypass the sharded part so we can set things up
	/* tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_mux_test"
	}

	testutil.RunDML("DROP TABLE " + tableName)
	return testutil.RunDML("CREATE TABLE " + tableName + " ( id bigint, int_val bigint, str_val varchar(128) )")
	// */
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

/*
export TWO_TASK_0=tcp(db.host..
export TWO_TASK_1=tcp(db.host..
create table hera_shard_map
(
    scuttle_id smallint not null,
    shard_id tinyint not null,
    status char(1) ,
    read_status char(1),
    write_status char(1),
    remarks varchar(500)
);
drop procedure populate_shard_map;
DELIMITER $$
create procedure populate_shard_map ( )
BEGIN
DECLARE counter  INT;              
SET     counter  = 0;              
1_to_5_counter: WHILE counter < 1024 DO                          
    insert into hera_shard_map ( scuttle_id, shard_id, status, read_status, write_status ) values ( counter, 0, 'Y', 'Y', 'Y' );
    SET counter = counter + 1;               
END WHILE 1_to_5_counter;              
END
$$
DELIMITER ;
delete from hera_shard_map;
call populate_shard_map();


and for oracle
create table hera_shard_map
(
    scuttle_id Number not null,
    shard_id Number not null,
    status char(1) ,
    read_status char(1),
    write_status char(1),
    remarks varchar2(500)
);

BEGIN
   FOR i IN 0..1023 LOOP
      INSERT INTO hera_shard_map VALUES (i,0,'Y','Y','Y','Initial');
   END LOOP;
   COMMIT;
END;
/
*/
func setupShardMap() {
	testutil.RunDML("DROP TABLE IF EXISTS test_str_sk")
	testutil.RunDML("create table test_str_sk (email_addr varchar(64), note varchar(64))")
	testutil.RunDML("DROP TABLE IF EXISTS hera_shard_map")
	testutil.RunDML("create table hera_shard_map ( scuttle_id smallint not null, shard_id smallint not null, status char(1) , read_status char(1), write_status char(1), remarks varchar(500))")
	for i := 0; i < 1024; i++ {
		testutil.RunDML(fmt.Sprintf("insert into hera_shard_map ( scuttle_id, shard_id, status, read_status, write_status ) values ( %d, 0, 'Y', 'Y', 'Y' )", i) )
	}
}

func TestShardingStr(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestShardingStr function, creating tables and setting up shard map")
	setupShardMap()
	logger.GetLogger().Log(logger.Debug, "TestShardingStr begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")


	hostname,_ := os.Hostname()
    fmt.Println ("Hostname: ", hostname);
	
	db, err := sql.Open("hera", hostname + ":31002")
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	defer conn.Close()

	tx, _ := conn.BeginTx(ctx, nil)
	// create table test_str_sk (email_addr varchar(64), note varchar(64));
	sqlDesc := "ins_test_str_sk"
	stmt, err := tx.PrepareContext(ctx, "/*"+sqlDesc+"*/ insert into test_str_sk (email_addr, note) VALUES ( :email_addr, :note)")
	if err != nil {
		t.Fatalf("Error prep %s %s\n", sqlDesc, err.Error())
	}
	_, err = stmt.Exec(sql.Named("email_addr", "FutureString"), sql.Named("note", "not an email"))
	if err != nil {
		t.Fatalf("Error exec %s %s\n", sqlDesc, err.Error())
	}
	if testutil.RegexCountFile("bucket = 786", "hera.log") != 1 {
		t.Fatalf("Error did not map to proper scuttle bucket")
	}
	_, err = stmt.Exec(sql.Named("email_addr", "FutureStringWithMod1024"), sql.Named("note", "not an email"))
	if err != nil {
		t.Fatalf("Error exec2 %s %s\n", sqlDesc, err.Error())
	}
	if testutil.RegexCountFile("bucket = 362", "hera.log") != 1 {
		t.Fatalf("Error2 did not map to proper scuttle bucket")
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s %s\n", sqlDesc, err.Error())
	}
	stmt.Close()
	cancel()
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestShardingStr done  -------------------------------------------------------------")
}
func getRows(id int, conn *sql.Conn) (int) {
	out := 0
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	stmt, _ := conn.PrepareContext(ctx, "/*cmd*/Select id, int_val from fooTb where id=?")
	rows, _ := stmt.Query(id)
	for rows.Next() {
		out++
	}

	rows.Close()
	stmt.Close()
	return out;
}
