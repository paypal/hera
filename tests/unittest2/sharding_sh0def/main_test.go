package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
	"os"
	"strings"
	"testing"
	"time"
)

var mx testutil.Mux
//var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "occ.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["enable_sharding"] = "true"
	appcfg["enable_whitelist_test"] = "true"
	appcfg["shard_key_name"] = "email_addr"
	appcfg["shard_key_value_type_is_string"] = "true"
	appcfg["num_shards"] = "2"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_requests_per_child"] = "333"
	opscfg["opscfg.default.server.max_lifespan_per_child"] = "555"
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

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
func setupShardMap(t *testing.T) {
	twoTask := os.Getenv("TWO_TASK")
	if !strings.HasPrefix(twoTask, "tcp") {
		// not mysql
		return
	}
	os.Setenv("TWO_TASK_1", twoTask)
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	defer conn.Close()

	testutil.RunDML("create table test_str_sk (email_addr varchar(64), note varchar(64))")
	testutil.RunDML("create table hera_shard_map ( scuttle_id smallint not null, shard_id tinyint not null, status char(1) , read_status char(1), write_status char(1), remarks varchar(500))")

	for i := 0; i < 1024; i++ {
		testutil.RunDML(fmt.Sprintf("insert into hera_shard_map ( scuttle_id, shard_id, status, read_status, write_status ) values ( %d, 1, 'Y', 'Y', 'Y' )", i) )
	}
}

func TestShardingSh0Def(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestShardingSh0Def function, now setting up shard map")
	setupShardMap(t)
	logger.GetLogger().Log(logger.Debug, "TestShardingSh0Def begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")



	// -1 as the shard does a reset so all the automatic things should work instead of assigning to a specific shard
	shard := -1
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	defer conn.Close()

	tx, _ := conn.BeginTx(ctx, nil)
	// create table test_str_sk (email_addr varchar(64), note varchar(64));
	sqlDesc := "ins|test_str_sk"
	//stmt, err := tx.PrepareContext(ctx, "/*"+sqlDesc+"*/ insert into test_str_sk (email_addr, note) VALUES ( :email_addr, :note)")
	stmt, err := tx.PrepareContext(ctx, "/*"+sqlDesc+"*/ insert into test_str_sk (note) VALUES ( :note)")
	if err != nil {
		t.Fatalf("Error prep %s %s\n", sqlDesc, err.Error())
	}
	//_, err = stmt.Exec("FutureString", "not an email", sql.Named("email_addr", ")
	_, err = stmt.Exec(sql.Named("note", "not an email"))
	if err != nil {
		t.Fatalf("Error exec %s %s\n", sqlDesc, err.Error())
	}
	if testutil.RegexCountFile("shd0.*:note", "occ.log") == 0 {
		t.Fatalf("insert without shard key did not land on sh0")
	}
	if testutil.RegexCountFile("shd[^0].*:note", "occ.log") != 0 {
		t.Fatalf("insert without shard key landed on wrong shard, not the expected sh0")
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s %s\n", sqlDesc, err.Error())
	}

	logger.GetLogger().Log(logger.Debug, "TestShardingSh0Def done  -------------------------------------------------------------")
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
