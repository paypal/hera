package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	//"github.com/paypal/hera/client/gosqldriver"
	_ "github.com/paypal/hera/client/gosqldriver/tcp" /*to register the driver*/

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
var tableName string
var max_conn float64

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["bind_eviction_names"] = "p"
	appcfg["bind_eviction_threshold_pct"] = "50"

	appcfg["request_backlog_timeout"] = "1000"
	appcfg["soft_eviction_probability"] = "10"

	opscfg := make(map[string]string)
	max_conn = 50
	opscfg["opscfg.default.server.max_connections"] = fmt.Sprintf("%d", 10)
	opscfg["opscfg.default.server.log_level"] = "5"

	opscfg["opscfg.default.server.saturation_recover_threshold"] = "10"
	//opscfg["opscfg.default.server.saturation_recover_throttle_rate"]= "100"
	opscfg["opscfg.hera.server.saturation_recover_throttle_rate"] = "100"
	// saturation_recover_throttle_rate

	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	fmt.Printf("before run mysql")
	testutil.RunMysql("create table sleep_info (id bigint, seconds float);")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(10, 0.01);")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(100, 0.1);")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(1600, 2.6);")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(21001111, 0.1);")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(22001111, 0.1);")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(29001111, 2.9);")
	out, err := testutil.RunMysql(`DELIMITER $$
CREATE FUNCTION sleep_option (id bigint)
RETURNS float
DETERMINISTIC
BEGIN
  declare dur float;
  declare rv bigint;
  select max(seconds) into dur from sleep_info where sleep_info.id=id;
  select sleep(dur) into rv;
  RETURN dur;
END$$
DELIMITER ;`)
	if err != nil {
		fmt.Printf("err after run mysql " + err.Error())
		return nil
	}
	fmt.Printf("after run mysql " + out) // */
	return nil
}

func TestMain(m *testing.M) {
	logger.GetLogger().Log(logger.Debug, "begin 20230918kkang TestMain")
	fmt.Printf("TestMain 20230918kkang\n")
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func sleepyQ(conn *sql.Conn, delayRow int) error {
	stmt, err := conn.PrepareContext(context.Background(), "select * from sleep_info where ( seconds > sleep_option(?) or seconds > 0.0 )")
	if err != nil {
		fmt.Printf("Error preparing sleepyQ %s\n", err.Error())
		return err
	}
	defer stmt.Close()
	rows, err := stmt.Query(delayRow)
	if err != nil {
		fmt.Printf("Error query sleepyQ %s\n", err.Error())
		return err
	}
	defer rows.Close()
	return nil
}

func sleepyDmlQ(conn *sql.Conn, delayRow int) error {
	inserQuery := "insert into sleep_info (id,seconds) values (:id, sleep_option(:seconds))"
	updateQuery := "update sleep_info set seconds = sleep_option(:seconds) where id=:id"
	defer func(conn *sql.Conn) {
		err := conn.Close()
		if err != nil {
			fmt.Printf("Error closing conn %s\n", err.Error())
		}
	}(conn)
	tx, _ := conn.BeginTx(context.Background(), nil)
	inst1, err := conn.PrepareContext(context.Background(), inserQuery)
	if err != nil {
		fmt.Printf("Error preparing sleepyDmlQ %s\n", err.Error())
		return err
	}
	defer func(inst1 *sql.Stmt) {
		err := inst1.Close()
		if err != nil {
			fmt.Printf("Error closing insert statement sleepyDmlQ %s\n", err.Error())
		}
	}(inst1)
	_, err = inst1.ExecContext(context.Background(), sql.Named("id", rand.Int()), sql.Named("seconds", delayRow))
	if err != nil {
		fmt.Printf("Error query sleepyDmlQ %s\n", err.Error())
		return err
	}
	updateStmt, err := conn.PrepareContext(context.Background(), updateQuery)
	if err != nil {
		fmt.Printf("Error preparing sleepyDmlQ %s\n", err.Error())
		return err
	}
	defer func(updateStmt *sql.Stmt) {
		err := updateStmt.Close()
		if err != nil {
			fmt.Printf("Error closing update statement sleepyDmlQ %s\n", err.Error())
		}
	}(updateStmt)
	_, err = updateStmt.ExecContext(context.Background(), sql.Named("id", rand.Int()), sql.Named("seconds", delayRow))
	if err != nil {
		fmt.Printf("Error query sleepyDmlQ %s\n", err.Error())
		return err
	}
	err = tx.Commit()
	if err != nil {
		fmt.Printf("Error committing sleepyDmlQ %s\n", err.Error())
		return err
	}
	return nil
}

func simpleEvict() {
	db, err := sql.Open("hera", "127.0.0.1:31002")
	if err != nil {
		fmt.Printf("Error db %s\n", err.Error())
		return
	}
	db.SetConnMaxLifetime(2 * time.Second)
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(22111)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		fmt.Printf("Error conn %s\n", err.Error())
		return
	}
	defer conn.Close()
	sleepyQ(conn, 1600)

	for i := 0; i < int(max_conn)+1; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			fmt.Printf("Error #%d conn %s\n", i, err.Error())
			continue
		}
		defer conn.Close()
		fmt.Printf("connected %d\n", i)
		go sleepyQ(conn, 1600)
	}
	fmt.Printf("done with bklg setup\n")
	for i := 0; i < 55; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			fmt.Printf("Error #%d conn %s\n", i, err.Error())
			continue
		}
		defer conn.Close()
		fmt.Printf("connected %d heartbeat\n", i)
		go sleepyQ(conn, 10)
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Printf("done waiting for bklg\n")
}

var normCliErr error

func NormCliErr() error {
	if normCliErr == nil {
		normCliErr = fmt.Errorf("normal client got error")
	}
	return normCliErr
}

func TestSqlEvict(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestSqlEvict begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	simpleEvict()
	if testutil.RegexCountFile("HERA-100: backlog timeout", "hera.log") == 0 {
		t.Fatal("backlog timeout was not triggered")
	}
	if testutil.RegexCountFile("coordinator dispatchrequest: no worker HERA-104: saturation soft sql eviction", "hera.log") == 0 {
		t.Fatal("soft eviction was not triggered")
	}
	if testutil.RegexCountFile("coordinator dispatchrequest: stranded conn HERA-101: saturation kill", "hera.log") == 0 {
		t.Fatal("eviction was not triggered")
	}
	logger.GetLogger().Log(logger.Debug, "TestSqlEvict stop +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	time.Sleep(10 * time.Second)
} // */

func TestSqlEvictDML(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestSqlEvictDML begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	dmlEvict()
	if testutil.RegexCountFile("HERA-100: backlog timeout", "hera.log") == 0 {
		t.Fatal("backlog timeout was not triggered")
	}
	if testutil.RegexCountFile("coordinator dispatchrequest: no worker HERA-104: saturation soft sql eviction", "hera.log") == 0 {
		t.Fatal("soft eviction was not triggered")
	}
	if testutil.RegexCountFile("coordinator dispatchrequest: stranded conn HERA-101: saturation kill", "hera.log") == 0 {
		t.Fatal("eviction was not triggered")
	}
	logger.GetLogger().Log(logger.Debug, "TestSqlEvictDML stop +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	time.Sleep(10 * time.Second)
}

func dmlEvict() {
	db, err := sql.Open("hera", "127.0.0.1:31002")
	if err != nil {
		fmt.Printf("Error db %s\n", err.Error())
		return
	}
	db.SetConnMaxLifetime(2 * time.Second)
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(22111)
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Printf("Error closing db %s\n", err.Error())
		}
	}(db)

	conn, err := db.Conn(context.Background())
	if err != nil {
		fmt.Printf("Error conn %s\n", err.Error())
		return
	}
	err = sleepyDmlQ(conn, 1600)
	if err != nil {
		fmt.Printf("Error Executing first sleepyDmlQ %s\n", err.Error())
		return
	}

	for i := 0; i < int(max_conn)+1; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			fmt.Printf("Error #%d conn %s\n", i, err.Error())
			continue
		}
		time.Sleep(time.Millisecond * 100)
		fmt.Printf("connection count %d\n", i)
		go func(index int) {
			err := sleepyDmlQ(conn, 1600)
			if err != nil {
				fmt.Printf("Long query Request Id: %d Error executing the sleepyDmlQ %s\n", index, err.Error())
			}
		}(i)
	}

	for i := 0; i < 50; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			fmt.Printf("Error #%d conn %s\n", i, err.Error())
			continue
		}
		time.Sleep(time.Millisecond * 100)
		fmt.Printf("connection count %d\n", i)
		go func(index int) {
			err := sleepyDmlQ(conn, 1600)
			if err != nil {
				fmt.Printf("Request id: %d Error executing the sleepyDmlQ %s\n", index, err.Error())
			}
		}(i)
	}
}
