/*
Copyright 2022 PayPal Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


//
// Tests high load connection resiliency
//
// Setup config files:
// python cdbmake.py hera.txt | cdbmake occ.cdb occ.cdb.tmp
// python cdbmake.py cal_client.txt | cdbmake cal_client.cdb cal_client.cdb.tmp
// echo | cdbmake version.cdb version.cdb.tmp
//
// Environment required for test:
// export ORACLE_HOME=[/path/to]
// export TWO_TASK=[dbNameInTnsnames]
// export username=[dbUser]
// export password=[dbPasswd]
// export OPS_CFG_FILE=occ.cdb
// export TNS_ADMIN=./
//
// Build oracleworker:
// ( cd ../../../worker/cppworker/worker ; make -f ../build/makefile oracleworker )
// cp ../../../worker/cppworker/worker/oracleworker .
//
// The test will copy tnsnames.ora from 
// $ORACLE_HOME/network/admin/tnsnames.ora to the current directory. This 
// creates a different ora error code to help test that second passwords are
// only attempted if ORA-01017 invalid password happens in a connect attempt.
//
/*
for cmd/rude.go 

create or replace function cur_micros
return number
is
    rv number;
    upper number;
begin
    select to_number(to_char(current_timestamp,'SSFF')) into rv from dual;
    select to_number(to_char(current_timestamp,'MI')) into upper from dual;
    rv := rv + 1000000 * 60 * upper;
    -- adding hh24 overflows
    return rv;
end;
/
select cur_micros() from dual;
select cur_micros() as chkStmtSpeed from dual;
create or replace function usleep (micros in number)
return number
is
    finish number;
    cur number;
begin
    cur := cur_micros();
    finish := cur + micros;
    while cur < finish loop
        cur := cur_micros();
    end loop;
    return cur-finish+micros;
end;
/
select current_timestamp from dual;
select usleep(2111000) from dual;
select current_timestamp from dual;
create public synonym usleep for usleep;
grant execute on usleep to app;

*/
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	_ "github.com/paypal/hera/client/gosqldriver/tcp" // to register sql driver

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
var tableName string
var max_conn float64

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "24317"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["child.executable"] = "oracleworker" // must match return testutil.WorkerType
	appcfg["enable_heartbeat_fix"] = "true"
	appcfg["high_load_max_stranded_time_interval"] = "5000" // keeps test faster than 1hr

	appcfg["request_backlog_timeout"] = "1000"
	appcfg["soft_eviction_probability"] = "100"
	appcfg["high_load_skip_initiate_recover_pct"] = "100"
	appcfg["high_load_pct"] = "30"
	appcfg["init_limit_pct"] = "25"

	opscfg := make(map[string]string)
	max_conn = 15
	opscfg["opscfg.default.server.max_connections"] = fmt.Sprintf("%d", int(max_conn))
	opscfg["opscfg.default.server.log_level"] = "5"

	// copy tnsnames.ora so connections should work
	src := os.Getenv("ORACLE_HOME")+"/network/admin/tnsnames.ora"
	dest := os.Getenv("TNS_ADMIN")
	out,err := exec.Command("/bin/cp", src, dest).Output()
	if err != nil {
		fmt.Printf("could not cp %s %s\n",src,dest)
		fmt.Printf("cp tnsnames output %s\n",out)
		fmt.Print("could not copy tnsnames to curdir==TNS_ADMIN ", err)
	}

	opscfg["opscfg.default.server.saturation_recover_threshold"] = "10"
	opscfg["opscfg.hera.server.saturation_recover_throttle_rate"] = "100"

	return appcfg, opscfg, testutil.OracleWorker
}

func before() error {
	fmt.Printf("before function")
	return nil
}

func TestMain(m *testing.M) {
	fmt.Printf("TestMain function")
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func mkConn(t *testing.T, db *sql.DB) (*sql.Conn, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 7*24*3600*time.Second)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	return conn, cancel
}

func TestBadPassword(t *testing.T) {
	fmt.Printf("badPass disabled function")
	return
	logger.GetLogger().Log(logger.Debug, "TestBadPassword +++++++++++++")

	retryPasswordLogCnt := testutil.RegexCountFile("Login Retry Attempt...:1", "hera.log")
	if retryPasswordLogCnt != 1 {
		t.Fatalf("Error did not use second password")
	}

	/* **** cause different error than 1017 invalid password **** */
	// setup already set TNS_ADMIN to current directory
	out, err := exec.Command("rm tnsnames.ora").Output()
	if err != nil {
		fmt.Printf("rm tnsnames output %s", out)
		t.Fatal("could not rm tnsnames to curdir==TNS_ADMIN", err)
	}

	// restart worker
	out, err = exec.Command("pkill oracleworker").Output()
	if err != nil {
		fmt.Printf("restart worker %s", out)
		t.Fatal("could not restart worker", err)
	}
	time.Sleep(50 * time.Millisecond)
	if testutil.RegexCountFile("Login Retry Attempt...:1", "hera.log") != retryPasswordLogCnt {
		t.Fatalf("Error used second password something other than ora-1017-invalid-password")
	}

	/* **** restore ****/
	out, err = exec.Command("cp", os.Getenv("ORACLE_HOME")+"/network/admin/tnsnames.ora", os.Getenv("TNS_ADMIN")).Output()
	if err != nil {
		fmt.Printf("cp tnsnames output %s", out)
		t.Fatal("could not copy tnsnames to curdir==TNS_ADMIN", err)
	}

	// restart worker
	out, err = exec.Command("pkill", "oracleworker").Output()
	if err != nil {
		fmt.Printf("restart worker %s", out)
		t.Fatal("could not restart worker", err)
	}
}

func TestLimitConcurrentInit(t *testing.T) {
	fmt.Printf("T Lim Concur Init function")
	logger.GetLogger().Log(logger.Debug, "TestLimitConcurrentInit +++++++++++++")
	if max_conn < 15 {
		t.Error("max_conn likely too low to see TestLimitConcurrentInit")
	}
	if testutil.RegexCountFile("is too many in init state. waiting to start", "hera.log") < 1 {
		t.Fatalf("Error did not limit concurrent init [mk oracle conn]")
	}
}

func TestSkipOciBreak(t *testing.T) {
	logMsg := ""
	fmt.Printf("Skip OCI break function")
	logger.GetLogger().Log(logger.Debug, "TestSkipOciBreak +++++++++++++")
	hostname, _ := os.Hostname()
	fmt.Println("Hostname: ", hostname)
	db, err := sql.Open("hera", hostname+":24317")
	if err != nil {
		t.Fatal("Error db conn", err)
		return
	}
	logMsg = "db conn ok"; fmt.Printf(logMsg); logger.GetLogger().Log(logger.Debug, logMsg)
	db.SetMaxIdleConns(0)
	defer db.Close()

	conn, _ := mkConn(t, db)
	defer conn.Close()
	logMsg = "pre tbl rm"; fmt.Printf(logMsg); logger.GetLogger().Log(logger.Debug, logMsg)
	execSql(t, conn, "delete from resilience_at_load", false)
	// execSql() commits, mux releases conn

	logMsg = "add load"; fmt.Printf(logMsg); logger.GetLogger().Log(logger.Debug, logMsg)
	// simulate high load
	numConn := 6
	stuckConn := make([]*sql.Conn, numConn)
	stuckTx := make([]*sql.Tx, numConn)
	for i := 0; i < numConn; i++ {
		c, _ := mkConn(t, db)
		stuckConn[i] = c
		stuckTx[i] = execSql(t, c, fmt.Sprintf("insert into resilience_at_load(id,note)values(%d,'stuckConn')", 1000+i), true)
	}
	logMsg = "add load done"; fmt.Printf(logMsg); logger.GetLogger().Log(logger.Debug, logMsg)
	time.Sleep(1000*time.Millisecond)

	// helper starts sql and rudely stops
	// first with insert which stays in wait state for the client
	// we want to keep current behavior
	logMsg = "doing rude.go"; fmt.Printf(logMsg); logger.GetLogger().Log(logger.Debug, logMsg)
	out, err := exec.Command(os.Getenv("GOROOT")+"/bin/go", "run", "cmd/rude.go", "insert").Output()
	if err != nil {
		fmt.Printf("go run rude.go - output %s", out)
		fmt.Print("could not go run rude.go", err)
	}
	time.Sleep(50*time.Millisecond)

	// check for behavior we want
	logMsg = "pre chk"; fmt.Printf(logMsg); logger.GetLogger().Log(logger.Debug, logMsg)
	if testutil.RegexCountFile("is high load, skipping", "hera.log") != 0 {
		t.Fatal("skip oci break, on waiting db txn")
	}


	// slow query, client timesout/crashes
	logMsg = "add slow clients"; fmt.Printf(logMsg); logger.GetLogger().Log(logger.Debug, logMsg)
	out, err = exec.Command(os.Getenv("GOROOT")+"/bin/go", "run", "cmd/rude.go", "usleep").Output()
	if err != nil {
		fmt.Printf("go run rude.go - output %s", out)
		fmt.Print("could not go run rude.go", err)
	}
	time.Sleep(50*time.Millisecond)

	// check for behavior we want
	logMsg = "pre chk, slow"; fmt.Printf(logMsg); logger.GetLogger().Log(logger.Debug, logMsg)
	if testutil.RegexCountFile("is high load, skipping", "hera.log") < 1 {
		t.Fatal("Error did not skip oci break")
	}

	// start restore
	// release stuckConn
	logMsg = "pre restore"; fmt.Printf(logMsg); logger.GetLogger().Log(logger.Debug, logMsg)
	for i := 0; i < numConn; i++ {
		stuckTx[i].Rollback()
		stuckConn[i].Close()
	}
	logMsg = "chk restore"; fmt.Printf(logMsg); logger.GetLogger().Log(logger.Debug, logMsg)
	acpt4 := 0
	for i := 0; i < 33; i++ {
		acpt4, _ = testutil.StatelogGetField(2)
		logger.GetLogger().Log(logger.Debug, "TestSkipOciBreak +++++++++++++ chk acpt", acpt4)
		if int(max_conn) == acpt4 {
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}
	if int(max_conn) != acpt4 {
		t.Fatal("conn's did not restore")
	}
	logMsg = "skip oci break test done"; fmt.Printf(logMsg); logger.GetLogger().Log(logger.Debug, logMsg)
	logger.GetLogger().Log(logger.Debug, "TestSkipOciBreak +++++++++++++ done")
}

func execSql(t *testing.T, conn *sql.Conn, sql string, skipCommit bool) *sql.Tx {
	ctx, _ := context.WithTimeout(context.Background(), 7*24*3600*time.Second)
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Error startT %s %s\n", sql, err.Error())
	}
	stmt, err := tx.PrepareContext(ctx, sql)
	if err != nil {
		t.Fatalf("Error prep %s %s\n", sql, err.Error())
	}
	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("Error exec %s %s\n", sql, err.Error())
	}
	if skipCommit {
		return tx
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s %s\n", sql, err.Error())
	}
	return nil
}
