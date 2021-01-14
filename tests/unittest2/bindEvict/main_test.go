package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"


        //"github.com/paypal/hera/client/gosqldriver"
        _ "github.com/paypal/hera/client/gosqldriver/tcp" /*to register the driver*/

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
	"github.com/paypal/hera/lib"
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

	appcfg["request_backlog_timeout"]="1000";
	appcfg["soft_eviction_probability"]="100";

	opscfg := make(map[string]string)
	max_conn = 25
	opscfg["opscfg.default.server.max_connections"] = fmt.Sprintf("%d", int(max_conn))
	opscfg["opscfg.default.server.log_level"] = "5"


	opscfg["opscfg.default.server.saturation_recover_threshold"]= "10"
	//opscfg["opscfg.default.server.saturation_recover_throttle_rate"]= "100"
	opscfg["opscfg.hera.server.saturation_recover_throttle_rate"]= "100"
	// saturation_recover_throttle_rate

	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	fmt.Printf("before run mysql")
	testutil.RunMysql("create table sleep_info (id bigint, seconds float)")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(10, 0.01)")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(100, 0.1)")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(1600, 2.6)")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(21001111, 0.1)")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(22001111, 0.1)")
	testutil.RunMysql("insert into sleep_info (id,seconds) values(29001111, 2.9)")
	out,err := testutil.RunMysql(`DELIMITER $$
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
DELIMITER ;`);
	if err != nil {
		fmt.Printf("err after run mysql "+err.Error())
		return nil
	}
	fmt.Printf("after run mysql "+out) // */
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}


func sleepyQ(conn *sql.Conn, delayRow int) (error) {
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

	for i := 0; i < int(max_conn)+1 ; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			fmt.Printf("Error #%d conn %s\n", i, err.Error())
			continue
		}
		defer conn.Close()
		fmt.Printf("connected %d\n", i)
		go sleepyQ(conn,1600)
	}
	fmt.Printf("done with bklg setup\n")
	for i := 0; i < 55 ; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			fmt.Printf("Error #%d conn %s\n", i, err.Error())
			continue
		}
		defer conn.Close()
		fmt.Printf("connected %d heartbeat\n", i)
		go sleepyQ(conn,10)
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Printf("done waiting for bklg\n")
}

func TestSqlEvict(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestSqlEvict begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	simpleEvict()
	if (testutil.RegexCountFile("HERA-100: backlog timeout", "hera.log") == 0) {
		t.Fatal("backlog timeout was not triggered")
	} // */
	/* if (testutil.RegexCountFile("coordinator dispatchrequest: no worker HERA-102: backlog eviction", "hera.log") == 0) {
		t.Fatal("backlog eviction was not triggered")
	} // */
	if (testutil.RegexCountFile("coordinator dispatchrequest: no worker HERA-104: saturation soft sql eviction", "hera.log") == 0) {
		t.Fatal("soft eviction was not triggered")
	}
	if (testutil.RegexCountFile("coordinator dispatchrequest: stranded conn HERA-101: saturation kill", "hera.log") == 0) {
		t.Fatal("eviction was not triggered")
	}
	logger.GetLogger().Log(logger.Debug, "TestSqlEvict stop +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	time.Sleep(2*time.Second)
}

func fastAndSlowBinds() (error) {
	db, err := sql.Open("hera", "127.0.0.1:31002")
	if err != nil {
		fmt.Printf("Error db %s\n", err.Error())
		return err
	}
	db.SetConnMaxLifetime(22 * time.Second)
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(22111)
	defer db.Close()

	// client threads of slow queries
	var stop2 int
	var badCliErr string
	mkClients(1+int(max_conn*1.6), &stop2, 29001111, "badClient", &badCliErr, db)
	time.Sleep(3100 * time.Millisecond)
	/* if (testutil.RegexCountFile("BIND_THROTTLE", "cal.log") == 0) {
		return fmt.Errorf("BIND_THROTTLE was not triggered")
	}
	if (testutil.RegexCountFile("BIND_EVICT", "cal.log") == 0) {
		return fmt.Errorf("BIND_EVICT was not triggered")
	} // */

	// start normal clients after initial backlog timeouts
	var normCliErrStr string
	var stop int
	mkClients(1, &stop, 21001111, "n client", &normCliErrStr, db)
	time.Sleep(1100 * time.Millisecond)

	// if we throttle down or stop, it restores
	stop2 = 1 // stop bad clients
	lib.GetConfig().BindEvictionDecrPerSec = 11500.1
	defer func(){ lib.GetConfig().BindEvictionDecrPerSec = 1.1 }()
	time.Sleep(1 * time.Second)
	conn, err := db.Conn(context.Background())
	if err != nil {
		fmt.Printf("Error conn %s\n", err.Error())
		return err
	}
	defer conn.Close()
	err = sleepyQ(conn,29001111)
	if err != nil {
		msg := fmt.Sprintf("test failed, throttle down didn't restore")
		fmt.Printf("%s", msg)
		return fmt.Errorf("%s", msg)
	}

	stop = 1
	if len(normCliErrStr) != 0 {
		return NormCliErr()
	}
	return nil
}
var normCliErr error
func NormCliErr() (error) {
	if normCliErr == nil {
		normCliErr = fmt.Errorf("normal client got error")
	}
	return normCliErr
}


func partialBadLoad(fracBad float64) (error) {
	db, err := sql.Open("hera", "127.0.0.1:31002")
	if err != nil {
		fmt.Printf("Error db %s\n", err.Error())
		return err
	}
	db.SetConnMaxLifetime(111 * time.Second)
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(22111)
	defer db.Close()


	// client threads of slow queries
	var stop2 int
	var stop3 int
	var badCliErr string
	var cliErr string
	numBad  := int(max_conn*fracBad)
	numNorm := int(max_conn*2.1)+1 - numBad
	fmt.Printf("spawning clients bad%d norm%d\n", numBad, numNorm)
	mkClients(numBad,  &stop2, 29001111, "badClient", &badCliErr, db)
	mkClients(numNorm, &stop3,      100, "normClient", &cliErr, db)   // bind value is short, so bindevict won't trigger
	time.Sleep(3100 * time.Millisecond)
	//time.Sleep(33100 * time.Millisecond)

	// start normal clients after initial backlog timeouts
	var stop int
	var normCliErrStr string
	mkClients(1, &stop, 21001111, "n client", &normCliErrStr, db)
	time.Sleep(1100 * time.Millisecond)

	// if we throttle down or stop, it restores
	stop2 = 1 // stop bad clients
	stop3 = 1
	lib.GetConfig().BindEvictionDecrPerSec = 11333.1
	defer func(){lib.GetConfig().BindEvictionDecrPerSec = 1.1 }()
	time.Sleep(1 * time.Second)
	conn, err := db.Conn(context.Background())
	if err != nil {
		fmt.Printf("Error conn %s\n", err.Error())
		return err
	}
	defer conn.Close()
	err = sleepyQ(conn,29001111)
	if err != nil {
		msg := fmt.Sprintf("test failed, throttle down didn't restore")
		fmt.Printf("%s", msg)
		return fmt.Errorf("%s", msg)
	}

	stop = 1
	// tolerate soft eviction on normal client when we did not use bind eviction
	if len(normCliErrStr) != 0 {
		return NormCliErr()
	} // */
	return nil
}

func mkClients(num int, stop *int, bindV int, grpName string, outErr *string, db *sql.DB) {
	for i := 0 ; i < num ; i++ {
		go func (clientId int) {
			count := 0
			var conn *sql.Conn
			var err error
			var curErr string
			for *stop == 0 {
				nowStr := time.Now().Format("15:04:05.000000 ")
				if conn == nil {
					conn, err = db.Conn(context.Background())
					fmt.Printf(grpName+" connected %d\n", clientId)
					if err != nil {
						fmt.Printf(nowStr + grpName+" Error %d conn %s\n", clientId, err.Error())
						time.Sleep(7 * time.Millisecond)
						continue
					}
				}

				fmt.Printf(nowStr + grpName+"%d loop%d %s\n", clientId, count, time.Now().Format("20060102j150405.000000"))
				err := sleepyQ(conn,bindV)
				if err != nil {
					if err.Error() == curErr {
						fmt.Printf(nowStr +grpName+"%d same err twice\n", clientId)
						conn.Close()
						conn = nil
					} else {
						curErr = err.Error()
						*outErr = curErr
						fmt.Printf(nowStr + grpName +"%d err %s\n", clientId, curErr)
					}
				}
				count++
				time.Sleep(10 * time.Millisecond)
			}
			fmt.Printf(time.Now().Format("15:04:05.000000 ") + grpName+"%d END loop%d\n", clientId, count)
		}(i)
	}
}


func TestBindEvict(t *testing.T) {
	// we would like to clear hera.log, but even if we try, lots of messages still go there
	logger.GetLogger().Log(logger.Debug, "TestBindEvict +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	err := fastAndSlowBinds()
	if err != nil {
		t.Fatalf("main step function returned err %s", err.Error())
	}
	if (testutil.RegexCountFile("BIND_THROTTLE", "cal.log") == 0) {
		t.Fatalf("BIND_THROTTLE was not triggered")
	}
	if (testutil.RegexCountFile("BIND_EVICT", "cal.log") == 0) {
		t.Fatalf("BIND_EVICT was not triggered")
	}
	logger.GetLogger().Log(logger.Debug, "TestBindEvict stop +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}

func TestBindLess(t *testing.T) {
	// we would like to clear hera.log, but even if we try, lots of messages still go there
	logger.GetLogger().Log(logger.Debug, "TestBindLess +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	testutil.BackupAndClear("cal", "BindLess start")
	testutil.BackupAndClear("hera", "BindLess start")
	err := partialBadLoad(0.10)
	if err != nil && err != NormCliErr() {
		t.Fatalf("main step function returned err %s", err.Error())
	}
	if (testutil.RegexCountFile("BIND_THROTTLE", "cal.log") > 0) {
		t.Fatalf("BIND_THROTTLE should not trigger")
	}
	if (testutil.RegexCountFile("BIND_EVICT", "cal.log") > 0) {
		t.Fatalf("BIND_EVICT should not trigger")
	}
	if (testutil.RegexCountFile("HERA-10", "hera.log") == 0) {
		t.Fatal("backlog timeout or saturation was not triggered")
	} // */

	if true {
	logger.GetLogger().Log(logger.Debug, "TestBindLess midpt +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	err := partialBadLoad(0.3)
	if err != nil {
		// t.Fatalf("main step function returned err %s", err.Error()) // can be triggered since test only has one sql
	}
	if (testutil.RegexCountFile("BIND_THROTTLE", "cal.log") == 0) {
		t.Fatalf("BIND_THROTTLE should trigger")
	}
	if (testutil.RegexCountFile("BIND_EVICT", "cal.log") == 0) {
		t.Fatalf("BIND_EVICT should trigger")
	}
	} // endif
	logger.GetLogger().Log(logger.Debug, "TestBindLess done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
} // */
