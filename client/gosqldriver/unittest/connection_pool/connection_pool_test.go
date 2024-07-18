package gosqldriver

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

/*
To run the test
export DB_USER=x
export DB_PASSWORD=x
export DB_DATASOURCE=x
export username=realU
export password=realU-pwd
export TWO_TASK='tcp(mysql.example.com:3306)/someSchema?timeout=60s&tls=preferred||tcp(failover.example.com:3306)/someSchema'
export TWO_TASK_READ='tcp(mysqlr.example.com:3306)/someSchema?timeout=6s&tls=preferred||tcp(failover.example.com:3306)/someSchema'
$GOROOT/bin/go install  .../worker/{mysql,oracle}worker
ln -s $GOPATH/bin/{mysql,oracle}worker .
$GOROOT/bin/go test -c .../tests/unittest/coordinator_basic && ./coordinator_basic.test
*/

var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["child.executable"] = "mysqlworker"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"

	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_hera_test"
	}
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		// mysql
		testutil.RunDML("create table jdbc_hera_test ( ID BIGINT, INT_VAL BIGINT, STR_VAL VARCHAR(500))")
	}
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func TestConnectionPoolManagement(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestConnectionPoolManagement begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	defer db.Close()

	maxOpenConnections := 5
	maxIdleConnections := 2

	db.SetMaxOpenConns(maxOpenConnections)
	db.SetMaxIdleConns(maxIdleConnections)

	connections := make([]*sql.Conn, maxOpenConnections)
	for i := 0; i < maxOpenConnections; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("Err setting up max open connections: %v", err)
		}
		connections[i] = conn
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

	/*
		At this current juncture, hera.log should reflect a total of 6 open connections (with ConnState value of 1 == idling)
		More specifically, there should be 6 statelog updateconnectionstate statements that reflect 6 connections with a newState value of 1.
	*/

	defer cancel()
	_, err = db.Conn(ctx)
	if err == nil {
		t.Fatalf("Expected an error when opening more than maxNumConnections")
	}

	stats := db.Stats()
	if stats.WaitCount != 1 || stats.WaitDuration < 1*time.Second {
		t.Fatalf("Expected to have connection waiting with duration of 1 second before timeout")
	}

	logger.GetLogger().Log(logger.Info, "===== Closing 6th connection after 1 second timeout to stay at 5 maximum open connections =====\n")

	/*
		Since the max number of open connections was previously set to 5, the 6th connection that was opened would be closed,
		hence in hera.log, there should be a statement of statelog updateconnectionstate 0 0 0 1 4 to
		indicate that an idling connection is now closed (1 == open/idle status, 4 == closed status)
	*/

	for i := 0; i < 3; i++ {
		connections[i].Close()
	}

	stats = db.Stats()
	if stats.MaxIdleClosed != 1 {
		t.Fatalf("Expected to have 1 idle connection closed due to set limit of maximum 2 idle connections")
	}

	/*
		After closing 3 of the 5 open connections, since there is a limit of 2 maximum idle connections, 1 of the 3 new idle connections will be closed,
		hence in hera.log, there should be a statement of statelog updateconnectionstate 0 0 0 1 4 to
		indicate that an idling connection is now closed (1 == open/idle status, 4 == closed status)
	*/

	time.Sleep(100 * time.Millisecond)

	conn := connections[3]

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, _ := conn.BeginTx(ctx, nil)
	sqlTxt := "/cmd/delete from " + tableName
	stmt, _ := tx.PrepareContext(ctx, sqlTxt)
	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("Error preparing test (delete table) %s with %s ==== sql\n", err.Error(), sqlTxt)
	}
	stmt, _ = tx.PrepareContext(ctx, "/cmd/insert into "+tableName+" (id, int_val, str_val) VALUES(?, ?, ?)")
	_, err = stmt.Exec(1, time.Now().Unix(), "val 1")
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}

	stmt, _ = conn.PrepareContext(ctx, "/cmd/Select id, int_val from "+tableName+" where id=?")
	rows, _ := stmt.Query(1)
	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}

	rows.Close()
	stmt.Close()

	cancel()
	conn.Close()
	stats = db.Stats()
	if stats.OpenConnections != 3 || stats.MaxIdleClosed != 2 {
		t.Fatalf("Expected to have only 3 open connections and 2 closed connections that were previously idle")
	}

	logger.GetLogger().Log(logger.Debug, "TestConnectionPoolManagement done  -------------------------------------------------------------")
}

func TestConnectionReuse(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestConnectionReuse begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
	}
	defer db.Close()

	maxOpenConnections := 5
	maxIdleConnections := 2
	db.SetMaxOpenConns(maxOpenConnections)
	db.SetMaxIdleConns(maxIdleConnections)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Error opening connection: %v", err)
	}
	conn.Close()

	stats := db.Stats()
	if stats.OpenConnections != 1 || stats.Idle != 1 {
		t.Fatalf("Expected 1 open and idling connection, but got %v open connections and %v idle connections", stats.OpenConnections, stats.Idle)
	}

	conn2, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Error opening connection: %v", err)
	}

	if stats.OpenConnections > 1 {
		t.Fatalf("Expected only 1 connection due to reuse of idle connection, but got %v", stats.OpenConnections)
	}
	/*
		hera.log should only contain 1 statelog updateconnectionstate 0 0 0 4 1 statement to indicate that only 1 connection was established
	*/
	conn2.Close()
	logger.GetLogger().Log(logger.Debug, "TestConnectionReuse done  -------------------------------------------------------------")
}

func TestConcurrentConnectionOpening(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestConcurrentConnectionOpening begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
	}
	defer db.Close()

	maxOpenConnections := 5
	db.SetMaxOpenConns(maxOpenConnections)
	db.SetMaxIdleConns(maxOpenConnections)

	var wg sync.WaitGroup
	errChan := make(chan error, 1000)

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			conn, err := db.Conn(ctx)
			if err != nil {
				errChan <- err
				return
			}

			// Simulate some work with the connection
			randomSleep := time.Duration(rand.Intn(200)) * time.Millisecond
			time.Sleep(randomSleep)
			conn.Close()
		}()
	}

	wg.Wait()
	close(errChan)

	/*
		Since there is a set limit of 5 open connections, attempting to create 1000 connections concurrently will result in many of these attempted
		connections to be closed. As such, in the hera.log file, there will only be 5 successful connections, denoted by
		5 statements of statelog updateconnectionstate 0 0 0 4 1. (status 1 == open/idle connection)

		In the event that the randomSleep time < the timeout of 100ms for a connection to be established, the existing connection will be reused, hence there will
		only be a maximum of 5 open connections at any point
	*/

	var timeoutErrors int
	for err := range errChan {
		if err != nil && err == context.DeadlineExceeded {
			timeoutErrors++
		} else if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	stats := db.Stats()

	minTimedOutConnections := 500
	maxTimedOutConnections := 1000 - maxOpenConnections

	if timeoutErrors < minTimedOutConnections || timeoutErrors > maxTimedOutConnections {
		t.Fatalf("Expected timeout errors between %d and %d, but got %d", minTimedOutConnections, maxTimedOutConnections, timeoutErrors)
	}

	/*
		Since the simulated work duration is randomized, the number of connections that timeout is non-deterministic,
		therefore a range of expected timed out connections is used when (min = 500, max = 1000 - maxOpenConnections = 995)
		making the assertions for this unit test.
	*/

	if stats.OpenConnections > maxOpenConnections {
		t.Fatalf("Expected max %d open connections, but got %d", maxOpenConnections, stats.OpenConnections)
	}
	logger.GetLogger().Log(logger.Debug, "TestConcurrentConnectionOpening done  -------------------------------------------------------------")
}
