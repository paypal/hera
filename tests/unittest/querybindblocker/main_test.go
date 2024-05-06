package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {
	fmt.Println("setup() begin")
	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["rac_sql_interval"] = "0"
	appcfg["enable_query_bind_blocker"] = "true"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}
	return appcfg, opscfg, testutil.MySQLWorker
}

func teardown() {
	mx.StopServer()
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, nil))
}

func executeQuery(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	defer conn.Close()
	if err != nil {
		t.Fatalf("conn %s", err.Error())
	}

	tx, _ := conn.BeginTx(ctx, nil)
	stmt, err := tx.PrepareContext(ctx, "/*qbb_test.find*/select id, note from qbb_test where id=? for update")
	defer stmt.Close()
	if err != nil {
		t.Fatalf("Error prep sel %s\n", err.Error())
		tx.Rollback()
	}
	_, err = stmt.Query(11)
	if err != nil {
		t.Fatalf("Failed to execute query with error: %v", err)
		tx.Rollback()
	}
	tx.Commit()
	stmt.Close()
	conn.Close()
}

func TestQueryBindBlocker(t *testing.T) {
	testutil.RunDML("DROP TABLE IF EXISTS hera_rate_limiter")
	testutil.RunDML("create table hera_rate_limiter (herasqlhash numeric not null, herasqltext varchar(4000) not null, bindvarname varchar(200) not null, bindvarvalue varchar(200) not null, blockperc numeric not null, heramodule varchar(100) not null, end_time numeric not null, remarks varchar(200) not null)")
	testutil.RunDML("create table qbb_test (id numeric, note varchar(111))")

	logger.GetLogger().Log(logger.Debug, "TestQueryBindBlocker begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx := context.Background()
	// cleanup and insert one row in the table
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	if true {
		tx0, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("tx0 %s", err.Error())
		}
		stmtD, err := tx0.PrepareContext(ctx, "delete from hera_rate_limiter")
		if err != nil {
			t.Fatalf("stmtD %s", err.Error())
		}
		_, err = stmtD.Exec()
		if err != nil {
			t.Fatalf("stmtD exec %s", err.Error())
		}
		err = tx0.Commit()
		if err != nil {
			t.Fatalf("commit0 %s", err.Error())
		}

		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("tx %s", err.Error())
		}
		stmt, err := tx.PrepareContext(ctx, "/*setup qbb t*/delete from qbb_test")
		if err != nil {
			t.Fatalf("prep %s", err.Error())
		}
		_, err = stmt.Exec()
		if err != nil {
			t.Fatalf("Error preparing test (delete table) %s\n", err.Error())
		}
		stmt, err = tx.PrepareContext(ctx, "/*setup qbb t*/insert into qbb_test(id, note) VALUES(?, ?)")
		if err != nil {
			t.Fatalf("prep ins %s", err.Error())
		}
		_, err = stmt.Exec(11, "eleven")
		if err != nil {
			t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
		}
		err = tx.Commit()
		if err != nil {
			t.Fatalf("Error commit %s\n", err.Error())
		}
	}

	if true {
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("tx findQ %s", err.Error())
		}
		stmt, err := tx.PrepareContext(ctx, "/*qbb_test.find*/select id, note from qbb_test where id=? for update")
		if err != nil {
			t.Fatalf("Error prep sel %s\n", err.Error())
		}
		rows, err := stmt.Query(11)
		if err != nil {
			t.Fatalf("Error query sel %s\n", err.Error())
		}
		if !rows.Next() {
			t.Fatalf("Expected 1 row")
		}
		err = tx.Rollback()
		if err != nil {
			t.Fatalf("rollback error %s", err.Error())
		}
	}
	// above baseline checks
	fmt.Printf("DONE DONE baseline check\n")

	if true {
		tx0, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("tx0 %s", err.Error())
		}
		stmtD, err := tx0.PrepareContext(ctx, "delete from hera_rate_limiter")
		if err != nil {
			t.Fatalf("prep stmtD %s", err.Error())
		}
		_, err = stmtD.Exec()
		if err != nil {
			t.Fatalf("stmtD %s", err.Error())
		}
		stmt, err := tx0.PrepareContext(ctx, "insert into hera_rate_limiter (herasqlhash, herasqltext, bindvarname, bindvarvalue, blockperc, heramodule, end_time, remarks) values ( ?, ?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			t.Fatalf("ins prep %s", err.Error())
		}
		_, err = stmt.Exec(51938198,
			"/*qbb_test.find*/selec",
			"p1",
			"11",
			100,
			"hera-test",
			2000111222,
			"block100")
		if err != nil {
			t.Fatalf("ins exec %s", err.Error())
		}
		err = tx0.Commit()
		if err != nil {
			t.Fatalf("commit tx0 %s", err.Error())
		}

		fmt.Printf("wait wait: loading basic block\n")
		time.Sleep(12 * time.Second)

		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("tx %s", err.Error())
		}
		stmt, err = tx.PrepareContext(ctx, "/*qbb_test.find*/select id, note from qbb_test where id=? for update")
		if err != nil {
			t.Fatalf("Error prep sel %s\n", err.Error())
		}
		_, err = stmt.Query(11)
		if err == nil {
			t.Fatalf("Error query should have been blocked")
		}
		tx.Rollback()
		// can have error because connection could be closed

		conn, err = db.Conn(ctx)
		if err != nil {
			t.Fatalf("conn %s", err.Error())
		}
	}

	if true {
		tx0, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("tx0 %s", err.Error())
		}
		stmtD, err := tx0.PrepareContext(ctx, "delete from hera_rate_limiter")
		if err != nil {
			t.Fatalf("prep err %s", err.Error())
		}
		_, err = stmtD.Exec()
		if err != nil {
			t.Fatalf("stmtD %s", err.Error())
		}
		stmt, err := tx0.PrepareContext(ctx, "insert into hera_rate_limiter (herasqlhash, herasqltext, bindvarname, bindvarvalue, blockperc, heramodule, end_time, remarks) values ( ?, ?, ?, ?, ?, ?, 2000111222, ?)")
		if err != nil {
			t.Fatalf("prep ins %s", err.Error())
		}
		_, err = stmt.Exec(51938197, "/*qbb_test.find*/select id, note from qbb_test where id=:p1 for upd", "p1", "11", 100, "hera-test", "WrongHash")
		if err != nil {
			t.Fatalf("exec1 %s", err.Error())
		}
		_, err = stmt.Exec(51938198, "/*bb_test.find*/select id, note from qbb_test where id=:p1 for upd", "p1", "11", 100, "hera-test", "WrongSqlText")
		if err != nil {
			t.Fatalf("exec2 %s", err.Error())
		}
		_, err = stmt.Exec(51938198, "/*bb_test.find*/select id, note from qbb_test where id=:p1 for upd", "notId", "11", 100, "hera-test", "WrongBindName")
		if err != nil {
			t.Fatalf("exec3 %s", err.Error())
		}
		_, err = stmt.Exec(51938198, "/*bb_test.find*/select id, note from qbb_test where id=:p1 for upd", "p1", "333", 100, "hera-test", "WrongBindVal")
		if err != nil {
			t.Fatalf("exec4 %s", err.Error())
		}
		_, err = stmt.Exec(51938198, "/*bb_test.find*/select id, note from qbb_test where id=:p1 for upd", "p1", "11", 100, "nothera-test", "WrongBindModule")
		if err != nil {
			t.Fatalf("exec5 %s", err.Error())
		}
		err = tx0.Commit()
		if err != nil {
			t.Fatalf("tx0 commit %s", err.Error())
		}

		fmt.Printf("wait wait: loading close to block, but ultimately not\n")
		time.Sleep(12 * time.Second)

		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin tx %s", err.Error())
		}
		stmt, err = tx.PrepareContext(ctx, "/*qbb_test.find*/select id, note from qbb_test where id=? for update")
		if err != nil {
			t.Fatalf("Error prep sel %s\n", err.Error())
		}
		_, err = stmt.Query(11)
		if err != nil {
			t.Fatalf("Error query might have been erroneously blocked %s", err.Error())
		}
		err = tx.Rollback()
		if err != nil {
			t.Fatalf("rollback %s", err.Error())
		}
	}

	if true {
		tx0, _ := conn.BeginTx(ctx, nil)
		stmtD, _ := tx0.PrepareContext(ctx, "delete from hera_rate_limiter")
		stmtD.Exec()
		stmt, _ := tx0.PrepareContext(ctx, "insert into hera_rate_limiter (herasqlhash, herasqltext, bindvarname, bindvarvalue, blockperc, heramodule, end_time, remarks) values ( ?, ?, ?, ?, ?, ?, ?, ?)")
		stmt.Exec(51938198,
			"/*qbb_test.find*/selec",
			"p1",
			"11",
			50,
			"hera-test",
			2000111222,
			"block100")
		tx0.Commit()

		fmt.Printf("wait wait: loading blockperc\n")
		time.Sleep(12 * time.Second)

		countBlock := 0
		for i := 0; i < 100; i++ {
			conn, err = db.Conn(ctx)
			if err != nil {
				t.Fatalf("conn %s", err.Error())
			}

			tx, _ := conn.BeginTx(ctx, nil)
			stmt, err := tx.PrepareContext(ctx, "/*qbb_test.find*/select id, note from qbb_test where id=? for update")
			if err != nil {
				t.Fatalf("Error prep sel %s\n", err.Error())
			}
			_, err = stmt.Query(11)
			if err != nil {
				countBlock++
			}
			tx.Rollback()
		}
		if countBlock < 25 || countBlock > 75 {
			t.Fatalf("50 blockperc saw too many/few blocks %d out of 100", countBlock)
		}
	}

	time.Sleep(11 * time.Millisecond)

	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestQueryBindBlocker done  -------------------------------------------------------------")
}

func TestQueryBindBlockerTableNotExistOrEmpty(t *testing.T) {
	testutil.RunDML("DROP TABLE IF EXISTS hera_rate_limiter")
	testutil.RunDML("DROP TABLE IF EXISTS qbb_test")
	testutil.RunDML("create table qbb_test (id numeric, note varchar(111))")

	logger.GetLogger().Log(logger.Debug, "TestQueryBindBlockerTableNotExis begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx := context.Background()
	// cleanup and insert one row in the table
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("tx %s", err.Error())
	}
	stmt, err := tx.PrepareContext(ctx, "/*setup qbb t*/delete from qbb_test")
	if err != nil {
		t.Fatalf("prep %s", err.Error())
	}
	_, err = stmt.Exec()
	if err != nil {
		t.Fatalf("Error preparing test (delete table) %s\n", err.Error())
	}
	stmt, err = tx.PrepareContext(ctx, "/*setup qbb t*/insert into qbb_test(id, note) VALUES(?, ?)")
	if err != nil {
		t.Fatalf("prep ins %s", err.Error())
	}
	_, err = stmt.Exec(11, "eleven")
	if err != nil {
		t.Fatalf("Error preparing test (create row in table) %s\n", err.Error())
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error commit %s\n", err.Error())
	}
	stmt.Close()
	conn.Close()

	time.Sleep(3 * time.Second)

	executeQuery(t, db)
	time.Sleep(3 * time.Second)
	if testutil.RegexCountFile("Error 1146: Table 'heratestdb.hera_rate_limiter' doesn't exist", "hera.log") == 0 {
		t.Fatalf("expected to see table 'hera_rate_limiter' doesn't exist error")
	}

	testutil.RunDML("create table hera_rate_limiter (herasqlhash numeric not null, herasqltext varchar(4000) not null, bindvarname varchar(200) not null, bindvarvalue varchar(200) not null, blockperc numeric not null, heramodule varchar(100) not null, end_time numeric not null, remarks varchar(200) not null)")
	time.Sleep(3 * time.Second)
	executeQuery(t, db)
	time.Sleep(15 * time.Second)
	if testutil.RegexCountFile("Loaded 0 sqlhashes, 0 entries, query bind blocker entries", "hera.log") == 0 {
		t.Fatalf("expected to 0 entries from hera_rate_limiter table")
	}
}
