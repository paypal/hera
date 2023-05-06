// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/tests/functionaltest/testutil"
)

type TB testing.B

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["lifo_scheduler_enabled"] = "false"
	appcfg["child.executable"] = "mysqlworker"
	appcfg["database_type"] = "mysql"
	appcfg["debug_mux"] = "true"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "8"
	opscfg["opscfg.default.server.log_level"] = "5"
	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}

	return appcfg, opscfg, testutil.MySQLWorker
}

func (tb *TB) check(err error) {
	if err != nil {
		tb.Fatal(err)
	}
}

func (tb *TB) checkDB(db *sql.DB, err error) *sql.DB {
	tb.check(err)
	return db
}

func (tb *TB) checkRows(rows *sql.Rows, err error) *sql.Rows {
	tb.check(err)
	return rows
}

func (tb *TB) checkStmt(stmt *sql.Stmt, err error) *sql.Stmt {
	tb.check(err)
	return stmt
}

func setupDb() error {
	testutil.RunDML("DROP TABLE IF EXISTS test_simple_table_1")
	if os.Getenv("WORKER") == "postgres" {
		return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID NUMERIC PRIMARY KEY, NAME VARCHAR(128), STATUS NUMERIC, PYPL_TIME_TOUCHED NUMERIC)")
	}
	return testutil.RunDML("CREATE TABLE test_simple_table_1 (ID INT PRIMARY KEY, NAME VARCHAR(128), STATUS INT, PYPL_TIME_TOUCHED INT)")
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, setupDb))
}

func initDB(b *testing.B, queries ...string) (*sql.DB, error) {
	hostname := testutil.GetHostname()
	db, err := sql.Open("hera", hostname+":31002")
	if err != nil {
		fmt.Println("Error connecting to OCC:", err)
		return nil, err
	}
	db.SetMaxIdleConns(0)
	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			b.Fatalf("error on %q: %v", query, err)
		}
	}
	return db, nil
}

const concurrencyLevel = 10

func BenchmarkQuery(b *testing.B) {
	tb := (*TB)(b)
	b.StopTimer()
	b.ReportAllocs()
	db, err2 := initDB(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	if err2 != nil {
		b.Fatalf("error in initialize DB, error: %v", err2)
	}
	db.SetMaxIdleConns(concurrencyLevel)
	defer db.Close()

	stmt := tb.checkStmt(db.Prepare("SELECT val FROM foo WHERE id=?"))
	defer stmt.Close()

	remain := int64(b.N)
	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)
	defer wg.Wait()
	b.StartTimer()

	for i := 0; i < concurrencyLevel; i++ {
		go func() {
			for {
				if atomic.AddInt64(&remain, -1) < 0 {
					wg.Done()
					return
				}

				var got string
				tb.check(stmt.QueryRow(1).Scan(&got))
				if got != "one" {
					b.Errorf("query = %q; want one", got)
					wg.Done()
					return
				}
			}
		}()
	}
}

func BenchmarkExec(b *testing.B) {
	tb := (*TB)(b)
	b.StopTimer()
	b.ReportAllocs()
	hostname := testutil.GetHostname()
	db, err2 := sql.Open("hera", hostname+":31002")
	db = tb.checkDB(db, err2)

	db.SetMaxIdleConns(concurrencyLevel)
	defer db.Close()

	stmt := tb.checkStmt(db.Prepare("DO 1"))
	defer stmt.Close()

	remain := int64(b.N)
	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)
	defer wg.Wait()
	b.StartTimer()

	for i := 0; i < concurrencyLevel; i++ {
		go func() {
			for {
				if atomic.AddInt64(&remain, -1) < 0 {
					wg.Done()
					return
				}

				if _, err := stmt.Exec(); err != nil {
					b.Logf("stmt.Exec failed: %v", err)
					b.Fail()
				}
			}
		}()
	}
}

// data, but no db writes
var roundtripSample []byte

func initRoundtripBenchmarks() ([]byte, int, int) {
	if roundtripSample == nil {
		roundtripSample = []byte(strings.Repeat("0123456789abcdef", 1024*1024))
	}
	return roundtripSample, 16, len(roundtripSample)
}

func BenchmarkRoundtripTxt(b *testing.B) {
	b.StopTimer()
	sample, min, max := initRoundtripBenchmarks()
	sampleString := string(sample)
	b.ReportAllocs()
	tb := (*TB)(b)
	hostname := testutil.GetHostname()
	db, err2 := sql.Open("hera", hostname+":31002")
	if err2 != nil {
		b.Fatalf("error in initialize DB, error: %v", err2)
	}
	defer db.Close()
	b.StartTimer()
	var result string
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := sampleString[0:length]
		rows := tb.checkRows(db.Query(`SELECT "` + test + `"`))
		if !rows.Next() {
			rows.Close()
			b.Fatalf("crashed")
		}
		err := rows.Scan(&result)
		if err != nil {
			rows.Close()
			b.Fatalf("crashed")
		}
		if result != test {
			rows.Close()
			b.Errorf("mismatch")
		}
		rows.Close()
	}
}

func BenchmarkRoundtripBin(b *testing.B) {
	b.StopTimer()
	sample, min, max := initRoundtripBenchmarks()
	b.ReportAllocs()
	tb := (*TB)(b)
	hostname := testutil.GetHostname()
	db, err2 := sql.Open("hera", hostname+":31002")
	if err2 != nil {
		b.Fatalf("error in initialize DB, error: %v", err2)
	}
	defer db.Close()
	stmt := tb.checkStmt(db.Prepare("SELECT ?"))
	defer stmt.Close()
	b.StartTimer()
	var result sql.RawBytes
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := sample[0:length]
		rows := tb.checkRows(stmt.Query(test))
		if !rows.Next() {
			rows.Close()
			b.Fatalf("crashed")
		}
		err := rows.Scan(&result)
		if err != nil {
			rows.Close()
			b.Fatalf("crashed")
		}
		if !bytes.Equal(result, test) {
			rows.Close()
			b.Errorf("mismatch")
		}
		rows.Close()
	}
}

func benchmarkQueryContext(b *testing.B, db *sql.DB, p int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.SetMaxIdleConns(p * runtime.GOMAXPROCS(0))

	tb := (*TB)(b)
	stmt := tb.checkStmt(db.PrepareContext(ctx, "SELECT val FROM foo WHERE id=?"))
	defer stmt.Close()

	b.SetParallelism(p)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var got string
		for pb.Next() {
			tb.check(stmt.QueryRow(1).Scan(&got))
			if got != "one" {
				b.Fatalf("query = %q; want one", got)
			}
		}
	})
}

func BenchmarkQueryContext(b *testing.B) {
	db, err := initDB(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	if err != nil {
		b.Fatalf("error in initialize DB, error: %v", err)
	}
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkQueryContext(b, db, p)
		})
	}
}

func benchmarkExecContext(b *testing.B, db *sql.DB, p int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.SetMaxIdleConns(p * runtime.GOMAXPROCS(0))

	tb := (*TB)(b)
	stmt := tb.checkStmt(db.PrepareContext(ctx, "DO 1"))
	defer stmt.Close()

	b.SetParallelism(p)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := stmt.ExecContext(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkExecContext(b *testing.B) {
	db, err := initDB(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	if err != nil {
		b.Fatalf("error in initialize DB, error: %v", err)
	}
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkExecContext(b, db, p)
		})
	}
}

// BenchmarkQueryRawBytes benchmarks fetching 100 blobs using sql.RawBytes.
// "size=" means size of each blobs.
func BenchmarkQueryRawBytes(b *testing.B) {
	var sizes []int = []int{100, 1000, 2000, 4000, 8000, 12000, 16000, 32000, 64000, 256000}
	db, err := initDB(b,
		"DROP TABLE IF EXISTS bench_rawbytes",
		"CREATE TABLE bench_rawbytes (id INT PRIMARY KEY, val LONGBLOB)",
	)
	if err != nil {
		b.Fatalf("error in initialize DB, error: %v", err)
	}
	defer db.Close()
	blob := make([]byte, sizes[len(sizes)-1])
	for i := range blob {
		blob[i] = 42
	}
	for i := 0; i < 100; i++ {
		_, err := db.Exec("INSERT INTO bench_rawbytes VALUES (?, ?)", i, blob)
		if err != nil {
			b.Fatal(err)
		}
	}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%v", s), func(b *testing.B) {
			db.SetMaxIdleConns(0)
			db.SetMaxIdleConns(1)
			b.ReportAllocs()
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				rows, err := db.Query("SELECT LEFT(val, ?) as v FROM bench_rawbytes", s)
				if err != nil {
					b.Fatal(err)
				}
				nrows := 0
				for rows.Next() {
					var buf sql.RawBytes
					err := rows.Scan(&buf)
					if err != nil {
						b.Fatal(err)
					}
					if len(buf) != s {
						b.Fatalf("size mismatch: expected %v, got %v", s, len(buf))
					}
					nrows++
				}
				rows.Close()
				if nrows != 100 {
					b.Fatalf("numbers of rows mismatch: expected %v, got %v", 100, nrows)
				}
			}
		})
	}
}
