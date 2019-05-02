// Test application for Hera using the sql driver for Golang's database/sql.
//
// It is driven by a configuration input file
// in JSON format (see in.txt as example), which instruct what SQLs to run, and how many times
package main

import (
	"flag"

	"github.com/paypal/hera/client/gosqldriver"
	_ "github.com/paypal/hera/client/gosqldriver/tcp" /*to register the driver*/
	"github.com/paypal/hera/utility/logger"

	"context"
	"database/sql"
	"fmt"

	"encoding/json"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"time"
	/* to cast to the Hera extension */)

var url = flag.String("url", "", "Heras URL, default to the Url from the input file")
var input = flag.String("input", "in.txt", "the input file to read the batch commands, default in.txt")

type Suite struct {
	LogLevel     int32
	Url          string
	MaxIdleConns int
	Tests        []Test
}

type Test struct {
	Name    string
	Setup   []Query
	Repeat  int
	Queries []Query
	Cleanup []Query
}

type Query struct {
	/*
		0 - select
		1 - DML
		2 - begin txn
	*/
	Type          int32
	Sql           string
	BindInNames   []string
	BindInValues  []string
	BindOutNames  []string
	BindOutValues []string
	ShardKey      string

	ExpectedError string

	ExpectedBindOutValues []string

	ExpectedNumRows int32
	ExpectedNumCols int32
	ExpectedResult  [][]string
	Print           bool
}

func main() {
	flag.Parse()
	if *url == "" {
		flag.PrintDefaults()
		return
	}
	file, err := os.Open(*input)
	if err != nil {
		fmt.Println(err)
		return
	}
	in, _ := ioutil.ReadAll(file)
	var suite Suite
	err = json.Unmarshal(in, &suite)
	if err != nil {
		fmt.Println(err)
		return
	}

	logger.SetLogVerbosity(suite.LogLevel)

	logger.GetLogger().Log(logger.Info, "Batch:", suite)
	logger.GetLogger().Log(logger.Info, "********************************")
	logger.GetLogger().Log(logger.Info, "Opening ", *url)

	db, err := sql.Open("hera", *url)
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	db.SetMaxIdleConns(suite.MaxIdleConns)
	defer db.Close()

	var wg sync.WaitGroup
	wg.Add(len(suite.Tests))
	for _, test := range suite.Tests {
		go func(t *Test) {
			run(db, t)
			wg.Done()
		}(&test)
	}
	wg.Wait()
}

func run(db *sql.DB, test *Test) error {
	logger.GetLogger().Log(logger.Info, "Begin test ********", test.Name)
	ctx := context.Background()
	c, err := db.Conn(ctx)
	if err != nil {
		logger.GetLogger().Log(logger.Warning, "Error connecting: ", err)
		return err
	}
	txn, _ := c.BeginTx(ctx, nil)
	for _, q := range test.Setup {
		runQ(ctx, db, &c, &txn, &q)
	}
	for i := 0; i <= test.Repeat; i++ {
		for _, q := range test.Queries {
			runQ(ctx, db, &c, &txn, &q)
		}
	}
	for _, q := range test.Cleanup {
		runQ(ctx, db, &c, &txn, &q)
	}
	txn.Rollback()
	c.Close()
	logger.GetLogger().Log(logger.Info, "Done test --------", test.Name)
	return nil
}

func runQ(ctx context.Context, db *sql.DB, pc **sql.Conn, ptxn **sql.Tx, q *Query) error {
	switch q.Type {
	case 0: //select
		return runSelect(ctx, *ptxn, q, *pc)
	case 1:
		return runDML(ctx, *ptxn, q, *pc)
	case 2:
		(*ptxn).Commit()
		*ptxn, _ = (*pc).BeginTx(ctx, nil)
	case 3:
		(*ptxn).Rollback()
		*ptxn, _ = (*pc).BeginTx(ctx, nil)
	case 4:
		(*pc).Close()
		*pc = nil
		var err error
		*pc, err = db.Conn(ctx)
		if err != nil {
			logger.GetLogger().Log(logger.Warning, "Error re-connecting: ", err)
			return err
		}
	case 5:
		ms, _ := strconv.Atoi(q.Sql)
		time.Sleep(time.Millisecond * time.Duration(ms))
	}
	return nil
}

// TODO: implement the expectations
func runSelect(ctx context.Context, txn *sql.Tx, q *Query, c *sql.Conn) error {
	if q.ShardKey != "" {
		hera := gosqldriver.InnerConn(c)
		hera.SetShardKeyPayload(q.ShardKey)
		defer hera.ResetShardKeyPayload()
	}
	stmt1, err := txn.PrepareContext(ctx, q.Sql)
	var bindIns []interface{}
	for i := range q.BindInNames {
		bindIns = append(bindIns, sql.Named(q.BindInNames[i], q.BindInValues[i]))
	}

	rows, err := stmt1.QueryContext(ctx, bindIns...)
	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "Err: ", err)
		}
		return err
	}

	if rows != nil {
		rowNum := 0
		names, err := rows.Columns()
		if err != nil {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "Err getting columns:", err)
			}
		}
		if q.Print {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "Column names (", len(names), "):", names)
			}
		}
		for {
			if rows.Next() {
				rowNum++
				if q.Print {
					vals := make([]interface{}, len(names))
					for i := range vals {
						var ii interface{}
						vals[i] = &ii
					}
					rows.Scan(vals...)
					rowMsg := fmt.Sprintf("Row(%d): = (", rowNum)
					for i := range vals {
						var rawVal = *(vals[i].(*interface{}))
						if i != 0 {
							rowMsg = rowMsg + ", "
						}
						rowMsg = rowMsg + string(rawVal.([]byte))
					}
					rowMsg = rowMsg + ")"
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, rowMsg)
					}
				}
			} else {
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, "Rows =", rowNum)
				}
				break
			}
		}
		rows.Close()
	}

	stmt1.Close()
	return nil
}

func runDML(ctx context.Context, txn *sql.Tx, q *Query, c *sql.Conn) error {
	if q.ShardKey != "" {
		hera := gosqldriver.InnerConn(c)
		hera.SetShardKeyPayload(q.ShardKey)
		defer hera.ResetShardKeyPayload()
	}
	stmt1, err := txn.PrepareContext(ctx, q.Sql)
	var bindIns []interface{}
	for i := range q.BindInNames {
		bindIns = append(bindIns, sql.Named(q.BindInNames[i], q.BindInValues[i]))
	}

	res, err := stmt1.ExecContext(ctx, bindIns...)
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Err: ", err)
		}
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Err rows: ", err)
		}
		return err
	}

	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Rows affected:", rows)
	}

	return nil
}
