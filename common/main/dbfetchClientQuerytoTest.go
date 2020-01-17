package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
  "context"
  "os"
  "strconv"
  common "github.com/paypal/hera/common"
  "io"
)

type Result struct {
	id int
	query string
  query_hash string
}

const batch_test = 100
const file_name = "TableParseOutput.csv"

func castOutput(input []string) (string, int ){
  var output string
  for _, each := range input {
    output = output + each + "|"
  }
  return output, len(input)
}

func initFile() *os.File{
  fo, err := os.Create(file_name)
  if err != nil {
      panic(err)
  }
  return fo
}

func main() {
  var start int
  if len(os.Args) != 2 {
		panic("Wrong arguments")
	}
  start, _ = strconv.Atoi(os.Args[1])
	db, err := sql.Open("mysql", "user_wossl:SSLOptional@123@tcp(10.176.12.241:3306)/dalusage")
	if err != nil {
		panic(err.Error())  // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

  ctx := context.Background()

	// Prepare statement for reading data
  var query string
  query = "select id, query, query_hash from sql_usage where id >= " + strconv.Itoa(start) + " and id < " + strconv.Itoa((start + batch_test));
	stmtOut, err := db.Prepare(query)
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	defer stmtOut.Close()

  var rec Result

  rows, err := stmtOut.QueryContext(ctx)
	if err != nil {
		fmt.Println("query err", err.Error())
		return
	}
	defer rows.Close()

  file := initFile()
  defer func() {
    if err := file.Close(); err != nil {
        panic(err)
    }
  }()
  var dump string
  dump = "ID, Table Counts, Tables, Query, QueryHash\n"
  if _, err := io.WriteString(file, dump); err != nil {
    panic(err)
  }
  for rows.Next() {
		err = rows.Scan(&rec.id, &rec.query, &rec.query_hash)
		if err != nil {
			fmt.Println("fetch err", err.Error())
			return
		}
    tables, count := castOutput(common.TableNameParser(rec.query))
    dump = fmt.Sprintf("%d, %d, %s, %s, %s\n", rec.id, count, tables, rec.query, rec.query_hash)
    if _, err := io.WriteString(file, dump); err != nil {
      panic(err)
    }
	}
}
