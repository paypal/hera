package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

func main() {
	db, err := sql.Open("mysql", "testuser:testuserpw@tcp(127.0.0.1:3306)/testschema?multiStatements=true")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()
	defer db.Exec(`drop table table1`)
	defer db.Exec(`drop table table2`)

	_, err = db.Exec(`create table table1(x varchar(10));create table table2(x varchar(10))`)
	if err != nil {
		panic(err.Error())
	}
	_, err = db.Exec(`insert into table1 values('Hi');insert into table1 values('Hello')`)
	if err != nil {
		panic(err.Error())
	}
	var got int
	err = db.QueryRow("select count(*) from table1").Scan(&got)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("Rows affected :", got)
}
