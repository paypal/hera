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
// Disconnects the app-hera connection without cleanup to test mux behavior
//
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/paypal/hera/client/gosqldriver/tcp" // to register sql driver

	"github.com/paypal/hera/utility/logger"
)

func mkConn(db *sql.DB) (*sql.Conn, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 7*24*3600*time.Second)
	conn, err := db.Conn(ctx)
	if err != nil {
		fmt.Printf("Error getting connection %s\n", err.Error())
		os.Exit(2)
	}
	return conn, cancel
}

func main() {
	logger.GetLogger().Log(logger.Debug, "rude.go +++++++++++++ begin")
	hostname, _ := os.Hostname()
	tdb, err := sql.Open("hera", hostname+":24317")
	if err != nil {
		fmt.Println("Error db conn", err)
		os.Exit(1)
		return
	}
	tdb.SetMaxIdleConns(0)
	tmpConn, _ := mkConn(tdb)
	logger.GetLogger().Log(logger.Debug, "rude.go +++++++++++++ preExec")
	execSQL(tmpConn, "insert into resilience_at_load(id,note)values(2000,'tmpConn')", true /*skipCommit*/)
	logger.GetLogger().Log(logger.Debug, "rude.go +++++++++++++ done")

	// need to do unclean exit, avoid Rollback()
	os.Exit(1)
}

func execSQL(conn *sql.Conn, sql string, skipCommit bool) *sql.Tx {
	ctx, _ := context.WithTimeout(context.Background(), 7*24*3600*time.Second)
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		fmt.Printf("Error startT %s %s\n", sql, err.Error())
		os.Exit(3)
	}
	stmt, err := tx.PrepareContext(ctx, sql)
	if err != nil {
		fmt.Printf("Error prep %s %s\n", sql, err.Error())
		os.Exit(3)
	}
	_, err = stmt.Exec()
	if err != nil {
		fmt.Printf("Error exec %s %s\n", sql, err.Error())
		os.Exit(3)
	}
	if skipCommit {
		return tx
	}
	err = tx.Commit()
	if err != nil {
		fmt.Printf("Error commit %s %s\n", sql, err.Error())
		os.Exit(3)
	}
	return nil
}
