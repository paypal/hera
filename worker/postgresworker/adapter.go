// Copyright 2019 PayPal Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	// _ "github.com/jackc/pgx/v4"

	_ "github.com/lib/pq"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/logger"
	"github.com/paypal/hera/worker/shared"
)

type psqlAdapter struct {
}

func (adapter *psqlAdapter) MakeSqlParser() (common.SQLParser, error) {
	return common.NewRegexSQLParser()
}

func (adapter *psqlAdapter) InitDB() (*sql.DB, error) {
	// LINES 55-74 USES LIB/PQ POSTGRESQL DRIVER WHICH IS NOT AS WELL MAINTAINED AS JACKC/PGX
	// https://github.com/lib/pq

	// HARD CODED VALUES FOR TESTING
	// LINES 61-65 ARE NOT HARD-CODED AND SHOULD REPLACE THESE
	host := "localhost"
	port := 5432
	user := "postgres"
	pass := "password"
	dbname := "testdb"

	// // Typically use localhost and port anyways
	// host := os.Getenv("host")
	// port := os.Getenv("port")
	// user := os.Getenv("username")
	// pass := os.Getev("password")
	// dbname := os.Getenv("dbname")
	// psqlInfo URL := postgres://username:password@host:port/database_name

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		logger.GetLogger().Log(logger.Info, "DB connection error")
	} else {
		logger.GetLogger().Log(logger.Info, user+" connect success "+host+strconv.Itoa(port)+dbname)
	}

	// createStmt := `CREATE TABLE IF NOT EXISTS sports (team varchar(255), league varchar(255));`
	// _, err = db.Exec(createStmt)
	// if err != nil {
	// 	fmt.Println("create err", err.Error())
	// }

	// waitStmt := `SELECT pg_sleep(2);`
	// _, err = db.Exec(waitStmt)
	// if err != nil {
	// 	fmt.Println("wait err", err.Error())
	// }

	insertStmt := `INSERT INTO sports(team, league) VALUES ('Golden State Warriors', 'NBA');`
	_, err = db.Exec(insertStmt)
	if err != nil {
		fmt.Println("insert err", err.Error())
	}

	// // USING PGX (POSTGRESQL DRIVER) -- REQUIRES GO VERSION 1.15 OR HIGHER
	// // https://github.com/jackc/pgx
	// // UNCOMMENT THIS SECTION AND COMMENT OUT SECTION ABOVE TO USE

	// // MUST RUN THIS DURING MANUAL BUILD FOR CONNECTION TO WORK
	// // export PSQL_URL="postgres://postgres:password@localhost:5432/testdb"
	// // urlExample := "postgres://username:password@localhost:5432/database_name"

	// psqlURL := os.Getenv("PSQL_URL")

	// if psqlURL == "" {
	// 	return nil, errors.New("Can't get 'psql URL' from env")
	// }

	// db, err := pgx.Connect(context.Background(), os.Getenv("PSQL_URL"))

	return db, err
}

// Checking master status
func (adapter *psqlAdapter) Heartbeat(db *sql.DB) bool {
	ctx, _ /*cancel*/ := context.WithTimeout(context.Background(), 10*time.Second)
	writable := false
	conn, err := db.Conn(ctx)
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "could not get connection "+err.Error())
		}
		return writable
	}
	defer conn.Close()

	if strings.HasPrefix(os.Getenv("logger.LOG_PREFIX"), "WORKER ") {
		stmt, err := conn.PrepareContext(ctx, "select * from sports")
		//stmt, err := conn.PrepareContext(ctx, "show variables where variable_name='read_only'")
		if err != nil {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "query ro check err ", err.Error())
			}
			return false
		}
		defer stmt.Close()

		rows, err := stmt.Query()
		if err != nil {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "ro check err ", err.Error())
			}
			return false
		}
		defer rows.Close()
		countRows := 0
		if rows.Next() {
			countRows++
			var readOnly int
			/*var nom string
			rows.Scan(&nom, &readOnly) // */
			rows.Scan(&readOnly)
			if readOnly == 0 {
				writable = true
			}
		}

		// read only connection
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "writable:", writable)
		}
	}
	return writable
}

// UseBindNames return false because the SQL string uses ? for bind parameters
func (adapter *psqlAdapter) UseBindNames() bool {
	return false
}

/**
 * @TODO infra.hera.jdbc.HeraResultSetMetaData mysql type to java type map.
 */
var colTypeMap = map[string]int{
	"NULL":      0,
	"CHAR":      1,
	"DECIMAL":   2,
	"INT":       3,
	"FLOAT":     4,
	"BIGINT":    8,
	"DOUBLE":    22,
	"BINARY":    23,
	"VARCHAR":   5,
	"BLOB":      113,
	"CLOB":      112,
	"TEXT":      112,
	"DATE":      184,
	"TIMESTAMP": 185,
}

func (adapter *psqlAdapter) GetColTypeMap() map[string]int {
	return colTypeMap
}

func (adapter *psqlAdapter) ProcessError(errToProcess error, workerScope *shared.WorkerScopeType, queryScope *shared.QueryScopeType) {
	errStr := errToProcess.Error()

	if strings.HasPrefix(errStr, "driver: bad connection") {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "mysql ProcessError badConnRecycle "+errStr+" sqlHash:"+(*queryScope).SqlHash+" Cmd:"+(*queryScope).NsCmd)
		}
		(*workerScope).Child_shutdown_flag = true
		return
	}

	idx := strings.Index(errStr, ":")
	if idx < 0 || idx >= len(errStr) {
		return
	}
	var errno int
	fmt.Sscanf(errStr[6:idx], "%d", &errno)

	if logger.GetLogger().V(logger.Warning) {
		logger.GetLogger().Log(logger.Warning, "mysql ProcessError "+errStr+" sqlHash:"+(*queryScope).SqlHash+" Cmd:"+(*queryScope).NsCmd+fmt.Sprintf(" errno:%d", errno))
	}

	switch errno {
	case 0:
		fallthrough // if there isn't a normal error number
	case 1153:
		fallthrough // pkt too large
	case 1154:
		fallthrough // read err fr pipe
	case 1155:
		fallthrough // err fnctl
	case 1156:
		fallthrough // pkt order
	case 1157:
		fallthrough // err uncompress
	case 1158:
		fallthrough // err read
	case 1159:
		fallthrough // read timeout
	case 1160:
		fallthrough // err write
	case 1161:
		fallthrough // write timeout
	case 1290:
		fallthrough // read-only mode
	case 1317:
		fallthrough // query interupt
	case 1836:
		fallthrough // read-only mode
	case 1874:
		fallthrough // innodb read-only
	case 1878: // temp file write fail
		(*workerScope).Child_shutdown_flag = true
	}
}

func (adapter *psqlAdapter) ProcessResult(colType string, res string) string {
	switch colType {
	case "DATE":
		var day, month, year int
		fmt.Sscanf(res, "%d-%d-%d", &year, &month, &day)
		return fmt.Sprintf("%02d-%02d-%d %02d:%02d:%02d.000", day, month, year, 0, 0, 0)
	case "TIME":
		var hour, min, sec int
		fmt.Sscanf(res, "%d:%d:%d", &hour, &min, &sec)
		return fmt.Sprintf("%02d-%02d-%d %02d:%02d:%02d.000", 0, 0, 0, hour, min, sec)
	case "TIMESTAMP", "DATETIME":
		var day, month, year, hour, min, sec int
		fmt.Sscanf(res, "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &min, &sec)
		return fmt.Sprintf("%02d-%02d-%d %02d:%02d:%02d.000", day, month, year, hour, min, sec)
	default:
		return res
	}
}
