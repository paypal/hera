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
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/logger"
)

type mysqlAdapter struct {
}

func (adapter *mysqlAdapter) MakeSqlParser() (common.SQLParser ,error) {
	return common.NewAutoCommitParser()
}

// InitDB creates sql.DB object for conection to the mysql database, using "username", "password" and
// "mysql_datasource" parameters
func (adapter *mysqlAdapter) InitDB() (*sql.DB, error) {
	user := os.Getenv("username")
	pass := os.Getenv("password")
	ds := os.Getenv("mysql_datasource")

	if user == "" {
		return nil, errors.New("Can't get 'username' from env")
	}
	if pass == "" {
		return nil, errors.New("Can't get 'password' from env")
	}
	if ds == "" {
		return nil, errors.New("Can't get 'mysql_datasource' from env")
	}

	var db *sql.DB
	var err error
	is_writable := false
	for idx, curDs := range strings.Split(ds, "||") {
		db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@%s", user, pass, curDs))
		if err != nil {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, user+" failed to connect to "+curDs+fmt.Sprintf(" %d", idx))
			}
			continue
		}
		is_writable = adapter.Heartbeat(db);
		if is_writable {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, user+" connect success "+curDs+fmt.Sprintf(" %d", idx))
			}
			err = nil
			break
		} else {
			// read only connection
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "recycling, got read-only conn " /*+curDs*/)
			}
			err = errors.New("cannot use read-only conn "+curDs)
			db.Close()
		}
	}
	return db, err
}

// Checking master status
func (adapter *mysqlAdapter) Heartbeat(db *sql.DB) bool {
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
		stmt, err := conn.PrepareContext(ctx, "select @@global.read_only")
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
func (adapter *mysqlAdapter) UseBindNames() bool {
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

func (adapter *mysqlAdapter) GetColTypeMap() map[string]int {
	return colTypeMap
}

func (adapter *mysqlAdapter) ProcessResult(colType string, res string) string {
	switch colType {
	case "DATE":
		var day, month, year int
		fmt.Sscanf(res, "%d-%d-%d", &year, &month, &day)
		return fmt.Sprintf("%02d-%02d-%d %02d:%02d:%02d.000", day, month, year, 0, 0, 0)
	case "TIMESTAMP":
		var day, month, year, hour, min, sec int
		fmt.Sscanf(res, "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &min, &sec)
		return fmt.Sprintf("%02d-%02d-%d %02d:%02d:%02d.000", day, month, year, hour, min, sec)
	default:
		return res
	}
}
