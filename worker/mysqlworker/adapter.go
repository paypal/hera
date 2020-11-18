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
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/logger"
	"github.com/paypal/hera/worker/shared"
)

type mysqlAdapter struct {
}

func (adapter *mysqlAdapter) MakeSqlParser() (common.SQLParser, error) {
	return common.NewRegexSQLParser()
}

// InitDB creates sql.DB object for conection to the mysql database, using "username", "password" and
// "mysql_datasource" parameters
func (adapter *mysqlAdapter) InitDB() (*sql.DB, error) {
	user := os.Getenv("username")
	pass := os.Getenv("password")
	ds := os.Getenv("mysql_datasource")
	calTrans := cal.NewCalTransaction(cal.TransTypeURL, "INITDB", cal.TransOK, "", cal.DefaultTGName)
	if user == "" {
		calTrans.AddDataStr("m_err", "USERNAME_NOT_FOUND")
		calTrans.AddDataStr("m_errtype", "CONNECT")
		calTrans.AddDataStr("m_datasource", ds)
		calTrans.SetStatus(cal.TransFatal)
		calTrans.Completed()
		return nil, errors.New("Can't get 'username' from env")
	}
	if pass == "" {
		calTrans.AddDataStr("m_err", "PASSWORD_NOT_FOUND")
		calTrans.AddDataStr("m_errtype", "CONNECT")
		calTrans.AddDataStr("m_datasource", ds)
		calTrans.SetStatus(cal.TransFatal)
		calTrans.Completed()
		return nil, errors.New("Can't get 'password' from env")
	}
	if ds == "" {
		calTrans.AddDataStr("m_err", "DATASOURCE_NOT_FOUND")
		calTrans.AddDataStr("m_errtype", "CONNECT")
		calTrans.SetStatus(cal.TransFatal)
		calTrans.Completed()
		return nil, errors.New("Can't get 'mysql_datasource' from env")
	}

	var db *sql.DB
	var err error
	for idx, curDs := range strings.Split(ds, "||") {
		user := os.Getenv("username")
		pass := os.Getenv("password")
		attempt := 1
		is_writable := false
		for attempt <= 3 {
			db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@%s", user, pass, curDs))
			if err != nil {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, user+" failed to connect to "+curDs+fmt.Sprintf(" %d", idx))
				}
				calTrans.AddDataStr("m_err", err.Error())
				calTrans.AddDataStr("m_errtype", "CONNECT")
				calTrans.AddDataStr("m_datasource", curDs+fmt.Sprintf(" %d", idx))
				calTrans.SetStatus(cal.TransFatal)
				break
			}
			is_writable = adapter.Heartbeat(db)
			if is_writable {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, user+" connect success "+curDs+fmt.Sprintf(" %d", idx))
				}
				err = nil
				break
			} else {
				// read only connection
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "recycling, got read-only conn " /*+curDs*/ +fmt.Sprintf("Attempt=%d", attempt))
				}
				db.Close()
				if attempt == 1 { // If attempt 1 failed then try with password2
					if os.Getenv("password2") !=  "" {
						pass = os.Getenv("password2")
					} else  {
						if logger.GetLogger().V(logger.Info) {
							logger.GetLogger().Log(logger.Info, "Password2 not found for " +curDs)
						}
						attempt = attempt + 1
					}
				}
				if attempt == 2 { // If attempt 2 failed then try with password3
					if os.Getenv("password3") !=  "" {
						pass = os.Getenv("password3")
					}  else {
						if logger.GetLogger().V(logger.Info) {
							logger.GetLogger().Log(logger.Info, "Password3 not found for " +curDs)
						}
						attempt = attempt + 1
					}
				}
				attempt = attempt + 1
				if attempt >= 3 {
					calTrans.AddDataStr("m_err", "READONLY_CONN")
					calTrans.AddDataStr("m_errtype", "CONNECT")
					calTrans.AddDataStr("m_datasource", curDs+fmt.Sprintf(" %d", idx))
					calTrans.SetStatus(cal.TransFatal)
					err = errors.New("cannot use read-only conn " + curDs)
				}
			}
		}
		if is_writable {
			break
		}
	}
	calTrans.Completed()
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

func (adapter *mysqlAdapter) ProcessError(errToProcess error, workerScope *shared.WorkerScopeType, queryScope *shared.QueryScopeType) {
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

func (adapter *mysqlAdapter) ProcessResult(colType string, res string) string {
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
