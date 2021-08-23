// Copyright 2021 PayPal Inc.
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
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	_ "github.com/lib/pq"
	"github.com/lib/pq"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/logger"
	"github.com/paypal/hera/worker/shared"
)

type postgresAdapter struct {
}

func (adapter *postgresAdapter) MakeSqlParser() (common.SQLParser, error) {
	return common.NewRegexSQLParser()
}

// InitDB creates sql.DB object for conection to the database, using "username", "password" and
// "mysql_datasource" parameters
func (adapter *postgresAdapter) InitDB() (*sql.DB, error) {
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
	// 
	// postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full
	// user=pqgotest dbname=pqgotest sslmode=verify-full
	// host=%s port=%d user=%s password=%s dbname=%s sslmode=disable
	//
	for idx, curDs := range strings.Split(ds, "||") {
		user := os.Getenv("username")
		pass := os.Getenv("password")
		attempt := 1
		is_writable := false
		for attempt <= 3 {
			//db, err = sql.Open("postgres", fmt.Sprintf("user=%s password=%s %s", user, pass, curDs))
			db, err = sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s", user, pass, curDs))
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

func (adapter *postgresAdapter) Heartbeat(db *sql.DB) bool {
	// perhaps - select inet_server_addr()
	return true
}
// UseBindNames return false because the SQL string uses $1 $2 for bind parameters
func (adapter *postgresAdapter) UseBindNames() bool {
	return false
}
func (adapter *postgresAdapter) UseBindQuestionMark() bool {
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

func (adapter *postgresAdapter) GetColTypeMap() map[string]int {
	return colTypeMap
}

func (adapter *postgresAdapter) ProcessError(errToProcess error, workerScope *shared.WorkerScopeType, queryScope *shared.QueryScopeType) {
	errStr := errToProcess.Error()

	pgErr, ok := errToProcess.(*pq.Error)
	if !ok {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "not postgres error type", errStr)
		}
		return
	}
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose,
			"s=",pgErr.Severity,                             // Sample: ERROR
			"m=",pgErr.Message,                              // Sample: syntax error at end of input
			"(",pgErr.Code.Class(), pgErr.Code.Name(),")",   // Sample: 42 syntax_error
			"d=",pgErr.Detail,
			"h=",pgErr.Hint,
			"p=",pgErr.Position,                             // Sample: 69
			"i=",pgErr.InternalPosition,
			"q=",pgErr.InternalQuery,
			"w=",pgErr.Where,
			"schema=",pgErr.Schema,
			"t=",pgErr.Table,
			"c=",pgErr.Column,
			"dtn=",pgErr.DataTypeName,
			"c=",pgErr.Constraint,
			"f=",pgErr.File,                                 // Sample: scan.l
			"l=",pgErr.Line,                                 // Sample: 1115
			"r=",pgErr.Routine )                             // Sample: scanner_yyerror
	}

	switch pgErr.Code.Class().Name() {
	case "08": fallthrough // Connection Exception
	case "24": fallthrough // Invalid Cursor State
	case "25": fallthrough // Invalid Transaction State
	case "2D": fallthrough // Invalid Transaction Termination
	case "3B": fallthrough // Savepoint Exception
	//case "53": fallthrough // Insufficient Resources
	case "58": fallthrough // External System Error
	case "XX": fallthrough // Internal Error
	case "handleErrorClass":
		// some fatal errors should set shutdown
		(*workerScope).Child_shutdown_flag = true
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, pgErr.Code.Class().Name()+"=errClass postgres ProcessError setting child shutdown flag "+errStr+" sqlHash:"+(*queryScope).SqlHash+" Cmd:"+(*queryScope).NsCmd)
		}
	}

	if strings.HasPrefix(errStr, "driver: bad connection") {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "postgres ProcessError badConnRecycle "+errStr+" sqlHash:"+(*queryScope).SqlHash+" Cmd:"+(*queryScope).NsCmd)
		}
		return
	}

	if logger.GetLogger().V(logger.Warning) {
		logger.GetLogger().Log(logger.Warning, "postgres ProcessError "+errStr+" sqlHash:"+(*queryScope).SqlHash+" Cmd:"+(*queryScope).NsCmd) // +fmt.Sprintf(" errno:%d", errno))
	}

}

func (adapter *postgresAdapter) ProcessResult(colType string, res string) string {
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
