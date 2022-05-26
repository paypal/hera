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
	"math/rand"
	"os"
	"strings"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/logger"
	"github.com/paypal/hera/worker/shared"
)

type mysqlAdapter struct {
	ReadOnMaster bool
	MasterDs string
}

func (adapter *mysqlAdapter) MakeSqlParser() (common.SQLParser, error) {
	return common.NewRegexSQLParser()
}


func (adapter *mysqlAdapter) initHelper(urlDs string) (*sql.DB, error) {
        user := os.Getenv("username")
        if user == "" {
		evt := cal.NewCalEvent("INITDB", "USERNAME_NOT_FOUND", cal.TransWarning, fmt.Sprintf("m_datasource %s", urlDs))
		evt.SetStatus(cal.TransError)
		evt.Completed()
                return nil, errors.New("Can't get 'username' from env")
        }
        if urlDs == "" {
		evt := cal.NewCalEvent("INITDB", "DATASOURCE_NOT_FOUND", cal.TransWarning, "")
		evt.SetStatus(cal.TransError)
		evt.Completed()
                return nil, errors.New("Can't get 'db_datasource' from env")
        }

        var db *sql.DB
        var err error
        var writable bool
        pwds := [3]string{os.Getenv("password"), os.Getenv("password2"), os.Getenv("password3")}
        // allows multiple db endpoints
        for idx, curDs := range strings.Split(urlDs, "||") {
                attempt := 0
                // retry on 3 password upon login error 
                for attempt <= 2 {
                        if len(pwds[attempt]) == 0 {
                                if logger.GetLogger().V(logger.Warning) {
                                        logger.GetLogger().Log(logger.Warning, fmt.Sprintf("InitDB password %d is not set", attempt))
                                }
                                attempt = attempt +1
                                continue
                        }

                        db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@%s", user, pwds[attempt], curDs))
                        if err == nil {
                                err, writable = adapter.Heartbeat(db)
                                if writable {
					adapter.MasterDs = curDs
				}
				if err != nil {
                                        // HB check failed
                                        if logger.GetLogger().V(logger.Warning) {
                                                logger.GetLogger().Log(logger.Warning, "HB failure " + fmt.Sprintf("%s, retry-attempt=%d", err.Error(), attempt))
                                        }
                                        evt := cal.NewCalEvent("INITDB", "HB_Failure", cal.TransWarning, fmt.Sprintf("%s, retry-attempt=%d", err.Error(), attempt))
                                        evt.SetStatus(cal.TransError)
                                        evt.Completed()
					//db.Close()
                                } else {
	                               break // try next endpoint
				}
                        } else {
                                if logger.GetLogger().V(logger.Warning) {
                                        logger.GetLogger().Log(logger.Warning, user+" sql.Open fail"+curDs+fmt.Sprintf(" %d. %s", idx, err.Error()))
                                }
                                // retry with next password if error is access denied 1044 or 1045 otherwise break to try next endpoint
                                errno := adapter.getDBErrCode(err.Error())
				if errno != 1044 && errno != 1045 {
					break // try next endpoint upon non-login error
				} 
                        }
			attempt = attempt + 1
                }

                if err == nil { // has a successful connection
                        if logger.GetLogger().V(logger.Info) {
                                logger.GetLogger().Log(logger.Warning, user+" connect success "+curDs+fmt.Sprintf(" %d", idx))
                        }
                        err = nil
                        break
                }
        }
	return db, err
}


// InitDB creates sql.DB object for conection to the mysql database, using "username", "password" and
// "db_datasource" parameters
func (adapter *mysqlAdapter) InitDB() (*sql.DB, error) {
	ds := os.Getenv("db_datasource")
	adapter.ReadOnMaster = false // only turn true if no read copy found
	calTrans := cal.NewCalTransaction(cal.TransTypeURL, "INITDB", cal.TransOK, "", cal.DefaultTGName)
	calTrans.AddDataStr("m_ds", ds)
	db, err := adapter.initHelper(ds)
	var spread time.Duration
	if err != nil {
		if strings.HasPrefix(err.Error(), "HB rw check") {
			wkrType := os.Getenv("logger.LOG_PREFIX")
			if strings.HasPrefix(wkrType, "R-WORKER ") || strings.HasPrefix(wkrType, "S-WORKER") {
				adapter.ReadOnMaster = true
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "INITDB allow read connection fallback to write") 
				}
				calTrans.AddDataStr("m_ReadOnMaster", strconv.FormatBool(adapter.ReadOnMaster))
				// read connection final try on the master if it's available.
				db, err = adapter.initHelper(adapter.MasterDs)
				if err == nil {
					calTrans.Completed()
					return db, err
				}
			}
			// if it's HB RW error, then reduce the range of wait
			spread = 5* time.Second + time.Duration(rand.Intn(5000000))
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "INITDB onErr apply short sleeping "+spread.String())
			}
		} else {
			spread = 11 * time.Second + time.Duration(rand.Intn(11000999888)/*ns*/)
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "INITDB onErr apply long sleeping "+spread.String())
			}
		}
		time.Sleep(spread)
	}

	calTrans.Completed()
	return db, err
}

// Checking master status and others
//  TODO: may extend to cover replica state 
func (adapter *mysqlAdapter) Heartbeat(db *sql.DB) (error, bool) {
	writable := false
	ctx, _ /*cancel*/ := context.WithTimeout(context.Background(), 10*time.Second)
	conn, err := db.Conn(ctx)
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "HB could not get connection "+err.Error())
		}
		return err, writable
	}
	defer conn.Close()

	stmt, err := conn.PrepareContext(ctx, "select @@global.read_only")
	//stmt, err := conn.PrepareContext(ctx, "show variables where variable_name='read_only'")
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "HB query ro check err ", err.Error())
		}
		return err, writable
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "HB ro check err ", err.Error())
		}
		return err, writable
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

	err = nil
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "HB ReadOnMaster=" +
			strconv.FormatBool(adapter.ReadOnMaster) + " DB writable=" + strconv.FormatBool(writable))
	}
	wkrType := os.Getenv("logger.LOG_PREFIX")
	if strings.HasPrefix(wkrType, "WORKER ") {
		// write connection
		if writable == false {
			err = errors.New("HB rw check: Write connection to read-only db")
		}
	} else if strings.HasPrefix(wkrType, "R-WORKER ") || strings.HasPrefix(wkrType, "S-WORKER") {
		// Read and standby workers should be able to fall back to write instance if it can't find a read copy
		if adapter.ReadOnMaster == false && writable == true {
			err = errors.New("HB rw check: Read connection should connect to read-only db")
		}
	} 
	return err, writable
}

// UseBindNames return false because the SQL string uses ? for bind parameters
func (adapter *mysqlAdapter) UseBindNames() bool {
	return false
}

func (adapter *mysqlAdapter) UseBindQuestionMark() bool {
	return true
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

func (adapter *mysqlAdapter) getDBErrCode(errMsg string) int {
        idx := strings.Index(errMsg, ":")
        if idx < 0 || idx >= len(errMsg) {
                return 0
        }
        var rc int
        fmt.Sscanf(errMsg[6:idx], "%d", &rc)
	return rc
}

func (adapter *mysqlAdapter) ProcessError(errToProcess error, workerScope *shared.WorkerScopeType, queryScope *shared.QueryScopeType) (code string, msg string) {
	errStr := errToProcess.Error()

	if strings.HasPrefix(errStr, "driver: bad connection") {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "mysql ProcessError badConnRecycle "+errStr+" sqlHash:"+(*queryScope).SqlHash+" Cmd:"+(*queryScope).NsCmd)
		}
		(*workerScope).Child_shutdown_flag = true
		return "0", errStr
	}

	errno := adapter.getDBErrCode(errStr)

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
	return strconv.Itoa(errno), errStr
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
