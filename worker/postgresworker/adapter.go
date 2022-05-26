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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
	"strconv"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/logger"
	"github.com/paypal/hera/worker/shared"
)

type postgresAdapter struct {
	ReadOnMaster bool
	MasterDs string
}

func (adapter *postgresAdapter) MakeSqlParser() (common.SQLParser, error) {
	return common.NewRegexSQLParser()
}


func (adapter *postgresAdapter) initHelper(urlDs string) (*sql.DB, error) {
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

	                db, err = sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s", user, pwds[attempt], curDs))
			errcode := ""
	                if err == nil {
	                        err, writable = adapter.Heartbeat(db)
	                        if writable {
	                                adapter.MasterDs = curDs
	                        }
				if err != nil {
	                                // HB check failed
					if err, ok := err.(*pq.Error); ok {
						errcode = string(err.Code)
					}
	                                if logger.GetLogger().V(logger.Warning) {
	                                        logger.GetLogger().Log(logger.Warning, "HB failure" + fmt.Sprintf(" %s %s, retry-attempt=%d", errcode, err.Error(), attempt))
					}
	                                evt := cal.NewCalEvent("INITDB", "HB_Failure", cal.TransWarning, fmt.Sprintf(" %s %s, retry-attempt=%d", errcode, err.Error(), attempt))
	                                evt.SetStatus(cal.TransError)
	                                evt.Completed()
	                                //db.Close() this ?
	                        } else {
	                               break // looks good
	                        }
	                } else {
				if err, ok := err.(*pq.Error); ok {
					errcode = string(err.Code)
				}
	                        if logger.GetLogger().V(logger.Warning) {
	                                logger.GetLogger().Log(logger.Warning, user+" sql.Open fail"+curDs+fmt.Sprintf(" %d. %s %s", idx, errcode, err.Error()))
	                        }
	                }

			if errcode == "28P01" {
				attempt = attempt + 1
			} else {
				break // no retry on passwords, try next end point instead.
	                }
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



// InitDB creates sql.DB object for conection to the database, using "username", "password" and
// "db_datasource" parameters
func (adapter *postgresAdapter) InitDB() (*sql.DB, error) {
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
func (adapter *postgresAdapter) Heartbeat(db *sql.DB) (error, bool) {
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

	stmt, err := conn.PrepareContext(ctx, "select pg_is_in_recovery()")
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

func (adapter *postgresAdapter) ProcessError(errToProcess error, workerScope *shared.WorkerScopeType, queryScope *shared.QueryScopeType) (code string, msg string){
	errStr := errToProcess.Error()

	pgErr, ok := errToProcess.(*pq.Error)
	if !ok {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "not postgres error type", errStr)
		}
		return "0", errToProcess.Error()
	}
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose,
			"s=",pgErr.Severity,	                     // Sample: ERROR
			"m=",pgErr.Message,	                      // Sample: syntax error at end of input
			"(",pgErr.Code.Class(), pgErr.Code.Name(),")",   // Sample: 42 syntax_error
			"d=",pgErr.Detail,
			"h=",pgErr.Hint,
			"p=",pgErr.Position,	                     // Sample: 69
			"i=",pgErr.InternalPosition,
			"q=",pgErr.InternalQuery,
			"w=",pgErr.Where,
			"schema=",pgErr.Schema,
			"t=",pgErr.Table,
			"c=",pgErr.Column,
			"dtn=",pgErr.DataTypeName,
			"c=",pgErr.Constraint,
			"f=",pgErr.File,	                         // Sample: scan.l
			"l=",pgErr.Line,	                         // Sample: 1115
			"r=",pgErr.Routine )	                     // Sample: scanner_yyerror
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
		return string(pgErr.Code), pgErr.Message
	}

	if logger.GetLogger().V(logger.Warning) {
		logger.GetLogger().Log(logger.Warning, "postgres ProcessError "+errStr+" sqlHash:"+(*queryScope).SqlHash+" Cmd:"+(*queryScope).NsCmd) // +fmt.Sprintf(" errno:%d", errno))
	}
	return string(pgErr.Code), pgErr.Message

}

func (adapter *postgresAdapter) ProcessResult(colType string, res string) string {
	switch colType {
	case "DATE":
		fallthrough
	case "TIME":
		fallthrough
	case "TIMESTAMP":
		fallthrough
	case "TIMESTAMPTZ":
		var day, month, year, hour, min, sec int
		fmt.Sscanf(res, "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &min, &sec)
		return fmt.Sprintf("%02d-%02d-%04d %02d:%02d:%02d.000", day, month, year, hour, min, sec)
	case "TIMETZ":
		var day, month, year, hour, min, sec, pr, tzh, tzm int
		fmt.Sscanf(res, "%d-%d-%dT%d:%d:%d%d:%d", &year, &month, &day, &hour, &min, &sec, &tzh, &tzm)
		if tzh == 0 && tzm == 0 {
			fmt.Sscanf(res, "%d-%d-%dT%d:%d:%d.%d%d:%d", &year, &month, &day, &hour, &min, &sec, &pr, &tzh, &tzm)	
		}
		return fmt.Sprintf("%02d-%02d-%04d %02d:%02d:%02d.000 %+03d:%02d", day, month, year, hour, min, sec, tzh, tzm)
	default:
		return res
	}
}
