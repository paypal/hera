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

package shared

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"

	"database/sql"
)

// CmdProcessorAdapter is interface for differentiating the specific database implementations.
// For example there is an adapter for MySQL, another for Oracle
type CmdProcessorAdapter interface {
	InitDB() (*sql.DB, error)
	UseBindNames() bool
	GetColTypeMap() map[string]int
	// this is used for date related types to translate between the database format to the mux format
	ProcessResult(colType string, res string) string
}

// bindType defines types of bind variables
type bindType int

// constants for BindType
const (
	btUnknown bindType = iota
	btIn
	btOut
)

// BindValue is a placeholder for a bind value, with index tracking its position in the query.
type BindValue struct {
	index int
	name  string
	value interface{}
	//
	// whether client has passed in a value.
	//
	valid bool
	//
	// input or output.
	//
	btype bindType
	// the data type
	dataType common.DataType
}

// CmdProcessor holds the data needed to process the client commmands
type CmdProcessor struct {
	ctx context.Context
	// adapter for various databases
	adapter CmdProcessorAdapter
	//
	// socket to mux
	//
	SocketOut *os.File
	//
	// db instance.
	//
	db *sql.DB
	//
	// the connection
	//
	conn *sql.Conn
	//
	// open txn if having dml.
	//
	tx *sql.Tx
	//
	// prepared statement yet to be executed.
	//
	stmt *sql.Stmt
	//
	// tells if the current SQL is a query which returns result set (i.e. SELECT)
	//
	hasResult bool
	// tells if the current connection is in transaction. it becomes true if a DML ran successfull
	inTrans bool
	// tells if the current connection has an open cursor
	inCursor bool
	//
	// all bindvar for the query after parsing.
	// using map with name key instead of array with position index for faster matching
	// when processing OCC_BINNAME/VALUE since some queres can set hundreds of bindvar.
	//
	bindVars map[string]*BindValue
	// placeholders for bindouts
	bindOuts    []string
	numBindOuts int
	//
	// matching bindname to location in query for faster lookup at OCC_EXEC.
	//
	bindPos []string
	//
	// occ protocol let client sends bindname in one ns command and bindvalue for the
	// bindname in the very next ns command. this parameter is used to track which
	// name is for the current value.
	//
	currentBindName string
	//
	// result set for read query.
	//
	rows *sql.Rows
	//
	// result for dml query.
	//
	result sql.Result
	//
	//
	//
	sqlParser     common.SQLParser
	regexBindName *regexp.Regexp
	//
	// cal txn for the current session.
	//
	calSessionTxn cal.Transaction
	// cal txn for a SQL
	calExecTxn cal.Transaction
	// last error
	lastErr error
	// the FNV hash of the SQL, for logging
	sqlHash uint32
	// the name of the cal TXN
	calSessionTxnName string
}

// NewCmdProcessor creates the processor using th egiven adapter
func NewCmdProcessor(adapter CmdProcessorAdapter, sockMux *os.File) *CmdProcessor {
	cs := os.Getenv("CAL_CLIENT_SESSION")
	if cs == "" {
		cs = "CLIENT_SESSION"
	}

	return &CmdProcessor{adapter: adapter, SocketOut: sockMux, calSessionTxnName: cs}
}

// ProcessCmd implements the client commands like prepare, bind, execute, etc
func (cp *CmdProcessor) ProcessCmd(ns *netstring.Netstring) error {
	if ns == nil {
		return errors.New("empty netstring passed to processcommand")
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "process command", DebugString(ns.Serialized))
	}
	var err error

outloop:
	switch ns.Cmd {
	case common.CmdClientCalCorrelationID:
		//
		// @TODO parse out correlationid.
		//
		if cp.calSessionTxn != nil {
			cp.calSessionTxn.SetCorrelationID("@todo")
		}
	case common.CmdPrepare, common.CmdPrepareV2, common.CmdPrepareSpecial:
		cp.lastErr = nil
		cp.sqlHash = 0
		//
		// need to turn "select * from table where ca=:a and cb=:b"
		// to "select * from table where ca=? and cb=?"
		// while keeping an ordered list of (":a"=>"val_:a", ":b"=>"val_:b") to run
		// stmt.Exec("val_:a", "val_:b"). val_:a and val_:b are extracted using
		// BindName and BindValue
		//
		sqlQuery := cp.preprocess(string(ns.Payload))
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "Preparing:", sqlQuery)
		}
		//
		// start a new transaction for the first dml request.
		//
		var startTrans bool
		cp.hasResult, startTrans = cp.sqlParser.Parse(sqlQuery)
		if cp.calSessionTxn == nil {
			cp.calSessionTxn = cal.NewCalTransaction(cal.TransTypeAPI, cp.calSessionTxnName, cal.TransOK, "", cal.DefaultTGName)
		}
		cp.sqlHash = utility.GetSQLHash(string(ns.Payload))
		cp.calExecTxn = cal.NewCalTransaction(cal.TransTypeExec, fmt.Sprintf("%d", cp.sqlHash), cal.TransOK, "", cal.DefaultTGName)
		if (cp.tx == nil) && (startTrans) {
			cp.tx, err = cp.db.Begin()
		}
		if cp.tx != nil {
			cp.stmt, err = cp.tx.Prepare(sqlQuery)
		} else {
			cp.stmt, err = cp.db.Prepare(sqlQuery)
		}
		if err != nil {
			cp.calExecErr("Prepare", err.Error())
			cp.lastErr = err
			err = nil
		}
		cp.rows = nil
		cp.result = nil
		cp.bindOuts = cp.bindOuts[:0]
		cp.numBindOuts = 0
	case common.CmdBindName, common.CmdBindOutName:
		if cp.stmt != nil {
			cp.currentBindName = string(ns.Payload)
			if strings.HasPrefix(string(ns.Payload), ":") {
				cp.currentBindName = string(ns.Payload)
			} else {
				var buffer bytes.Buffer
				buffer.WriteString(":")
				buffer.Write(ns.Payload)
				cp.currentBindName = buffer.String()
			}
			if cp.bindVars[cp.currentBindName] == nil {
				//
				// @TODO a bindname not in the query.
				//
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "nonexisting bindname", cp.currentBindName)
				}
				err = fmt.Errorf("bindname not found in query: %s", cp.currentBindName)
				cp.calExecErr("Bind error", cp.currentBindName)
				break
			}
			if ns.Cmd == common.CmdBindName {
				cp.bindVars[cp.currentBindName].btype = btIn
			} else {
				cp.bindVars[cp.currentBindName].btype = btOut
				cp.bindVars[cp.currentBindName].valid = true
				cp.numBindOuts++
			}
			cp.bindVars[cp.currentBindName].dataType = common.DataTypeString
		}
	case common.CmdBindType:
		if cp.stmt != nil {
			var btype int
			btype, err = strconv.Atoi(string(ns.Payload))
			if err != nil {
				cp.calExecErr("BindTypeConv", err.Error())
				break
			}
			cp.bindVars[cp.currentBindName].dataType = common.DataType(btype)
		}
	case common.CmdBindValue:
		if cp.stmt != nil {
			//
			// double check to make sure.
			//
			if cp.bindVars[cp.currentBindName] == nil {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "nonexisting bindname", cp.currentBindName)
				}
				err = fmt.Errorf("bindname not found in query: %s", cp.currentBindName)
				cp.calExecErr("BindValNF", cp.currentBindName)
				break
			} else {
				if len(ns.Payload) == 0 {
					cp.bindVars[cp.currentBindName].value = sql.NullString{}
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, "BindValue:", cp.currentBindName, ":", cp.bindVars[cp.currentBindName].dataType, ":<nil>")
					}
				} else {
					switch cp.bindVars[cp.currentBindName].dataType {
					case common.DataTypeTimestamp:
						var day, month, year, hour, min, sec, ms int
						fmt.Sscanf(string(ns.Payload), "%d-%d-%d %d:%d:%d.%d", &day, &month, &year, &hour, &min, &sec, &ms)
						cp.bindVars[cp.currentBindName].value = time.Date(year, time.Month(month), day, hour, min, sec, ms*1000000, time.UTC)
					case common.DataTypeTimestampTZ:
						var day, month, year, hour, min, sec, ms, tzh, tzm int
						fmt.Sscanf(string(ns.Payload), "%d-%d-%d %d:%d:%d.%d %d:%d", &day, &month, &year, &hour, &min, &sec, &ms, &tzh, &tzm)
						// Note: the Go Oracle driver ignores th elocation, always uses time.Local
						cp.bindVars[cp.currentBindName].value = time.Date(year, time.Month(month), day, hour, min, sec, ms*1000000, time.FixedZone("Custom", tzh*3600))
					case common.DataTypeRaw,  common.DataTypeBlob:
						cp.bindVars[cp.currentBindName].value = ns.Payload
					default:
						cp.bindVars[cp.currentBindName].value = sql.NullString{String: string(ns.Payload), Valid: true}
					}
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, "BindValue:", cp.currentBindName, ":", cp.bindVars[cp.currentBindName].dataType, ":", cp.bindVars[cp.currentBindName].value)
					}
				}
				cp.bindVars[cp.currentBindName].valid = true
			}
		}
	case common.CmdBindNum:
		if cp.stmt != nil {
			err = fmt.Errorf("Batch not supported")
			cp.calExecErr("Batch", err.Error())
			break
		}
	case common.CmdExecute:
		if cp.stmt != nil {
			//
			// step through bindvar at each location to build bindinput.
			//
			bindinput := make([]interface{}, 0)
			if cap(cp.bindOuts) >= cp.numBindOuts {
				cp.bindOuts = cp.bindOuts[:cp.numBindOuts]
				// clear old values just in case
				for i := range cp.bindOuts {
					cp.bindOuts[i] = ""
				}
			} else {
				cp.bindOuts = make([]string, cp.numBindOuts)
			}
			curbindout := 0
			for i := 0; i < len(cp.bindPos); i++ {
				key := cp.bindPos[i]
				val := cp.bindVars[key]
				if val.btype == btIn {
					if !val.valid {
						err = fmt.Errorf("bindname undefined: %s", key)
						break outloop
					}
					if cp.adapter.UseBindNames() {
						bindinput = append(bindinput, sql.Named(key[1:], val.value))
					} else {
						bindinput = append(bindinput, val.value)
					}
				} else if val.btype == btOut {
					if cp.adapter.UseBindNames() {
						value := sql.Named(key[1:], sql.Out{Dest: &(cp.bindOuts[curbindout])})
						bindinput = append(bindinput, value)
						if logger.GetLogger().V(logger.Debug) {
							logger.GetLogger().Log(logger.Debug, "bindout", val.index, value, curbindout)
						}
						curbindout++
					} else {
						err = errors.New("outbind not supported")
						break outloop
					}
				}
			}
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "Executing ", cp.inTrans)
				logger.GetLogger().Log(logger.Debug, "BINDS", bindinput)
			}
			if len(bindinput) == 0 {
				//
				// @TODO: do we keep a flag for curent statement.
				//
				if cp.hasResult {
					cp.rows, err = cp.stmt.Query()
				} else {
					cp.result, err = cp.stmt.Exec()
				}
			} else {
				if cp.hasResult {
					cp.rows, err = cp.stmt.Query(bindinput...)
				} else {
					cp.result, err = cp.stmt.Exec(bindinput...)
				}
			}
			if err != nil {
				cp.calExecErr("RC", err.Error())
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "Execute error:", err.Error())
				}
				if cp.inTrans {
					cp.eor(common.EORInTransaction, netstring.NewNetstringFrom(common.RcSQLError, []byte(err.Error())))
				} else {
					cp.eor(common.EORFree, netstring.NewNetstringFrom(common.RcSQLError, []byte(err.Error())))
				}
				err = nil
				break
			}
			if cp.tx != nil {
				cp.inTrans = true
			}
			cp.calExecTxn.Completed()
			cp.calExecTxn = nil
			if cp.result != nil {
				var rowcnt int64
				rowcnt, err = cp.result.RowsAffected()
				if err != nil {
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, "RowsAffected():", err.Error())
					}
					cp.calExecErr("RowsAffected", err.Error())
					break
				}
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, "exe row", rowcnt)
				}
				sz := 2
				if len(cp.bindOuts) > 0 {
					sz++
					sz += len(cp.bindOuts)
				}
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, "BINDOUTS", len(cp.bindOuts), cp.bindOuts)
				}

				nss := make([]*netstring.Netstring, sz)
				nss[0] = netstring.NewNetstringFrom(common.RcValue, []byte("0"))
				nss[1] = netstring.NewNetstringFrom(common.RcValue, []byte(strconv.FormatInt(rowcnt, 10)))
				if sz > 2 {
					if len(cp.bindOuts) > 0 {
						nss[2] = netstring.NewNetstringFrom(common.RcValue, []byte("1"))
						for i := 0; i < len(cp.bindOuts); i++ {
							nss[i+3] = netstring.NewNetstringFrom(common.RcValue, []byte(cp.bindOuts[i]))
						}
					}
				}
				resns := netstring.NewNetstringEmbedded(nss)
				err = cp.eor(common.EORInTransaction, resns)
			}
			if cp.rows != nil {
				var cols []string
				cols, err = cp.rows.Columns()
				if err != nil {
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "rows.Columns()", err.Error())
					}
					cp.calExecErr("Columns", err.Error())
					break
				}
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, "exe col", cols, len(cols))
				}
				// TODO: what is there are rows?
				sz := 2
				if len(cp.bindOuts) > 0 {
					sz++
				}

				nss := make([]*netstring.Netstring, sz)
				nss[0] = netstring.NewNetstringFrom(common.RcValue, []byte(strconv.Itoa(len(cols))))
				nss[1] = netstring.NewNetstringFrom(common.RcValue, []byte("0"))
				if sz > 2 {
					nss[2] = netstring.NewNetstringFrom(common.RcValue, []byte("0"))
				}
				resns := netstring.NewNetstringEmbedded(nss)
				if cp.hasResult {
					/*
						TODO: this is the proper implementation, need to fix mux, meanwhile just done use EOR_IN_CURSOR_...
						if cp.inTrans {
							cp.eor(EOR_IN_CURSOR_IN_TRANSACTION, resns)
						} else {
							cp.eor(EOR_IN_CURSOR_NOT_IN_TRANSACTION, resns)
						}
					*/
					WriteAll(cp.SocketOut, resns.Serialized)
				} else {
					if cp.inTrans {
						cp.eor(common.EORInTransaction, resns)
					} else {
						cp.eor(common.EORFree, resns)
					}
				}
			}
		} else {
			if cp.inTrans {
				cp.eor(common.EORInTransaction, netstring.NewNetstringFrom(common.RcSQLError, []byte(cp.lastErr.Error())))
			} else {
				cp.eor(common.EORFree, netstring.NewNetstringFrom(common.RcSQLError, []byte(cp.lastErr.Error())))
			}
		}
	case common.CmdFetch:
		// TODO fecth chunk size
		if cp.rows != nil {
			calt := cal.NewCalTransaction(cal.TransTypeFetch, fmt.Sprintf("%d", cp.sqlHash), cal.TransOK, "", cal.DefaultTGName)
			var cts []*sql.ColumnType
			cts, err = cp.rows.ColumnTypes()
			if err != nil {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "rows.Columns()", err.Error())
				}
				calt.AddDataStr("RC", err.Error())
				calt.SetStatus(cal.TransError)
				calt.Completed()
				break
			}
			var nss []*netstring.Netstring
			cols, _ := cp.rows.Columns()
			readCols := make([]interface{}, len(cols))
			writeCols := make([]sql.NullString, len(cols))
			for i := range writeCols {
				readCols[i] = &writeCols[i]
			}
			for cp.rows.Next() {
				err = cp.rows.Scan(readCols...)
				if err != nil {
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "fetch:", err.Error())
					}
					calt.AddDataStr("RC", err.Error())
					calt.SetStatus(cal.TransError)
					calt.Completed()
					break
				}
				for i := range writeCols {
					var outstr string
					if writeCols[i].Valid {
						outstr = cp.adapter.ProcessResult(cts[i].DatabaseTypeName(), writeCols[i].String)
					}
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, "query result", outstr)
					}
					nss = append(nss, netstring.NewNetstringFrom(common.RcValue, []byte(outstr)))
				}
			}
			if len(nss) > 0 {
				resns := netstring.NewNetstringEmbedded(nss)
				err = WriteAll(cp.SocketOut, resns.Serialized)
				if err != nil {
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "Error writing to mux", err.Error())
					}
					calt.AddDataStr("RC", "Comm error")
					calt.SetStatus(cal.TransError)
					calt.Completed()
					break
				}
			}
			calt.Completed()
			if cp.inTrans {
				cp.eor(common.EORInTransaction, netstring.NewNetstringFrom(common.RcNoMoreData, nil))
			} else {
				cp.eor(common.EORFree, netstring.NewNetstringFrom(common.RcNoMoreData, nil))
			}
			cp.rows = nil
		} else {
			if cp.inTrans {
				cp.eor(common.EORInTransaction, netstring.NewNetstringFrom(common.RcError, []byte("fetch requested but no statement exists")))
			} else {
				cp.eor(common.EORFree, netstring.NewNetstringFrom(common.RcError, []byte("fetch requested but no statement exists")))
			}
		}
	case common.CmdColsInfo:
		if cp.rows == nil {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "OCC_COLS_INFO with no cursor, possible after a failed query?")
			}
			// no error returned, this happens if the query fails so the client doesn't expect response
			break
		}
		var cts []*sql.ColumnType
		cts, err = cp.rows.ColumnTypes()
		if err != nil {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "rows.Columns()", err.Error())
			}
			break
		}
		if cts == nil {
			ns := netstring.NewNetstringFrom(common.RcValue, []byte("0"))
			err = WriteAll(cp.SocketOut, ns.Serialized)
		} else {
			nss := make([]*netstring.Netstring, len(cts)*5+1)
			nss[0] = netstring.NewNetstringFrom(common.RcValue, []byte(strconv.Itoa(len(cts))))
			var cnt = 1
			var width, prec, scale int64
			var ok = true
			for _, ct := range cts {
				nss[cnt] = netstring.NewNetstringFrom(common.RcValue, []byte(ct.Name()))
				cnt++
				typename := ct.DatabaseTypeName()
				if len(typename) == 0 {
					typename = "UNDEFINED"
				}
				nss[cnt] = netstring.NewNetstringFrom(common.RcValue, []byte(strconv.Itoa(cp.adapter.GetColTypeMap()[strings.ToUpper(typename)])))
				cnt++
				width, ok = ct.Length()
				if !ok {
					width = 0
				}
				nss[cnt] = netstring.NewNetstringFrom(common.RcValue, []byte(strconv.FormatInt(width, 10)))
				cnt++
				prec, scale, ok = ct.DecimalSize()
				if !ok {
					prec = 0
					scale = 0
				}
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, "colinfo", cnt, ct.Name(), typename, width, prec, scale)
				}
				//
				// java int is 32bit, occclientimpl.java has
				// meta.setPrecision(Integer.parseInt(new String(obj.getData())))
				// that would not take value like 9223372036854775807.
				//
				if prec > 2147483647 {
					prec = 2147483647
				}
				if scale > 2147483647 {
					scale = 2147483647
				}
				nss[cnt] = netstring.NewNetstringFrom(common.RcValue, []byte(strconv.FormatInt(prec, 10)))
				cnt++
				nss[cnt] = netstring.NewNetstringFrom(common.RcValue, []byte(strconv.FormatInt(scale, 10)))
				cnt++
			}
			resns := netstring.NewNetstringEmbedded(nss)
			err = WriteAll(cp.SocketOut, resns.Serialized)
		}
	case common.CmdCommit:
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "Commit")
		}
		if cp.tx != nil {
			calevt := cal.NewCalEvent("COMMIT", "Local", cal.TransOK, "")
			err = cp.tx.Commit()
			if err != nil {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "Commit error:", err.Error())
				}
				calevt.AddDataStr("RC", err.Error())
				calevt.SetStatus(cal.TransError)
			} else {
				cp.tx = nil
			}
			calevt.Completed()
		} else {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "Commit issued without a transaction")
			}
		}
		if err == nil {
			cp.inTrans = false
			cp.eor(common.EORFree, netstring.NewNetstringFrom(common.RcOK, nil))
		} else {
			cp.eor(common.EORInTransaction, netstring.NewNetstringFrom(common.RcSQLError, []byte(err.Error())))
			err = nil
		}
	case common.CmdRollback:
		if cp.tx != nil {
			calevt := cal.NewCalEvent("ROLLBACK", "Local", cal.TransOK, "")
			err = cp.tx.Rollback()
			if err != nil {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "Rollback error:", err.Error())
				}
				calevt.AddDataStr("RC", err.Error())
				calevt.SetStatus(cal.TransError)
			} else {
				cp.tx = nil
			}
			calevt.Completed()
		} else {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "Rollback issued without a transaction")
			}
		}
		if err == nil {
			cp.inTrans = false
			cp.eor(common.EORFree, netstring.NewNetstringFrom(common.RcOK, nil))
		} else {
			cp.eor(common.EORInTransaction, netstring.NewNetstringFrom(common.RcSQLError, []byte(err.Error())))
			err = nil
		}
	}

	return err
}

// InitDB performs various initializations at start time
func (cp *CmdProcessor) InitDB() error {
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "setup db connection.")
	}
	var err error
	cp.db, err = cp.adapter.InitDB()
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "driver error", err.Error())
		}
		return err
	}
	cp.ctx = context.Background()
	cp.db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(cp.ctx, time.Second*60)
	defer cancel()
	cp.conn, err = cp.db.Conn(ctx)
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "db connection error", err.Error())
		}
		return err
	}

	//
	cp.sqlParser, err = common.NewRegexSQLParser()
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "bindname regex complie:", err.Error())
		}
		return err
	}
	// MySQL can have ` as the first character in the table name as well as the column_name
	cp.regexBindName, err = regexp.Compile(":([`]?[a-zA-Z])\\w*[`]?")
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "bindname regex complie:", err.Error())
		}
		return err
	}

	return nil
}

func (cp *CmdProcessor) eor(code int, ns *netstring.Netstring) error {
	if (code == common.EORFree) && (cp.calSessionTxn != nil) {
		cp.calSessionTxn.Completed()
		cp.calSessionTxn = nil
	}
	var payload []byte
	if ns != nil {
		payload = make([]byte, len(ns.Serialized)+1)
		payload[0] = byte('0' + code)
		copy(payload[1:], ns.Serialized)
	} else {
		payload = []byte{byte('0' + code)}
	}
	return WriteAll(cp.SocketOut, netstring.NewNetstringFrom(common.CmdEOR, payload).Serialized)
}

func (cp *CmdProcessor) calExecErr(field string, err string) {
	cp.calExecTxn.AddDataStr(field, err)
	cp.calExecTxn.SetStatus(cal.TransError)
	cp.calExecTxn.Completed()
	cp.calExecTxn = nil
}

/**
 * extract bindnames and save them in bindVars with their position index.
 * replace bindnames in query with "?"
 */
func (cp *CmdProcessor) preprocess(query string) string {
	//
	// @TODO strip comment sections which could have ":".
	//

	//
	// SELECT account_number,flags,return_url,time_created,identity_token FROM wseller
	// WHERE account_number=:account_number
	// and flags=:flags and return_url=:return_url,
	//
	binds := cp.regexBindName.FindAllString(query, -1)
	//
	// just create a new map for each query. the old map if any will be gc out later.
	//
	cp.bindVars = make(map[string]*BindValue)
	cp.bindPos = make([]string, len(binds))
	for i, val := range binds {
		cp.bindVars[val] = &(BindValue{index: i, name: val, valid: false, btype: btUnknown})
		cp.bindPos[i] = val
	}
	if !(cp.adapter.UseBindNames()) {
		query = cp.regexBindName.ReplaceAllString(query, "?")
	}
	return query
}
