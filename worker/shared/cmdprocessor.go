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
	MakeSqlParser() (common.SQLParser, error)
	GetColTypeMap() map[string]int
	Heartbeat(*sql.DB) bool
	InitDB() (*sql.DB, error)
	/* ProcessError's workerScope["child_shutdown_flag"] = "1 or anything" can help terminate after the request */
	ProcessError(errToProcess error, workerScope *WorkerScopeType, queryScope *QueryScopeType)
	// ProcessResult is used for date related types to translate between the database format to the mux format
	ProcessResult(colType string, res string) string
	UseBindNames() bool
	UseBindQuestionMark() bool // true for mysql, false for postgres $1 $2 binds
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
	// socket receiving ctrl messages from mux
	//
	SocketCtrl *os.File
	//
	// db instance.
	//
	db *sql.DB
	//
	// open txn if having dml.
	//
	tx *sql.Tx
	//
	// prepared statement yet to be executed.
	//
	stmt *sql.Stmt
	didExecAtPrepare bool
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
	// when processing CmdBindName/Value since some queres can set hundreds of bindvar.
	//
	bindVars map[string]*BindValue
	// placeholders for bindouts
	bindOuts    []string
	numBindOuts int
	sendLastInsertId bool
	//
	// matching bindname to location in query for faster lookup at CmdExec.
	//
	bindPos []string
	//
	// hera protocol let client sends bindname in one ns command and bindvalue for the
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
	// corr_id for logging
	m_corr_id string
	// the name of the cal TXN
	calSessionTxnName string
	heartbeat         bool
	// counter for requests, acting like ID
	rqId uint32
	// request ID of the last EOR free
	rqIdEORFree uint32
	// used in eor() to send the right code
	moreIncomingRequests func() bool
	queryScope           QueryScopeType
	WorkerScope          WorkerScopeType
	// tells if the worker is dedicated: either in cursor or in transaction
	dedicated bool
}

type QueryScopeType struct {
	NsCmd   string
	SqlHash string
}
type WorkerScopeType struct {
	Child_shutdown_flag bool
}

const LAST_INSERT_ID_BIND_OUT_NAME = ":p5000"

const ErrInFailedTransaction = "pq: Could not complete operation in a failed transaction"

// NewCmdProcessor creates the processor using th egiven adapter
func NewCmdProcessor(adapter CmdProcessorAdapter, sockMux *os.File, sockMuxCtrl *os.File) *CmdProcessor {
	cs := os.Getenv("CAL_CLIENT_SESSION")
	if cs == "" {
		cs = "CLIENT_SESSION"
	}

	return &CmdProcessor{adapter: adapter, SocketOut: sockMux, SocketCtrl: sockMuxCtrl, calSessionTxnName: cs, heartbeat: true}
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

	cp.queryScope.NsCmd = fmt.Sprintf("%d", ns.Cmd)
outloop:
	switch ns.Cmd {
	case common.CmdClientCalCorrelationID:
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "CmdClientCalCorrelationID:", string(ns.Payload), string(ns.Serialized))
		}
		cp.m_corr_id = "unset"
		if len(string(ns.Payload)) > 0 {
			splits := strings.Split(string(ns.Payload), "=")
			if (len(splits) == 2) && (len(splits[1]) > 0) {
				logger.GetLogger().Log(logger.Verbose, "splits:", len(splits), splits[0], splits[1])
				cp.m_corr_id = splits[1]
			} else {
				logger.GetLogger().Log(logger.Warning, "CmdClientCalCorrelationID: Payload not in expected K=V format:", string(ns.Payload))
			}
		}
	case common.CmdClientInfo:
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "CmdClientInfo:", string(ns.Payload), string(ns.Serialized))
		}
		if len(string(ns.Payload)) > 0 {
			splits := strings.Split(string(ns.Payload), "|")
			if (len(splits) == 2) {
				logger.GetLogger().Log(logger.Verbose, "len clientApplication:", len(splits[0]))
				logger.GetLogger().Log(logger.Verbose, "len poolStack:", len(splits[1]))
				if len(splits[0]) == 0 {
					logger.GetLogger().Log(logger.Verbose, "clientApplication: unknown")
				} else {
					logger.GetLogger().Log(logger.Verbose, "clientApplication:", splits[0])
				}
				logger.GetLogger().Log(logger.Verbose, "poolStack:", splits[1])
				//
				// @TODO Add CLIENT_INFO event inside calSessionTxn
				//
			} else {
				logger.GetLogger().Log(logger.Debug, "CmdClientInfo: Payload not in expected Client&PoolStack format:", string(ns.Payload))
			}
		}
	case common.CmdPrepare, common.CmdPrepareV2, common.CmdPrepareSpecial:
		cp.dedicated = true
		cp.queryScope = QueryScopeType{}
		cp.lastErr = nil
		cp.sqlHash = 0
		cp.heartbeat = false // for hb
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
			cp.calSessionTxn.AddDataStr("corrid", cp.m_corr_id)
		}
		cp.m_corr_id = "unset" // Reset after logging
		cp.calSessionTxn.SendSQLData(string(ns.Payload))
		cp.sqlHash = utility.GetSQLHash(string(ns.Payload))
		cp.queryScope.SqlHash = fmt.Sprintf("%d", cp.sqlHash)
		cp.calExecTxn = cal.NewCalTransaction(cal.TransTypeExec, fmt.Sprintf("%d", cp.sqlHash), cal.TransOK, "", cal.DefaultTGName)
		if (cp.tx == nil) && (startTrans) {
			cp.tx, err = cp.db.Begin()
		}
		if cp.stmt != nil {
			cp.stmt.Close()
			cp.stmt = nil
		}
		cp.didExecAtPrepare = false
		if cp.sqlParser.MustExecInsteadOfPrepare(sqlQuery) {
			cp.didExecAtPrepare = true
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "didExecAtPrepare: exec'ing at prepare")
			}
			_, err = cp.tx.Exec(sqlQuery)
			cp.calExecTxn.AddDataStr("directExec","t")
			cp.calExecTxn.Completed()
			cp.calExecTxn = nil
			// keep cp.stmt nil so we don't exec
			cp.stmt = nil
		} else if cp.tx != nil {
			cp.stmt, err = cp.tx.Prepare(sqlQuery)
		} else {
			cp.stmt, err = cp.db.Prepare(sqlQuery)
		}
		if err != nil {
			cp.adapter.ProcessError(err, &cp.WorkerScope, &cp.queryScope)
			cp.calExecErr("Prepare", err.Error())
			cp.lastErr = err
			err = nil
		}
		cp.rows = nil
		cp.result = nil
		cp.bindOuts = cp.bindOuts[:0]
		cp.numBindOuts = 0
		cp.sendLastInsertId = false
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
			if cp.bindVars[cp.currentBindName] == nil && cp.currentBindName != LAST_INSERT_ID_BIND_OUT_NAME {
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
			if cp.currentBindName == LAST_INSERT_ID_BIND_OUT_NAME {
				cp.bindVars[cp.currentBindName] = &(BindValue{index: 5000, name: LAST_INSERT_ID_BIND_OUT_NAME, valid: false, btype: btUnknown})
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
					case common.DataTypeRaw, common.DataTypeBlob:
						cp.bindVars[cp.currentBindName].value = ns.Payload
					case common.DataTypeBool:
						bValue, err := strconv.ParseBool(string(ns.Payload))
						if err != nil {
							cp.calExecErr("BindValueConv", err.Error())
							break
						}
						cp.bindVars[cp.currentBindName].value = bValue
					case common.DataTypeInt:
						bValue, err := strconv.Atoi(string(ns.Payload))
						if err != nil {
							cp.calExecErr("BindValueConv", err.Error())
							break
						}
						cp.bindVars[cp.currentBindName].value = bValue
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
			if _,ok := cp.bindVars[LAST_INSERT_ID_BIND_OUT_NAME]; ok {
				cp.sendLastInsertId = true
			}
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
				cp.adapter.ProcessError(err, &cp.WorkerScope, &cp.queryScope)
				cp.calExecErr("RC", err.Error())
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "Execute error:", err.Error())
				}
				// Adding additional check to see if txn is already open. cp.inTrans is set to true only when a DML ran successfully.
				// If the first statement in a txn fails, worker thinks that it is not in a txn and returns EOR free to mux.
				// The worker moves from Busy -> Finished -> Accept again. The rollback sent by the client is a NoOp and mux responds OK.
				// The older txn is not closed and is used for newer transactions too, thereby causing the "PSQLException: current transaction is aborted, commands ignored until end of transaction block"
				// Adding this check ensures that the worker is moved to wait state and waits for the client to send either commit/rollback.
				if cp.inTrans || cp.tx != nil {
					cp.eor(common.EORInTransaction, netstring.NewNetstringFrom(common.RcSQLError, []byte(err.Error())))
				} else {
					cp.eor(common.EORFree, netstring.NewNetstringFrom(common.RcSQLError, []byte(err.Error())))
				}
				cp.lastErr = err
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

				lastId, err := cp.result.LastInsertId()
				if err != nil {
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, "LastInsertId():", err.Error(), "sendLastInsertId:",cp.sendLastInsertId)
					}
				} else {
					// have last insert id
					if cp.sendLastInsertId {
						cp.bindOuts[0] = fmt.Sprintf("%d", lastId)
						if logger.GetLogger().V(logger.Debug) {
							logger.GetLogger().Log(logger.Debug, "LastInsertId() bindOut:", lastId)
						}
					}
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
					WriteAll(cp.SocketOut, resns)
				} else {
					if cp.inTrans {
						cp.eor(common.EORInTransaction, resns)
					} else {
						cp.eor(common.EORFree, resns)
					}
				}
			}
		} else {
			if cp.didExecAtPrepare {
				// for mysql begin/start transaction
				// exec already done instead of prepare
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, "didExecAtPrepare exec skip since exec'd at prepare")
				}
				nss := make([]*netstring.Netstring, 2)
				nss[0] = netstring.NewNetstringFrom(common.RcValue, []byte("0")) // cols
				nss[1] = netstring.NewNetstringFrom(common.RcValue, []byte("0")) // rows
				// no bind outs
				resns := netstring.NewNetstringEmbedded(nss)
				cp.eor(common.EORInTransaction, resns)
			} else if cp.inTrans || cp.tx != nil {
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
					cp.adapter.ProcessError(err, &cp.WorkerScope, &cp.queryScope)
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
				err = WriteAll(cp.SocketOut, resns)
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
			// send back to client only if last result was ok
			var nsr *netstring.Netstring
			if cp.lastErr == nil {
				nsr = netstring.NewNetstringFrom(common.RcError, []byte("fetch requested but no statement exists"))
			}
			if cp.inTrans {
				cp.eor(common.EORInTransaction, nsr)
			} else {
				cp.eor(common.EORFree, nsr)
			}
		}
	case common.CmdColsInfo:
		if cp.rows == nil {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "CmdColsInfo with no cursor, possible after a failed query?")
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
			err = WriteAll(cp.SocketOut, ns)
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
				//
				// java int is 32bit, HeraClientImpl.java has
				// meta.setPrecision(Integer.parseInt(new String(obj.getData())))
				// that would not take value like 9223372036854775807.
				//
				if width > 2147483647 {
					width = 2147483647
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
			err = WriteAll(cp.SocketOut, resns)
		}
	case common.CmdCommit:
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "Commit")
		}
		if cp.tx != nil {
			calevt := cal.NewCalEvent("COMMIT", "Local", cal.TransOK, "")
			err = cp.tx.Commit()
			if err != nil {
				cp.adapter.ProcessError(err, &cp.WorkerScope, &cp.queryScope)
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "Commit error:", err.Error())
				}
				// This is a postgres specific error that is returned by the pq driver to the client to indicate that the client
				// atttempted to commit a failed transaction. It does a rollback instead of commit and returns the message.
				//  For more details refer: https://github.com/lib/pq/blob/master/conn.go#L571
				if err.Error() == ErrInFailedTransaction {
					logger.GetLogger().Log(logger.Debug, "Issued Commit in a failed transaction")
					calevt.AddDataStr("RC", err.Error())
					cp.tx = nil
					err = nil
				} else {
					calevt.AddDataStr("RC", err.Error())
					calevt.SetStatus(cal.TransError)
				}
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
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "Rollback")
		}
		if cp.tx != nil {
			calevt := cal.NewCalEvent("ROLLBACK", "Local", cal.TransOK, "")
			err = cp.tx.Rollback()
			if err != nil {
				cp.adapter.ProcessError(err, &cp.WorkerScope, &cp.queryScope)
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

func (cp *CmdProcessor) SendDbHeartbeat() bool {
	var masterIsUp bool
	masterIsUp = cp.adapter.Heartbeat(cp.db)
	return masterIsUp
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
	cp.db.SetMaxOpenConns(1)

	//
	// cp.sqlParser, err = common.NewRegexSQLParser()
	cp.sqlParser, err = cp.adapter.MakeSqlParser()
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
	if code == common.EORFree {
		if cp.moreIncomingRequests() {
			code = common.EORMoreIncomingRequests
		} else {
			if cp.rqId == cp.rqIdEORFree {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "EOR free again, rqId:", cp.rqId)
				}
				calevt := cal.NewCalEvent("ERROR", "EOR_FREE_AGAIN", cal.TransError, "")
				calevt.Completed()
				return errors.New("EOR_FREE_AGAIN")
			}
			if cp.calSessionTxn != nil {
				cp.calSessionTxn.Completed()
				cp.calSessionTxn = nil
			}
			cp.rqIdEORFree = cp.rqId
			cp.dedicated = false
		}

	}

	datalen := 0
	if ns != nil {
		datalen = len(ns.Serialized)
	}
	payload := make([]byte, 1 /*code*/ +4 /*rqId*/ +datalen)
	payload[0] = byte('0' + code)
	payload[1] = byte((cp.rqId & 0xFF000000) >> 24)
	payload[2] = byte((cp.rqId & 0x00FF0000) >> 16)
	payload[3] = byte((cp.rqId & 0x0000FF00) >> 8)
	payload[4] = byte(cp.rqId & 0x000000FF)
	if datalen != 0 {
		copy(payload[5:], ns.Serialized)
	}
	cp.heartbeat = true
	return WriteAll(cp.SocketOut, netstring.NewNetstringFrom(common.CmdEOR, payload))
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
	// @TODO duplicate bind names
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
		if cp.adapter.UseBindQuestionMark() {
			query = cp.regexBindName.ReplaceAllString(query, "?")
		} else {
			var dollarBindQuery strings.Builder
			curIdx := 0
			// TODO share FindAll.. with binds, also check bind order!
			for _,matchIdx := range cp.regexBindName.FindAllStringIndex(query, -1) {
				curBindName := query[matchIdx[0]:matchIdx[1]]
				dollarBindQuery.WriteString(query[curIdx:matchIdx[0]])
				dollarBindQuery.WriteString(fmt.Sprintf("$%d",cp.bindVars[curBindName].index+1))
				curIdx = matchIdx[1]
			}
			dollarBindQuery.WriteString(query[curIdx:])
			query = dollarBindQuery.String()
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, query, "dollarBindQ")
			}
		}
	}
	return query
}

func (cp *CmdProcessor) isIdle() bool {
	return !(cp.inCursor) && !(cp.inTrans)
}
