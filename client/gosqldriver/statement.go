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

package gosqldriver

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

// implements sql/driver Stmt interface and the newer StmtQueryContext and StmtExecContext interfaces
type stmt struct {
	hera           heraConnectionInterface
	sql            string
	fetchChunkSize []byte
}

func newStmt(hera heraConnectionInterface, sql string) *stmt {
	st := &stmt{hera: hera, fetchChunkSize: []byte("0")}
	// replace '?' with named parameters p1, p2, ...
	var bf bytes.Buffer
	s := sql
	idx := 1
	for {
		pos := strings.Index(s, "?")
		if pos == -1 {
			bf.WriteString(s)
			break
		}
		bf.WriteString(s[:pos])
		bf.WriteString(fmt.Sprintf(":p%d", idx))
		idx++
		s = s[pos+1:]
	}
	st.sql = bf.String()
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, hera.getID(), "final SQL:", st.sql)
	}
	return st
}

func (st *stmt) Close() error {
	return errors.New("stmt.Close() not implemented")
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (st *stmt) NumInput() int {
	return -1
}

// Implements driver.Stmt.
// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
func (st *stmt) Exec(args []driver.Value) (driver.Result, error) {
	sk := 0
	if len(st.hera.getShardKeyPayload()) > 0 {
		sk = 1
	}
	crid := 0
	if st.hera.getCorrID() != nil {
		crid = 1
	}
	binds := len(args)
	nss := make([]*netstring.Netstring, crid /*CmdClientCorrelationID*/ +1 /*CmdPrepare*/ +2*binds /* CmdBindName and CmdBindValue */ +sk /*CmdShardKey*/ +1 /*CmdExecute*/)
	idx := 0
	if crid == 1 {
		nss[0] = st.hera.getCorrID()
		st.hera.setCorrID(nil)
		idx++
	}
	nss[idx] = netstring.NewNetstringFrom(common.CmdPrepareV2, []byte(st.sql))
	idx++
	for _, val := range args {
		nss[idx] = netstring.NewNetstringFrom(common.CmdBindName, []byte(fmt.Sprintf("p%d", (idx-crid)/2+1)))
		idx++
		switch val := val.(type) {
		default:
			return nil, fmt.Errorf("unexpected parameter type %T, only int,string and []byte supported", val)
		case int:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(fmt.Sprintf("%d", int(val))))
		case int64:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(fmt.Sprintf("%d", int(val))))
		case []byte:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, val)
		case string:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(val))
		}
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, st.hera.getID(), "Bind name =", string(nss[idx-1].Payload), ", value=", string(nss[idx].Payload))
		}
		idx++
	}
	if sk == 1 {
		nss[idx] = netstring.NewNetstringFrom(common.CmdShardKey, st.hera.getShardKeyPayload())
		idx++
	}
	nss[idx] = netstring.NewNetstringFrom(common.CmdExecute, nil)
	cmd := netstring.NewNetstringEmbedded(nss)
	err := st.hera.execNs(cmd)
	if err != nil {
		return nil, err
	}
	ns, err := st.hera.getResponse()
	if err != nil {
		return nil, err
	}
	if ns.Cmd != common.RcValue {
		switch ns.Cmd {
		case common.RcSQLError:
			return nil, fmt.Errorf("SQL error: %s", string(ns.Payload))
		case common.RcError:
			return nil, fmt.Errorf("Internal hera error: %s", string(ns.Payload))
		default:
			return nil, fmt.Errorf("Unknown code: %d, data: %s", ns.Cmd, string(ns.Payload))
		}
	}
	// it was columns number, irelevant for DML
	ns, err = st.hera.getResponse()
	if err != nil {
		return nil, err
	}
	if ns.Cmd != common.RcValue {
		return nil, fmt.Errorf("Unknown code2: %d, data: %s", ns.Cmd, string(ns.Payload))
	}
	res := &result{}
	res.nRows, err = strconv.Atoi(string(ns.Payload))
	if err != nil {
		return nil, err
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, st.hera.getID(), "DML successfull, rows affected:", res.nRows)
	}
	return res, nil
}

// Implement driver.StmtExecContext method to execute a DML
func (st *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	//TODO: refactor ExecContext / Exec to reuse code
	//TODO: honor the context timeout and return when it is canceled
	sk := 0
	if len(st.hera.getShardKeyPayload()) > 0 {
		sk = 1
	}
	crid := 0
	if st.hera.getCorrID() != nil {
		crid = 1
	}
	binds := len(args)
	nss := make([]*netstring.Netstring, crid /*CmdClientCalCorrelationID*/ +1 /*CmdPrepare*/ +2*binds /* CmdBindName and BindValue */ +sk /*CmdShardKey*/ +1 /*CmdExecute*/)
	idx := 0
	if crid == 1 {
		nss[0] = st.hera.getCorrID()
		st.hera.setCorrID(nil)
		idx++
	}
	nss[idx] = netstring.NewNetstringFrom(common.CmdPrepareV2, []byte(st.sql))
	idx++
	for _, val := range args {
		if len(val.Name) > 0 {
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindName, []byte(val.Name))
		} else {
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindName, []byte(fmt.Sprintf("p%d", (idx-crid)/2+1)))
		}
		idx++
		switch val := val.Value.(type) {
		default:
			return nil, fmt.Errorf("unexpected parameter type %T, only int,string and []byte supported", val)
		case int:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(fmt.Sprintf("%d", int(val))))
		case int64:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(fmt.Sprintf("%d", int(val))))
		case []byte:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, val)
		case string:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(val))
		}
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, st.hera.getID(), "Bind name =", string(nss[idx-1].Payload), ", value=", string(nss[idx].Payload))
		}
		idx++
	}
	if sk == 1 {
		nss[idx] = netstring.NewNetstringFrom(common.CmdShardKey, st.hera.getShardKeyPayload())
		idx++
	}
	nss[idx] = netstring.NewNetstringFrom(common.CmdExecute, nil)
	cmd := netstring.NewNetstringEmbedded(nss)
	err := st.hera.execNs(cmd)
	if err != nil {
		return nil, err
	}
	ns, err := st.hera.getResponse()
	if err != nil {
		return nil, err
	}
	if ns.Cmd != common.RcValue {
		switch ns.Cmd {
		case common.RcSQLError:
			return nil, fmt.Errorf("SQL error: %s", string(ns.Payload))
		case common.RcError:
			return nil, fmt.Errorf("Internal hera error: %s", string(ns.Payload))
		default:
			return nil, fmt.Errorf("Unknown code: %d, data: %s", ns.Cmd, string(ns.Payload))
		}
	}
	// it was columns number, irelevant for DML
	ns, err = st.hera.getResponse()
	if err != nil {
		return nil, err
	}
	if ns.Cmd != common.RcValue {
		return nil, fmt.Errorf("Unknown code2: %d, data: %s", ns.Cmd, string(ns.Payload))
	}
	res := &result{}
	res.nRows, err = strconv.Atoi(string(ns.Payload))
	if err != nil {
		return nil, err
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, st.hera.getID(), "DML successfull, rows affected:", res.nRows)
	}
	return res, nil
}

// Implements driver.Stmt.
// Query executes a query that may return rows, such as a SELECT.
func (st *stmt) Query(args []driver.Value) (driver.Rows, error) {
	sk := 0
	if len(st.hera.getShardKeyPayload()) > 0 {
		sk = 1
	}
	crid := 0
	if st.hera.getCorrID() != nil {
		crid = 1
	}
	binds := len(args)
	nss := make([]*netstring.Netstring, crid /*CmdClientCorrelationID*/ +1 /*CmdPrepare*/ +2*binds /* CmdBindName and BindValue */ +sk /*CmdShardKey*/ +1 /*CmdExecute*/ +1 /* CmdFetch */)
	idx := 0
	if crid == 1 {
		nss[0] = st.hera.getCorrID()
		st.hera.setCorrID(nil)
		idx++
	}
	nss[idx] = netstring.NewNetstringFrom(common.CmdPrepareV2, []byte(st.sql))
	idx++
	for _, val := range args {
		nss[idx] = netstring.NewNetstringFrom(common.CmdBindName, []byte(fmt.Sprintf("p%d", (idx-crid)/2+1)))
		idx++
		switch val := val.(type) {
		default:
			return nil, fmt.Errorf("unexpected parameter type %T, only int,string and []byte supported", val)
		case int:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(fmt.Sprintf("%d", val)))
		case int64:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(fmt.Sprintf("%d", val)))
		case []byte:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, val)
		case string:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(val))
		}
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, st.hera.getID(), "Bind name =", string(nss[idx-1].Payload), ", value=", string(nss[idx].Payload))
		}
		idx++
	}
	if sk == 1 {
		nss[idx] = netstring.NewNetstringFrom(common.CmdShardKey, st.hera.getShardKeyPayload())
		idx++
	}
	nss[idx] = netstring.NewNetstringFrom(common.CmdExecute, nil)
	idx++
	nss[idx] = netstring.NewNetstringFrom(common.CmdFetch, st.fetchChunkSize)
	cmd := netstring.NewNetstringEmbedded(nss)
	err := st.hera.execNs(cmd)
	if err != nil {
		return nil, err
	}

	var ns *netstring.Netstring
Loop:
	for {
		ns, err = st.hera.getResponse()
		if err != nil {
			return nil, err
		}
		if ns.Cmd != common.RcValue {
			switch ns.Cmd {
			case common.RcStillExecuting:
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, st.hera.getID(), " Still executing ...")
				}
				// continues the loop
			case common.RcSQLError:
				return nil, fmt.Errorf("SQL error: %s", string(ns.Payload))
			case common.RcError:
				return nil, fmt.Errorf("Internal hera error: %s", string(ns.Payload))
			default:
				return nil, fmt.Errorf("Unknown code: %d, data: %s", ns.Cmd, string(ns.Payload))
			}
		} else {
			break Loop
		}
	}
	cols, err := strconv.Atoi(string(ns.Payload))
	if err != nil {
		return nil, err
	}

	ns, err = st.hera.getResponse()
	if err != nil {
		return nil, err
	}
	if ns.Cmd != common.RcValue {
		return nil, fmt.Errorf("Unknown code2: %d, data: %s", ns.Cmd, string(ns.Payload))
	}
	// number of rows is ignored
	_, err = strconv.Atoi(string(ns.Payload))
	if err != nil {
		return nil, err
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, st.hera.getID(), "Query successfull, num columns:", cols)
	}
	return newRows(st.hera, cols, st.fetchChunkSize)
}

// Implements driver.StmtQueryContextx
// QueryContext executes a query that may return rows, such as a SELECT
func (st *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// TODO: refactor Query/QueryContext to reuse code
	// TODO: honor the context timeout and return when it is canceled
	sk := 0
	if len(st.hera.getShardKeyPayload()) > 0 {
		sk = 1
	}
	crid := 0
	if st.hera.getCorrID() != nil {
		crid = 1
	}
	binds := len(args)
	nss := make([]*netstring.Netstring, crid /*ClientCalCorrelationID*/ +1 /*CmdPrepare*/ +2*binds /* CmdBindName and BindValue */ +sk /*ShardKey*/ +1 /*Execute*/ +1 /* Fetch */)
	idx := 0
	if crid == 1 {
		nss[0] = st.hera.getCorrID()
		st.hera.setCorrID(nil)
		idx++
	}
	nss[idx] = netstring.NewNetstringFrom(common.CmdPrepareV2, []byte(st.sql))
	idx++
	for _, val := range args {
		if len(val.Name) > 0 {
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindName, []byte(val.Name))
		} else {
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindName, []byte(fmt.Sprintf("p%d", (idx-crid)/2+1)))
		}
		idx++
		switch val := val.Value.(type) {
		default:
			return nil, fmt.Errorf("unexpected parameter type %T, only int,string and []byte supported", val)
		case int:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(fmt.Sprintf("%d", val)))
		case int64:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(fmt.Sprintf("%d", val)))
		case []byte:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, val)
		case string:
			nss[idx] = netstring.NewNetstringFrom(common.CmdBindValue, []byte(val))
		}
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, st.hera.getID(), "Bind name =", string(nss[idx-1].Payload), ", value=", string(nss[idx].Payload))
		}
		idx++
	}
	if sk == 1 {
		nss[idx] = netstring.NewNetstringFrom(common.CmdShardKey, st.hera.getShardKeyPayload())
		idx++
	}
	nss[idx] = netstring.NewNetstringFrom(common.CmdExecute, nil)
	idx++
	nss[idx] = netstring.NewNetstringFrom(common.CmdFetch, st.fetchChunkSize)
	cmd := netstring.NewNetstringEmbedded(nss)
	err := st.hera.execNs(cmd)
	if err != nil {
		return nil, err
	}

	var ns *netstring.Netstring
Loop:
	for {
		ns, err = st.hera.getResponse()
		if err != nil {
			return nil, err
		}
		if ns.Cmd != common.RcValue {
			switch ns.Cmd {
			case common.RcStillExecuting:
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, st.hera.getID(), " Still executing ...")
				}
				// continues the loop
			case common.RcSQLError:
				return nil, fmt.Errorf("SQL error: %s", string(ns.Payload))
			case common.RcError:
				return nil, fmt.Errorf("Internal hera error: %s", string(ns.Payload))
			default:
				return nil, fmt.Errorf("Unknown code: %d, data: %s", ns.Cmd, string(ns.Payload))
			}
		} else {
			break Loop
		}
	}
	cols, err := strconv.Atoi(string(ns.Payload))
	if err != nil {
		return nil, err
	}

	ns, err = st.hera.getResponse()
	if err != nil {
		return nil, err
	}
	if ns.Cmd != common.RcValue {
		return nil, fmt.Errorf("Unknown code2: %d, data: %s", ns.Cmd, string(ns.Payload))
	}
	// number of rows is ignored
	_, err = strconv.Atoi(string(ns.Payload))
	if err != nil {
		return nil, err
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, st.hera.getID(), "Query successfull, num columns:", cols)
	}
	return newRows(st.hera, cols, st.fetchChunkSize)
}

// implementing the extension HeraStmt interface
func (st *stmt) SetFetchSize(num int) {
	st.fetchChunkSize = []byte(fmt.Sprintf("%d", num))
}
