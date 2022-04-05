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
	"database/sql"
	"database/sql/driver"
	"reflect"
	"unsafe"

	"github.com/paypal/hera/utility/logger"
)

//HeraConn is an API extension for sql.Conn
type HeraConn interface {
	// manualy set the shard ID to which the SQL are executed. it is "sticky", it stays set for all the subsequent
	// SQLs, until ResetShardID() is called. When the shard ID is set, the shard autodiscovery is disabled
	SetShardID(shard int) error
	// resets the shard ID set by SetShardID
	ResetShardID() error
	// returns the number os shards
	GetNumShards() (int, error)

	// This is used for queries which don't have shard key. The format is "<key>=<value1>;<value2>;...<valuen>"
	SetShardKeyPayload(payload string)
	// Reset the state set via SetShardKeyPayload
	ResetShardKeyPayload()

	// Identifier used for logging, specific to a session of SQLs
	SetCalCorrID(corrID string)

	SetClientInfo(poolname string, host string) error
}

// HeraStmt is an API extension for *sql.Stmt
type HeraStmt interface {
	// A hit to the server for how many rows to return at once
	SetFetchSize(num int)
}

// InnerConn returns a HeraConn interface from a *sql.Conn. It uses reflection to walk the internal structure of *sql.Conn to
// return the inner structure implemented in the driver
func InnerConn(c *sql.Conn) HeraConn {
	nilv := reflect.Value{}
	el := reflect.ValueOf(c).Elem().FieldByName("dc")
	if el == nilv {
		logger.GetLogger().Log(logger.Alert, "sql.Conn doesn't have a field dc")
		return nil
	}
	el = el.Elem().FieldByName("ci")
	if el == nilv {
		logger.GetLogger().Log(logger.Alert, "sql.Conn.dc doesn't have a field ci")
		return nil
	}
	drvConn := (*driver.Conn)(unsafe.Pointer(el.UnsafeAddr()))
	if drvConn == nil {
		logger.GetLogger().Log(logger.Alert, "nil connection")
		return nil
	}
	return (*drvConn).(*heraConnection)
}

// InnerStmt returns a HeraStmt interface from a *sql.Stmt. It uses reflection to walk the internal structure of *sql.Stmt to
// return the inner structure implemented in the driver
func InnerStmt(st *sql.Stmt) HeraStmt {
	nilv := reflect.Value{}
	el := reflect.ValueOf(st).Elem().FieldByName("cgds")
	if el == nilv {
		logger.GetLogger().Log(logger.Alert, "sql.Stmt doesn't have a field cgds")
		return nil
	}
	el = el.Elem().FieldByName("si")
	if el == nilv {
		logger.GetLogger().Log(logger.Alert, "sql.Stmt.cgds doesn't have a field si")
		return nil
	}
	drvStmt := (*driver.Stmt)(unsafe.Pointer(el.UnsafeAddr()))
	if drvStmt == nil {
		logger.GetLogger().Log(logger.Alert, "nil stmt")
		return nil
	}
	return (*drvStmt).(*stmt)
}
