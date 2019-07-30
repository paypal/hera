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
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	_ "gopkg.in/goracle.v2"
	"github.com/paypal/hera/utility/logger"
	"github.com/paypal/hera/worker/shared"
)

type oracleAdapter struct {
}

// InitDB creates sql.DB object for conection to the database, using "username", "password" and "TWO_TASK" environment
func (adapter *oracleAdapter) InitDB() (*sql.DB, error) {
	user := os.Getenv("username")
	pass := os.Getenv("password")
	ds := os.Getenv("TWO_TASK")

	if user == "" {
		return nil, errors.New("Can't get 'username' from env")
	}
	if pass == "" {
		return nil, errors.New("Can't get 'password' from env")
	}
	if ds == "" {
		return nil, errors.New("Can't get 'TWO_TASK' from env")
	}

	return sql.Open("goracle", fmt.Sprintf("%s/%s@%s", user, pass, ds))
}

func (adapter *oracleAdapter) UseBindNames() bool {
	return true
}

/**
 * @TODO
 */
func (adapter *oracleAdapter) Heartbeat(db *sql.DB) (bool) {
	return true
}
/**
 * @TODO infra.hera.jdbc.HeraResultSetMetaData mysql type to java type map.
 */
var colTypeMap = map[string]int{
	"NULL":                    0,
	"NUMBER":                  2,
	"VARCHAR2":                1,
	"RAW":                     23,
	"LONG RAW":                113,
	"LONG":                    112,
	"DATE":                    12,
	"TIMESTAMP":               187,
	"TIMESTAMP WITH TIMEZONE": 188,
}

func (adapter *oracleAdapter) GetColTypeMap() map[string]int {
	return colTypeMap
}

func (adapter *oracleAdapter) ProcessError(errToProcess error, workerScope *shared.WorkerScopeType, queryScope *shared.QueryScopeType) {
        if logger.GetLogger().V(logger.Warning) {
                logger.GetLogger().Log(logger.Warning, "oracle ProcessError "+ errToProcess.Error() + " "+ (*queryScope).SqlHash +" "+(*queryScope).NsCmd)
        }
        if strings.Contains(errToProcess.Error(), "ORA-03113") {
                (*workerScope).Child_shutdown_flag = true
        }
}

func (adapter *oracleAdapter) ProcessResult(colType string, res string) string {
	switch colType {
	case "DATE":
		fallthrough
	case "TIMESTAMP":
		var day, month, year, hour, min, sec int
		fmt.Sscanf(res, "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &min, &sec)
		return fmt.Sprintf("%02d-%02d-%d %02d:%02d:%02d.000", day, month, year, hour, min, sec)
	case "TIMESTAMP WITH TIMEZONE":
		var day, month, year, hour, min, sec, tzh int
		fmt.Sscanf(res, "%d-%d-%dT%d:%d:%d%d:00", &year, &month, &day, &hour, &min, &sec, &tzh)
		return fmt.Sprintf("%02d-%02d-%d %02d:%02d:%02d.000 %+03d:00", day, month, year, hour, min, sec, tzh)
	default:
		return res
	}
}
