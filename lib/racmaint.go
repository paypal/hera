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

package lib

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
)

type racCfg struct {
	inst   int
	status string
	tm     int
	module string
}

type racAct struct {
	instID int
	tm     int
	delay  bool
}

type racCfgKey struct {
	inst   int
	module string
}

// MaxRacID is the maximum number of racs supported
const MaxRacID = 16

var curTime int64
var hostName string

// InitRacMaint initializes RAC maintenance, if enabled, by starting one goroutine racMaintMain per shard
func InitRacMaint(cmdLineModuleName string) {
	curTime = time.Now().Unix()
	hostName, _ = os.Hostname()
	interval := GetConfig().RacMaintReloadInterval
	if interval > 0 {
		for i := 0; i < GetConfig().NumOfShards; i++ {
			go racMaintMain(i, interval, cmdLineModuleName)
		}
	}
}

// racMaintMain wakes up every n seconds (configured in "rac_sql_interval") and reads the table
//	[ManagementTablePrefix]_maint table to see if maintenance is requested
func racMaintMain(shard int, interval int, cmdLineModuleName string) {
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Rac maint for shard =", shard, ", interval =", interval)
	}
	ctx := context.Background()
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (db) rac maint for shard =", shard)
		return
	}
	defer db.Close()
	db.SetMaxIdleConns(0)
	prev := make(map[racCfgKey]racCfg, MaxRacID+1)
	for i := 0; i <= MaxRacID; i++ {
		racRow := racCfg{}
		racRow.inst = i
		racRow.status = "U"
		racRow.tm = 0
		racRow.module = ""

		tempKey := racCfgKey{}
		tempKey.inst = i
		tempKey.module = ""
		prev[tempKey] = racRow
	}
	racSQL := fmt.Sprintf("/*shard=%d*/ SELECT inst_id, UPPER(status), status_time, UPPER(module) "+
		"FROM %s_maint "+
		"WHERE UPPER(machine) = ? and "+
		"UPPER(module) in ( ?, ? ) "+ //IN ( UPPER(sys_context('USERENV', 'MODULE')), UPPER(sys_context('USERENV', 'MODULE') || '_TAF' ) ) "+
		"ORDER BY inst_id", shard, GetConfig().ManagementTablePrefix)
	/* binds := make([]string, 2)
	binds[0], err = os.Hostname()
	binds[0] = strings.ToUpper(binds[0])
	binds[1] = strings.ToUpper(cmdLineModuleName) // */
	for {
		racMaint(ctx, shard, db, racSQL, cmdLineModuleName, prev)
		time.Sleep(time.Second * time.Duration(interval))
	}
}

/*
	racMaint is the main function for RAC maintenance processing, being called regularly.
	When maintenance is planned, it calls workerpool.RacMaint to start the actuall processing
*/
func racMaint(ctx context.Context, shard int, db *sql.DB, racSQL string, cmdLineModuleName string, prev map[racCfgKey]racCfg) {
	//
	// print this log for unittesting
	//
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Rac maint check, shard =", shard)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "Error (conn) rac maint for shard =", shard, ",err :", err)
		}
		return
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, racSQL)
	if err != nil {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "Error (stmt) rac maint for shard =", shard, ",err :", err)
		}
		return
	}

	hostname, _ := os.Hostname()
	hostname = strings.ToUpper(hostname)
	module := strings.ToUpper(cmdLineModuleName)
	module_taf := fmt.Sprintf("%s_TAF", module)
	rows, err := stmt.QueryContext(ctx, hostname, module_taf, module)
	if err != nil {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "Error (query) rac maint for shard =", shard, ",err :", err)
		}
		return
	}
	defer rows.Close()

	// TODO: we could have this cal transaction however, it is no longer needed since
	// there is an EXEC cal transaction by the worker
	evt := cal.NewCalEvent("FETCH_MGMT", fmt.Sprintf("MAINT_%d", shard), cal.TransOK, "")
	evt.Completed()
	for rows.Next() {
		row := racCfg{}
		// use NullXYZ types for NULLABLE db columns
		var status sql.NullString
		var tm sql.NullString
		err = rows.Scan(&(row.inst), &(status), &(tm), &(row.module))
		if err != nil {
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "Error (rows) rac maint for shard =", shard, ",err :", err)
			}
		}
		// set the status when Not Null
		if status.Valid {
			row.status = status.String
		}

		// set the timestamp when Not NULL
		if tm.Valid { // sql.NULLInt* does not work as expected, so have to use sql.NULLString for Number type as well
			tmVal, _ := strconv.ParseInt(tm.String, 10, 32)
			row.tm = int(tmVal)
		}
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "Rac maint row, shard =", shard, ", inst =", row.inst, ", status =", row.status, ", time =", row.tm, ", module = ", row.module)
		}
		if row.inst > MaxRacID {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "Rac maint: more than ", err)
			}
		} else {
			cfgKey := racCfgKey{}
			cfgKey.inst = row.inst
			cfgKey.module = row.module
			_, ok := prev[cfgKey]
			if false == ok {
				racRow := racCfg{}
				racRow.inst = row.inst
				racRow.status = "U"
				racRow.tm = 0
				racRow.module = row.module
				prev[cfgKey] = racRow
			}
			if row.tm != prev[cfgKey].tm || (row.status != prev[cfgKey].status) {
				racReq := racAct{instID: row.inst, tm: row.tm, delay: true}
				switch row.status {
				case "R":
					racReq.delay = true
				case "F":
					racReq.delay = false
				case "U":
					racReq.tm = 0 //This avoid accidental recycle of worker
				default:
					// any invalid command void the action
					racReq.tm = 0
					evt := cal.NewCalEvent("RACMAINT", "invalid_status", cal.TransOK, "")
					evt.AddDataInt("inst", int64(row.inst))
					evt.AddDataStr("module", row.module)
					evt.AddDataStr("status", row.status)
					evt.AddDataInt("tm", int64(row.tm))
					evt.AddDataStr("priv_status", prev[cfgKey].status)
					evt.Completed()
				}

				// Code flow will gets executed irrespective of valid or in-valid status but actual recycle of workers
				//controlled by `racReq.tm`.
				//`racReq.tm` value we are setting to "0" in-case of status "U" or "unknown" status.
				var workerpool *WorkerPool
				if strings.HasSuffix(row.module, "_TAF") {
					workerpool, err = GetWorkerBrokerInstance().GetWorkerPool(wtypeStdBy, 0, shard)
				} else {
					workerpool, err = GetWorkerBrokerInstance().GetWorkerPool(wtypeRW, 0, shard)
				}
				if err == nil {
					go workerpool.RacMaint(racReq)
				}
				if GetConfig().ReadonlyPct > 0 {
					workerpool, err := GetWorkerBrokerInstance().GetWorkerPool(wtypeRO, 0, shard)
					if err == nil {
						go workerpool.RacMaint(racReq)
					}
				}
				prev[cfgKey] = row

				out := fmt.Sprintf("%+v", prev[cfgKey])
				evt := cal.NewCalEvent("RACMAINT_INFO_CHANGE", hostName, cal.TransOK, out)
				evt.Completed()
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, "Rac maint: Change detected : ", out)
				}
			} else {
				if curTime <= time.Now().Unix() {
					out := fmt.Sprintf("%+v", prev[cfgKey])
					evt := cal.NewCalEvent("RACMAINT_INFO", hostName, cal.TransOK, out)
					evt.Completed()
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, "Rac maint: Data from DB: ", out, " time Now:", time.Now().Unix())
					}
					curTime = time.Now().Unix() + 60 // Print the racmaint_info once in every 60 seconds
				}
			}
		}
	}
}
