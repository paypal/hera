// Copyright 2022 PayPal Inc.
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
	"math/rand"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/paypal/hera/utility"
	"github.com/paypal/hera/utility/logger"
)


type QueryBindBlockerEntry struct {
	Herasqlhash uint32
    Herasqltext string // prefix since some sql is too long
	Bindvarname string // prefix for in clause
	Bindvarvalue string // when set to "BLOCKALLVALUES" should block all sqltext queries
	Blockperc int
	Heramodule string
}

type QueryBindBlockerCfg struct {
	// lookup by sqlhash
	// then by bind name, then by bind value
	BySqlHash map[uint32]map[string]map[string][]QueryBindBlockerEntry
	// check by sqltext prefix (delay to end)
}

func (cfg * QueryBindBlockerCfg) IsBlocked(sqltext string, bindPairs []string) (bool,string) {
	sqlhash := uint32(utility.GetSQLHash(sqltext))
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, fmt.Sprintf("query bind blocker sqlhash and text %d %s", sqlhash, sqltext))
	}
	byBindName, ok := cfg.BySqlHash[sqlhash]
	if !ok {
		return false, ""
	}
	for i := range bindPairs {
		if i%2 == 1 {
			continue
		}
		if strings.HasPrefix(bindPairs[i], ":") {
			bindPairs[i] = bindPairs[i][1:]
		}
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, fmt.Sprintf("query bind blocker bind name and value %d %s %s", i, bindPairs[i], bindPairs[i+1]))
		}
		byBindValue, ok := byBindName[bindPairs[i]]
		if !ok {
			// strip numeric suffix to try to match
			withoutNumSuffix := regexp.MustCompile("[_0-9]*$").ReplaceAllString(bindPairs[i],"")
			byBindValue, ok = byBindName[withoutNumSuffix]
			if !ok {
				continue
			}
		}

		val := bindPairs[i+1]
		list, ok := byBindValue[val]
		if !ok {
			val = "BLOCKALLVALUES"
			list, ok = byBindValue[val]
			if !ok {
				continue
			}
		}

		// found
		for _, entry := range list {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, fmt.Sprintf("query bind blocker checking prefix %s", entry.Herasqltext))
			}
			if !strings.HasPrefix(sqltext, entry.Herasqltext) {
				continue
			}
			dice := rand.Intn(100)
			rv := true
			rvStr := "blockRv=true"
			var logLevel int32
			logLevel = logger.Warning
			if dice > entry.Blockperc-1 {
				// got lucky, don't block
				rv = false
				rvStr = "blockRv=false"
				logLevel = logger.Debug
			}
			if logger.GetLogger().V(logLevel) {
				logger.GetLogger().Log(logLevel, fmt.Sprintf("query bind blocker on %d %s %s %d dice:%d %s", sqlhash, bindPairs[i], val, entry.Blockperc, dice, rvStr))
			}
			return rv, val
		}
	} // end for each bind
	return false, ""
}

var g_module string
var gQueryBindBlockerCfg atomic.Value

func GetQueryBindBlockerCfg() (*QueryBindBlockerCfg) {
    cfg := gQueryBindBlockerCfg.Load()
    if cfg == nil {
        return nil
    }
    return cfg.(*QueryBindBlockerCfg)
}


func InitQueryBindBlocker(modName string) {
	g_module = modName

    db, err := sql.Open("heraloop", fmt.Sprintf("0:0:0"))
    if err != nil {
		logger.GetLogger().Log(logger.Alert, "Loading query bind blocker - conn err ", err)
        return
    }
    db.SetMaxIdleConns(0)

	go func() {
		time.Sleep(4*time.Second)
		logger.GetLogger().Log(logger.Info, "Loading query bind blocker - initial")
		loadBlockQueryBind(db)
		c := time.Tick(11 * time.Second)
		for now := range c {
			logger.GetLogger().Log(logger.Info, now, "Loading query bind blocker")
			loadBlockQueryBind(db)
		}
	}()
}

func loadBlockQueryBind(db *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer cancel()
	conn, err := db.Conn(ctx);
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (conn) loading query bind blocker:", err)
		return
	}
	defer conn.Close()
	q := fmt.Sprintf("SELECT /*queryBindBlocker*/ %ssqlhash, %ssqltext, bindvarname, bindvarvalue, blockperc, %smodule FROM %s_rate_limiter where %smodule='%s'", GetConfig().StateLogPrefix, GetConfig().StateLogPrefix, GetConfig().StateLogPrefix, GetConfig().ManagementTablePrefix, GetConfig().StateLogPrefix, g_module)
	logger.GetLogger().Log(logger.Info, "Loading query bind blocker meta-sql "+q)
	stmt, err := conn.PrepareContext(ctx, q)
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (stmt) loading query bind blocker:", err)
		return
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (query) loading query bind blocker:", err)
		return
	}
	defer rows.Close()

	cfgLoad := QueryBindBlockerCfg{BySqlHash:make(map[uint32]map[string]map[string][]QueryBindBlockerEntry)}

	rowCount := 0
	for rows.Next() {
		var entry QueryBindBlockerEntry
		err = rows.Scan(&(entry.Herasqlhash), &(entry.Herasqltext), &(entry.Bindvarname), &(entry.Bindvarvalue), &(entry.Blockperc), &(entry.Heramodule))
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "Error (row scan) loading query bind blocker:", err)
			continue
		}
	
		if len(entry.Herasqltext) < GetConfig().QueryBindBlockerMinSqlPrefix {
			logger.GetLogger().Log(logger.Alert, "Error (row scan) loading query bind blocker - sqltext must be ", GetConfig().QueryBindBlockerMinSqlPrefix," bytes or more - sqlhash:", entry.Herasqlhash)
			continue
		}
		rowCount++
		sqlHash, ok := cfgLoad.BySqlHash[entry.Herasqlhash]
		if !ok {
			sqlHash = make(map[string]map[string][]QueryBindBlockerEntry)
			cfgLoad.BySqlHash[entry.Herasqlhash] = sqlHash
		}
		bindName, ok := sqlHash[entry.Bindvarname]
		if !ok {
			bindName = make(map[string][]QueryBindBlockerEntry)
			sqlHash[entry.Bindvarname] = bindName
		}
		bindVal, ok := bindName[entry.Bindvarvalue]
		if !ok {
			bindVal = make([]QueryBindBlockerEntry,0)
			bindName[entry.Bindvarvalue] = bindVal
		}
		bindName[entry.Bindvarvalue] = append(bindName[entry.Bindvarvalue], entry)
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, fmt.Sprintf("query bind blocker entry %d %s %s %s %d", entry.Herasqlhash, entry.Herasqltext, entry.Bindvarname, entry.Bindvarvalue, entry.Blockperc))
		}
	}
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, fmt.Sprintf("Loaded %d sqlhashes, %d entries, query bind blocker entries", len(cfgLoad.BySqlHash), rowCount))
	}
	gQueryBindBlockerCfg.Store(&cfgLoad)
}
