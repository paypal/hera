// Copyright 2023 PayPal Inc.
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
	"errors"
	"fmt"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// CacheRecord represents the <ManagementTablePrefix>_caching record
type CacheRecord struct {
	query_id               string
	sqlHash                uint32
	sqlText                string
	binds                  string
	ttl                    uint32
	enableShadowTest       string
	tableName              string
	invalidationClause     string
	cachingEnabled         string
	remarks                string
	module                 string
	cacheByCorrId          string
	cacheEnabledClientApps string
}

// CacheCfg contains a map of CacheRecords and is used to determine whether a sql is enabled for caching
type CacheCfg struct {
	cacheCfgRecords map[uint32]*CacheRecord
	lock            *sync.Mutex
}

var moduleName string
var gCacheCfg atomic.Value

// getCacheCfgSQL returns the query to load cache cfg.
func getCacheCfgSQL() string {
	return fmt.Sprintf(
		"SELECT query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, cache_by_corrid, caching_enabled_apps, remarks, %s_module FROM %s_caching WHERE %s_module ='%s'",
		GetConfig().StateLogPrefix, GetConfig().ManagementTablePrefix, GetConfig().StateLogPrefix, moduleName,
	)
}

// getCacheCfg returns the cache cfg.
func getCacheCfg() *CacheCfg {
	cfg := gCacheCfg.Load()
	if cfg == nil {
		out := &CacheCfg{cacheCfgRecords: make(map[uint32]*CacheRecord), lock: &sync.Mutex{}}
		gCacheCfg.Store(out)
		return out
	}
	return cfg.(*CacheCfg) //Assertion to type case
}

// loadCacheCfg queries the <ManagementTablePrefix>_caching table and populates the cache cfg.
func loadCacheCfg(ctx context.Context, db *sql.DB) error {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Begin loading CacheCfg")
	}
	if logger.GetLogger().V(logger.Verbose) {
		defer func() {
			logger.GetLogger().Log(logger.Verbose, "Done loading cache config")
		}()
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (conn) loading cache config", err)
		return fmt.Errorf("Error (conn) loading cache config: %s", err.Error())
	}
	defer conn.Close()
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "CacheCfgSQL:", getCacheCfgSQL())
	}
	stmt, err := conn.PrepareContext(ctx, getCacheCfgSQL())
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (stmt) loading cache config", err)
		return fmt.Errorf("Error (stmt) loading cache config: %s", err.Error())
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (query) loading cache config", err)
		return fmt.Errorf("Error (query) loading cache config: %s", err.Error())
	}
	defer rows.Close()

	cfgLoad := &CacheCfg{cacheCfgRecords: make(map[uint32]*CacheRecord), lock: &sync.Mutex{}}
	logger.GetLogger().Log(logger.Verbose, "No errors. Begin loading rows...")
	rowCount := 0
	for rows.Next() {
		var rec CacheRecord
		var bindVariables sql.NullString
		var invalidationClause sql.NullString
		err = rows.Scan(
			&(rec.query_id), &(rec.sqlHash), &(rec.sqlText), &bindVariables, &(rec.ttl), &(rec.enableShadowTest),
			&(rec.tableName), &invalidationClause, &(rec.cachingEnabled), &(rec.cacheByCorrId), &(rec.cacheEnabledClientApps), &(rec.remarks), &(rec.module),
		)
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "Error (rows scan) loading cache config", err)
			return fmt.Errorf("Error (rows scan) loading cache config: %s", err.Error())
		}

		if bindVariables.Valid {
			rec.binds = bindVariables.String
		}

		if invalidationClause.Valid {
			rec.invalidationClause = invalidationClause.String
		}

		rowCount++
		// To-Do: Any pre-validation checks if required
		cfgLoad.cacheCfgRecords[rec.sqlHash] = &rec
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(
				logger.Verbose, fmt.Sprintf(
					"cacheCfgRecords entry: queryId:%s, sqlHash:%d, sqlText:%s, Binds:%s, TTL: %d, enableShadowTest:%s, tableName:%s, invClause:%s, cachingEnabled:%s, cacheByCorrId:%s, cacheEnabledClientApps:%s, remarks:%s, module:%s",
					rec.query_id, rec.sqlHash, rec.sqlText, rec.binds, rec.ttl, rec.enableShadowTest, rec.tableName,
					rec.invalidationClause, rec.cachingEnabled, rec.cacheByCorrId, rec.cacheEnabledClientApps, rec.remarks, rec.module,
				),
			)
		}
	}
	logger.GetLogger().Log(logger.Verbose, fmt.Sprintf("Loaded %d sqlhashes, %d cacheCfg entries", len(cfgLoad.cacheCfgRecords), rowCount))
	if rowCount > 0 {
		host, _ := os.Hostname()
		if len(host) >= 40 {
			host = host[:39]
		}
		calevt := cal.NewCalEvent("loadCacheCfg", host, cal.TransOK, "")
		calevt.AddDataInt("cacheCfgRecordsSize", int64(len(cfgLoad.cacheCfgRecords)))
		calevt.Completed()
	}
	gCacheCfg.Store(cfgLoad)
	return err
}

// InitCachingCfg tries to load the cache cfg. If it fails after 3 attempts, it gives up. If the initial load is successful,
// it tries to refresh the cache cfg every CachingCfgReloadInterval seconds.
func InitCachingCfg(modName string) error {
	logger.GetLogger().Log(logger.Verbose, "InitCachingCfg for module:", modName)
	moduleName = modName
	ctx := context.Background()
	var db *sql.DB
	var err error
	i := 0
	for ; i < 3; i++ {
		time.Sleep(time.Millisecond * 2000)
		if db != nil {
			db.Close()
		}
		db, err = sql.Open("heraloop", fmt.Sprintf("0:0:0"))
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "Error (db) InitCaching - conn error ", err)
			return err
		}
		db.SetMaxIdleConns(0)
		err = loadCacheCfg(ctx, db)
		if err == nil {
			break
		}
	}
	if i == 3 {
		logger.GetLogger().Log(logger.Verbose, "Reached max retries (3)")
		return errors.New("Failed to load caching config, no more retry..." + err.Error())
	}
	go func() {
		for {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "Inside Routine to periodically load CacheConfig")
				temp := getCacheCfg()
				temp.lock.Lock()
				logger.GetLogger().Log(logger.Verbose, fmt.Sprintf("cacheCfgRecord size inside routine: %d", len(temp.cacheCfgRecords)))
				for k, v := range temp.cacheCfgRecords {
					logger.GetLogger().Log(logger.Verbose, fmt.Sprintf("Key SQLHash:%d", k))
					logger.GetLogger().Log(logger.Verbose, fmt.Sprintf("Value:%s", v.sqlText))
				}
				temp.lock.Unlock()
			}
			time.Sleep(time.Second * time.Duration(GetConfig().CachingCfgReloadInterval))
			if db != nil {
				db.Close()
			}
			db, err = sql.Open("heraloop", fmt.Sprintf("0:0:0"))
			db.SetMaxIdleConns(0)
			if err == nil {
				err = loadCacheCfg(ctx, db)
				if err != nil {
					logger.GetLogger().Log(logger.Alert, "Error loading CacheCfg ", err)
				}
			} else {
				logger.GetLogger().Log(logger.Alert, "Error (db) InitCaching - conn error ", err)
			}
		}
	}()

	return nil

}
