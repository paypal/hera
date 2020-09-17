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
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
)

// ShardMapRecord is the mapping between a physical bin to a logical shard
type ShardMapRecord struct {
	bin     int
	logical int
	flags   int
}

// ShardingCfg is an array of 1024 ShardMapRecord
type ShardingCfg struct {
	records [1024]*ShardMapRecord
}

var gShardingCfg atomic.Value

// GetShardingCfg atomically get the sharding config
func GetShardingCfg() *ShardingCfg {
	cfg := gShardingCfg.Load()
	if cfg == nil {
		return nil
	}
	return cfg.(*ShardingCfg)
}

// WLCfg keeps the whitelist configuration
// keys should be int64 or string
type WLCfg struct {
	records map[interface{}]*ShardMapRecord
}

var gWLCfg atomic.Value

// GetWLCfg atomically get whitelist config
func GetWLCfg() *WLCfg {
	cfg := gWLCfg.Load()
	if cfg == nil {
		return nil
	}
	return cfg.(*WLCfg)
}

/*
	get the SQL used to read the shard map configuration
*/
func getSQL() string {
	// TODO: add hostname in the comment
	if len(GetConfig().ShardingPostfix) != 0 {
		return fmt.Sprintf("SELECT scuttle_id, shard_id, read_status, write_status from %s_shard_map_%s where status = 'Y'", GetConfig().ManagementTablePrefix, GetConfig().ShardingPostfix)
	}
	//TODO: is this still needed?
	slowf := fmt.Sprintf("slow.%d", os.Getpid())
	f, err := os.OpenFile(slowf, os.O_RDONLY, 0)
	if err == nil {
		buf := make([]byte, 64)
		n, err := f.Read(buf[:63])
		if err == nil {
			buf[n] = 0
			sql := fmt.Sprintf("SELECT scuttle_id + usleep(%s) - %s, shard_id, read_status, write_status from %s_shard_map where status = 'Y'", string(buf), string(buf), GetConfig().ManagementTablePrefix)
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "slow shard map query ", sql)
			}
			return sql
		}
	}
	// if we get here, it means it can't get the slow query
	return fmt.Sprintf("SELECT scuttle_id, shard_id, read_status, write_status from %s_shard_map where status = 'Y'", GetConfig().ManagementTablePrefix)
}

/*
	load the physical to logical maping
*/
func loadMap(ctx context.Context, db *sql.DB) error {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Begin loading shard map")
	}
	if logger.GetLogger().V(logger.Verbose) {
		defer func() {
			logger.GetLogger().Log(logger.Verbose, "Done loading shard map")
		}()
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("Error (conn) loading shard map: %s", err.Error())
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, getSQL())
	if err != nil {
		return fmt.Errorf("Error (stmt) loading shard map: %s", err.Error())
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return fmt.Errorf("Error (query) loading shard map: %s", err.Error())
	}
	defer rows.Close()

	buckets := GetConfig().MaxScuttleBuckets
	shards := GetConfig().NumOfShards
	var cfg ShardingCfg
	for rows.Next() {
		var rstatus, wstatus sql.NullString
		var rec ShardMapRecord
		err = rows.Scan(&(rec.bin), &(rec.logical), &rstatus, &wstatus)
		if err != nil {
			return fmt.Errorf("Error (rows) loading shard map: %s", err.Error())
		}
		if rstatus.Valid && rstatus.String[0] == 'N' {
			rec.flags |= 0x0008
		}
		if wstatus.Valid && wstatus.String[0] == 'N' {
			rec.flags |= 0x0002
		}
		if (rec.bin < 0) || (rec.bin >= buckets) || (cfg.records[rec.bin] != nil) {
			if logger.GetLogger().V(logger.Alert) {
				logger.GetLogger().Log(logger.Alert, "shard map cfg bin range or doubly set", rec.bin)
			}
			msg := fmt.Sprintf("bin=%d", rec.bin)
			evt := cal.NewCalEvent(cal.EventTypeError, "SHARDMAP_BIN2X", cal.TransOK, msg)
			evt.Completed()
			continue
		}
		if (rec.logical < 0) || (rec.logical >= shards) {
			if logger.GetLogger().V(logger.Alert) {
				logger.GetLogger().Log(logger.Alert, "shard map bad logical for sbucket", rec.bin)
			}
			msg := fmt.Sprintf("sbucket=%d", rec.bin)
			evt := cal.NewCalEvent(cal.EventTypeError, "SHARDMAP_BADLOGICAL", cal.TransOK, msg)
			evt.Completed()
			rec.flags |= ShardMapRecordFlagsBadLogical
			rec.logical = -1
		}
		cfg.records[rec.bin] = &rec
	}
	old := GetShardingCfg()
	same := (old != nil)
	// only log once, the first bucket not configured
	//check all are set
	for i := 0; i < buckets; i++ {
		if cfg.records[i] == nil {
			if err == nil {
				if logger.GetLogger().V(logger.Alert) {
					logger.GetLogger().Log(logger.Alert, "shard map cfg bin not configured", i)
				}
				msg := fmt.Sprintf("sbucket=%d", i)
				evt := cal.NewCalEvent(cal.EventTypeError, "SHARDMAP_BIN0", cal.TransOK, msg)
				evt.Completed()
				err = fmt.Errorf("Error loading shard map, scuttle %d not configured", i)
			}
			rec := ShardMapRecord{bin: i, flags: ShardMapRecordFlagsBadLogical, logical: -1}
			cfg.records[i] = &rec
		}
		if same && ((old.records[i].logical != cfg.records[i].logical) || (old.records[i].flags != cfg.records[i].flags)) {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "shard map updated.", i, "is the first differing scuttle")
			}
			evt := cal.NewCalEvent(EvtTypeSharding, "shard_map_change", cal.TransOK, fmt.Sprintf("list=[%d,%d]", i, i))
			evt.Completed()
			same = false
		}
	}
	if !same {
		gShardingCfg.Store(&cfg)
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "Shard map loaded:", GetConfig().MaxScuttleBuckets, "buckets")
		}
	}
	return err
}

/**
get the SQL used to read the whitelist configuration
*/
func getWLSQL() string {
	skCol := "shard_key"

	if GetConfig().ShardKeyValueTypeIsString {
		skCol = "shard_key_string"
	}
	if len(GetConfig().ShardingPostfix) != 0 {
		return fmt.Sprintf("SELECT %s, shard_id, read_status, write_status FROM %s_whitelist_%s WHERE enable = 'Y'", skCol, GetConfig().ManagementTablePrefix, GetConfig().ShardingPostfix)
	}
	return fmt.Sprintf("SELECT %s, shard_id, read_status, write_status FROM %s_whitelist WHERE enable = 'Y'", skCol, GetConfig().ManagementTablePrefix)
}

/*
	load the whitelist mapping
*/
func loadWhitelist(ctx context.Context, db *sql.DB) {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Begin loading whitelist")
	}
	if logger.GetLogger().V(logger.Verbose) {
		defer func() {
			logger.GetLogger().Log(logger.Verbose, "Done loading whitelist")
		}()
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (conn) loading whitelist:", err)
		return
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, getWLSQL())
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (stmt) loading whitelist:", err)
		return
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (query) loading whitelist:", err)
		return
	}
	defer rows.Close()

	cfg := WLCfg{records: make(map[interface{}]*ShardMapRecord)}
	for rows.Next() {
		var shardKey uint64
		var shardKeyStr string
		var rstatus, wstatus sql.NullString
		var rec ShardMapRecord
		if GetConfig().ShardKeyValueTypeIsString {
			err = rows.Scan(&shardKeyStr, &(rec.logical), &rstatus, &wstatus)
		} else {
			err = rows.Scan(&shardKey, &(rec.logical), &rstatus, &wstatus)
		}
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "Error (rows) loading whitelist:", err)
			return
		}
		if rstatus.Valid && rstatus.String[0] == 'N' {
			rec.flags |= 0x0008
		}
		if wstatus.Valid && wstatus.String[0] == 'N' {
			rec.flags |= 0x0002
		}
		if GetConfig().ShardKeyValueTypeIsString {
			cfg.records[shardKeyStr] = &rec
		} else {
			cfg.records[shardKey] = &rec
		}
	}
	gWLCfg.Store(&cfg)
}

// initialize the golang's database/sql object used to read the database configuration. The connection is created using the loopdriver,
// a sql driver used internally for ease of programming: the config load routines use standard database/sql interface.
func openDb(shard int) (*sql.DB, error) {
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(0)
	return db, nil
}

// InitShardingCfg initializes the sharding config. If sharding and using shard map is enabled InitShardingCfg runs a go-routine which loads shard map configuration periodically
func InitShardingCfg() error {
	if GetConfig().UseShardMap {
		ctx := context.Background()
		var db *sql.DB
		var err error

		i := 0
		for ; i < 60; i++ {
			for shard := 0; shard < GetConfig().NumOfShards; shard++ {
				if db != nil {
					db.Close()
				}
				db, err = openDb(shard)
				if err == nil {
					err = loadMap(ctx, db)
					if err == nil {
						break
					}
				}
				logger.GetLogger().Log(logger.Warning, "Error <", err, "> loading the shard map from shard", shard)
				evt := cal.NewCalEvent(cal.EventTypeError, "no_shard_map", cal.TransOK, "Error loading shard map")
				evt.Completed()
			}
			if err == nil {
				break
			}
			logger.GetLogger().Log(logger.Warning, "Error loading the shard map, retry in one second")
			time.Sleep(time.Second)
		}
		if i == 60 {
			return errors.New("Failed to load shard map, no more retry")
		}
		if GetConfig().EnableWhitelistTest {
			loadWhitelist(ctx, db)
		}
		go func() {
			for {
				time.Sleep(time.Second * time.Duration(GetConfig().ShardingCfgReloadInterval))
				for shard := 0; shard < GetConfig().NumOfShards; shard++ {
					if db != nil {
						db.Close()
					}
					db, err = openDb(shard)
					if err == nil {
						err = loadMap(ctx, db)
						if err == nil {
							if shard == 0 && GetConfig().EnableWhitelistTest {
								loadWhitelist(ctx, db)
							}
							break
						}
					}
					logger.GetLogger().Log(logger.Warning, "Error <", err, "> loading the shard map from shard", shard)
					evt := cal.NewCalEvent(cal.EventTypeError, "no_shard_map", cal.TransOK, "Error loading shard map")
					evt.Completed()
				}
			}
		}()
	} else {
		var cfg ShardingCfg
		max := GetConfig().MaxScuttleBuckets
		for i := 0; i < max; i++ {
			var rec ShardMapRecord
			rec.bin = i
			cfg.records[i] = &rec
		}
		gShardingCfg.Store(&cfg)
		if GetConfig().EnableWhitelistTest {
			cfg := WLCfg{records: make(map[interface{}]*ShardMapRecord)}
			gWLCfg.Store(&cfg)
		}
	}
	return nil
}
