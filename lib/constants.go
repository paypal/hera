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
	"errors"
)

// CAL constants
const (
	EvtTypeTAF     = "TAF"
	EvtNameTAFTmo  = "TMO"
	EvtNameTAFOra  = "ORA_"
	EvtNAmeTafBklg = "BKLG"

	EvtTypeSharding           = "SHARDING"
	EvtTypeMux                = "HERAMUX"
	EvtNameBadShardID         = "bad_shard_id"
	EvtNameUnkKey             = "unknown_key_name"
	EvtNameShardIDAndKey      = "shard_id_shard_key_coexist"
	EvtNameMultiShard         = "multi_shard_key_values"
	EvtNameSetShardIDInTxn    = "set_shard_id_in_txn"
	EvtNameAutodiscSetShardID = "autodiscover_while_set_shard_id"
	EvtNameScuttleMkdR        = "scuttle_mark_down_r"
	EvtNameScuttleMkdW        = "scuttle_mark_down_w"
	EvtNameXKeysTxn           = "cross_keys_txn"
	EvtNameXShardsTxn         = "cross_shards_txn"
	EvtNameNoShardKey         = "shard_key_not_found"
	EvtNameBadShardKey        = "shard_key_bad_value"
	EvtNameWhitelist          = "db_whitelist"
	EvtNameShardKeyAutodisc   = "shard_key_auto_discovery"
	EvtNameBadMapping         = "bad_mapping"
)

// Shard map configuration
const (
	ShardMapRecordFlagsNotFound     = 0x0020
	ShardMapRecordFlagsBadLogical   = 0x0010
	ShardMapRecordFlagsReadStatusN  = 0x0008
	ShardMapRecordFlagsWriteStatusN = 0x0002
	ShatdMapRecordFlagsWhitelist    = 0x0001
)

// Errors returned to the client
var (
	ErrBklgTimeout,
	ErrSaturationKill,
	ErrCrossShardDML,
	ErrBadShardID,
	//  ErrShardingNotEnabled,
	ErrChangeShardIDInTxn,
	ErrScuttleMarkdownR,
	ErrScuttleMarkdownW,
	ErrBklgEviction,
	ErrRejectDbDown,
	ErrSaturationSoftSQLEviction,
	ErrBindThrottle,
	ErrBindEviction,
	ErrNoShardKey,
	ErrNoShardValue,
	ErrAutodiscoverWhileSetShardID,
	ErrNoScuttleIdPredicate,
	ErrCrossKeysDML,
	ErrOther,
	ErrReqParseFail error
)

// Initializes error strings with a prefix like "HERA"
// HERA-100, HERA-101..
func MkErr(prefix string) {
	if ErrBklgTimeout != nil {
		return // already initialized
	}
	ErrBklgTimeout = errors.New(prefix + "-100: backlog timeout")
	ErrSaturationKill = errors.New(prefix + "-101: saturation kill")
	ErrCrossShardDML = errors.New(prefix + "-200: cross shard dml")
	ErrBadShardID = errors.New(prefix + "-201: shard id out of range")
	//  ErrShardingNotEnabled               = errors.New(prefix+"-202: sharding not enabled")
	ErrChangeShardIDInTxn = errors.New(prefix + "-203: changing shard_id while in txn")
	ErrScuttleMarkdownR = errors.New(prefix + "-204: scuttle/wl markdown for read")
	ErrScuttleMarkdownW = errors.New(prefix + "-205: scuttle/wl markdown for write")
	ErrBklgEviction = errors.New(prefix + "-102: backlog eviction")
	ErrRejectDbDown = errors.New(prefix + "-103: request rejected, database down")
	ErrSaturationSoftSQLEviction = errors.New(prefix + "-104: saturation soft sql eviction")
	ErrBindThrottle = errors.New(prefix + "-105: bind throttle")
	ErrBindEviction = errors.New(prefix + "-106: bind eviction")
	ErrNoScuttleIdPredicate = errors.New(prefix + "-372: no scuttle_id predicate, please remove scuttle_id in sql")
	ErrNoShardKey = errors.New(prefix + "-373: no shard key or more than one or bad logical db")
	ErrAutodiscoverWhileSetShardID = errors.New(prefix + "-374: autodiscover while set shard id")
	ErrNoShardValue = errors.New(prefix + "-375: no shard value or wrong sharKey array binding")
	ErrCrossKeysDML = errors.New(prefix + "-206: cross key dml")
	ErrOther = errors.New(prefix + "-1000: unknown error")
	ErrReqParseFail = errors.New("Request error")
}

// Configuration entry names
const (
	ConfigMaxWorkers   = "max_connections"
	ConfigDatabaseType = "database_type"
)

type dbtype int

// Database typoe constants
const (
	Oracle dbtype = iota
	MySQL
)

// env variables to workers
const (
	envCalClientSession = "CAL_CLIENT_SESSION"
	envDbHostName       = "DB_HOSTNAME"
	envLogPrefix        = "logger.LOG_PREFIX"
	envHeraName         = "HERA_NAME"
	envTwoTask          = "TWO_TASK"
)
