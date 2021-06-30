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
	"fmt"
	"strconv"
	"strings"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

/*
 There are three APIs to deal with sharding:
 - HERA_SET_SHARD_ID / HERA_GET_NUM_SHARDS: for use cases where there is no shard key
 - ShardKey: data is <key>=<val1>;<val2>...
 - autodiscovery
*/

// ShardInfo contains the shard information. Although only shard_id is needed to route the request to the the
// appropriate worker, this structure contains the information about how the shard id was calculated (which API was used and the parameters).
// The reason for the redundant info is for logging and for catching mis-use of the sharding APIs (for example if autodiscovery is tried
// after HERA_SET_SHARD_ID)
type shardInfo struct {
	// list of shard values, deteremined via ShardKey or autodiscovery
	shardValues []string
	// list of shard map records corresponding to the values stored in shardValues
	shardRecs []*ShardMapRecord

	sessionShardID int // shard ID set manualy via HERA_SET_SHARD_ID

	shardID int // the shard id, set via one of the 3 APIs

	sqlhash int32 // the sql hash of the last query, used for logging
}

func (crd *Coordinator) copyShardInfo(dest *shardInfo, src *shardInfo) {
	dest.shardValues = src.shardValues
	dest.shardRecs = src.shardRecs
	dest.sessionShardID = src.sessionShardID
	dest.shardID = src.shardID
	dest.sqlhash = src.sqlhash
}

// Determines shard info from the shard key value. If sharding_algo is "hash" it calculates first a murmur3 hash of the key.
// Then it determines the bucket via a mod op, and after that it looks into the shard map to determine the physical shard
func (crd *Coordinator) getShardRec(key0 interface{}) *ShardMapRecord {
	var key uint64
	if GetConfig().ShardingAlgoHash {
		if GetConfig().ShardKeyValueTypeIsString {
			keyStr := key0.(string)
			//keyStr, ok := key0.(string)
			key = uint64(Murmur3([]byte(keyStr)))
		} else {
			bytes := make([]byte, 8)
			keyNum := key0.(uint64)
			//keyNum, ok := key0.(uint64)
			for i := 0; i < 8; i++ {
				bytes[i] = byte(keyNum & 0xFF)
				keyNum >>= 8
			}
			key = uint64(Murmur3(bytes))
		}
	} else {
		key = key0.(uint64)
	}
	bucket := key % uint64(GetConfig().MaxScuttleBuckets)
	shardRec := GetShardingCfg().records[bucket]
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, crd.id, "Sharding map lookup: hash =", key, ", bucket =", bucket, ", shardID =", shardRec.logical)
	}
	return shardRec
}

/**
processSetShardId handles HERA_SET_SHARD_ID command from the client to set the shard ID to be used for the following requests.
The shard ID remains set until is reset via this command with shard id equal -1.
HERA_SET_SHARD_ID is used for the rare cases when a table is not sharded. In fact if later a query is attempted on a sharded table,
the query will fail - that way catching a bug in the client application which would otherwise break the database integrity.
- If the sharding is disabled, set shard id to 0 (first shard) or -1 (reset) is allowed, noop, oterwise it returns error
- If called while in a transaction, it will return an error if it tries to change shard
- If white list is enabled or no shard map then the shard id is reset (i.e. -1) then it is forced to 0
*/
func (crd *Coordinator) processSetShardID(val []byte) error {
	sh, err := strconv.ParseInt(string(val), 10, 32)
	// allow set shard id 0 or -1 (i.e. reset) if sharding disabled
	if (!(GetConfig().EnableSharding)) && (sh != 0) && (sh != -1) {
		return ErrBadShardID
	}
	if (err != nil) || (int(sh) < -1) || (int(sh) >= GetConfig().NumOfShards) {
		evt := cal.NewCalEvent(EvtTypeSharding, EvtNameBadShardID, cal.TransOK, "")
		evt.AddDataStr("shard_id", string(val))
		evt.Completed()
		return ErrBadShardID
	}
	crd.shard.sessionShardID = int(sh)
	if crd.inTransaction && (crd.worker != nil) {
		// in transaction
		if crd.worker.shardID != crd.shard.sessionShardID {
			evt := cal.NewCalEvent(EvtTypeSharding, EvtNameSetShardIDInTxn, cal.TransOK, "")
			evt.AddDataInt("txn_shard_id", int64(crd.worker.shardID))
			evt.AddDataStr("requested_shard_id", string(val))
			evt.Completed()
			return ErrChangeShardIDInTxn
		}
	}
	crd.shard.shardID = crd.shard.sessionShardID
	if (GetConfig().EnableWhitelistTest || (!(GetConfig().EnableSharding))) && (crd.shard.shardID == -1) {
		crd.shard.shardID = 0
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, crd.id, "Shard ID reset to 0")
		}
	} else {
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, crd.id, "Shard ID forced to", crd.shard.shardID)
		}
	}
	return nil
}

// Parses the payload of the ShardKey command to get the shard key values. The shard key values are in the form
// <key>=<val1>;<val2>;...;<valn>. It returns the key name as well as the array of values
func (crd *Coordinator) parseShardKey(val []byte) (string, []string) {
	var key string
	var values []string
	sz := len(val)
	i := 0
	for ; i < sz; i++ {
		if val[i] == '=' {
			key = string(val[:i])
			break
		}
	}
	i++
	bf := make([]byte, 64)[:0]
	escape := false
	for ; i < sz; i++ {
		if escape {
			escape = false
		} else {
			if val[i] == '\\' {
				escape = true
				continue
			} else {
				if val[i] == ';' {
					values = append(values, string(bf))
					bf = bf[:0]
					continue
				}
			}
		}
		bf = append(bf, val[i])
	}
	if len(bf) > 0 {
		values = append(values, string(bf))
	}
	return strings.ToLower(key), values
}

// Compute the logical shards from the shard key values, first looking in the whitelist before looking in the shard map
func (crd *Coordinator) computeLogicalShards() {
	crd.shard.shardRecs = crd.shard.shardRecs[:0]
	// TODO whitelist
	for _, rec := range crd.shard.shardValues {
		if len(rec) == 0 && GetConfig().EnableWhitelistTest {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, crd.id, "null shard key value with whitelist enable, defaulting to shard 0")
			}
			crd.shard.shardRecs = append(crd.shard.shardRecs, &ShardMapRecord{logical: 0})
			/* the sh0 default here is ok
			since sharding isn't on at this point and
			queries WHERE x_shard_key IS NULL don't have the binds
			which would make them sharding complaint.
			this is allows old data methods to work
			while new data methods start populating shard key
			and a db cutover populates old data and
			the new db needs populated shard key to keep queries fast.
			only once on the new db, can the standard app sharding work begin */
			break
		}
		// filter only the numeric part of the ShardValue
		var key interface{}
		if GetConfig().ShardKeyValueTypeIsString {
			key = rec
		} else {
			key, _ = atoui(rec)
		}
		var wlcfg *WLCfg
		if GetConfig().EnableWhitelistTest || !GetConfig().UseShardMap {
			if len(crd.shard.shardRecs) == 1 {
				// we log it and accept this
				evt := cal.NewCalEvent(EvtTypeSharding, EvtNameMultiShard, cal.TransOK, "")
				evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
				evt.Completed()
				crd.shard.shardValues = crd.shard.shardValues[:1] /* is this right? probably should process all shardValues */
				break
			}
			wlcfg = GetWLCfg()
		}
		if wlcfg != nil {
			shardRec, ok := wlcfg.records[key]
			if ok {
				evt := cal.NewCalEvent(EvtTypeSharding, EvtNameWhitelist, cal.TransOK, "")
				evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
				evt.AddDataStr("shard_key", rec)
				evt.AddDataInt("logical_shard_id", int64(shardRec.logical))
				evt.Completed()
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, crd.id, "Sharding Key =", rec, "found in whitelist, shard =", shardRec.logical)
				}
				// check if WL entry is valid
				// TODO: is this OK, should we fallback or error instead?
				if (shardRec.logical >= 0) && (shardRec.logical < GetConfig().NumOfShards) {
					crd.shard.shardRecs = append(crd.shard.shardRecs, shardRec)
				} else {
					// fallback to check the shard map
					crd.shard.shardRecs = append(crd.shard.shardRecs, crd.getShardRec(key))
				}
			} else {
				// not in the WL, fallback to check the shard map
				crd.shard.shardRecs = append(crd.shard.shardRecs, crd.getShardRec(key))
			}
		} else {
			crd.shard.shardRecs = append(crd.shard.shardRecs, crd.getShardRec(key))
		}
	}
	if len(crd.shard.shardRecs) > 0 {
		crd.shard.shardID = crd.shard.shardRecs[0].logical
	} else {
		// empty out ivalid shadr values
		crd.shard.shardValues = crd.shard.shardValues[:0]
	}
}

// compare case-insensitive to see if this is a shard key
// also, if it is in a format from IN clause <shard key>_<number>
func (crd *Coordinator) isShardKey(bind string) bool {
	if len(bind) == 0 {
		return false
	}
	if bind[0] == ':' {
		bind = bind[1:]
	}
	skey := GetConfig().ShardKeyName
	lbind := len(bind)
	lskey := len(skey)
	if lbind < lskey {
		return false
	}
	if strings.ToLower(bind[:lskey]) != skey {
		return false
	}
	if lbind == lskey {
		return true
	}
	// look for _<number>
	if bind[lskey] != '_' {
		return false
	}
	bind = bind[lskey+1:]
	for _, ch := range bind {
		if (ch < '0') || (ch > '9') {
			return false
		}
	}
	return true
}

// PreprocessSharding is doing shard info calculation and validation checks (by calling verifyValidShard)
// before determining if the current request should continue, returning nil error if the request should be allowed.
// If error is not nil, the second parameter says if the coordinator should hangup the client connection.
// The decision to hang-up or not in case of error is based on backward compatibility
func (crd *Coordinator) PreprocessSharding(requests []*netstring.Netstring) (bool, error) {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "PreprocessSharding:", crd.shard)
	}
	if crd.inTransaction {
		crd.copyShardInfo(crd.prevShard, crd.shard)
		crd.shard.shardValues = make([]string, 0, 1)
		crd.shard.shardRecs = make([]*ShardMapRecord, 0, 1)
	} else {
		if len(crd.shard.shardRecs) > 0 {
			crd.shard.shardRecs = crd.shard.shardRecs[:0]
		}
		if len(crd.shard.shardValues) > 0 {
			crd.shard.shardValues = crd.shard.shardValues[:0]
		}
		// TODO: why is this needed
		crd.prevShard.sessionShardID = crd.shard.sessionShardID
	}

	sz := len(requests)
	autodisc := false /* ShardKey can overwrite the autodiscovery */
	for i := 0; i < sz; i++ {
		if requests[i].Cmd == common.CmdPrepare {
			lowerSql := strings.ToLower(string(requests[i].Payload))
			scuttle_idx := strings.LastIndex(lowerSql, strings.ToLower(GetConfig().ScuttleColName))
			if scuttle_idx < 0 || scuttle_idx > strings.Index(lowerSql, " from ") {
				continue
			}
			evt := cal.NewCalEvent(EvtTypeSharding, "RM_SCUTTLE_ID_FETCH_COL", cal.TransOK, "")
			evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
			evt.Completed()
			ns := netstring.NewNetstringFrom(common.RcError, []byte(ErrNoScuttleIdPredicate.Error()))
			crd.respond(ns.Serialized)
			return true, ErrNoScuttleIdPredicate
		}
		if (requests[i].Cmd == common.CmdBindName) && crd.isShardKey(string(requests[i].Payload)) {
			if crd.shard.sessionShardID != -1 {
				evt := cal.NewCalEvent(EvtTypeSharding, EvtNameAutodiscSetShardID, cal.TransOK, "")
				evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
				evt.Completed()
				ns := netstring.NewNetstringFrom(common.RcError, []byte(ErrAutodiscoverWhileSetShardID.Error()))
				crd.respond(ns.Serialized)
				return true, ErrAutodiscoverWhileSetShardID
			}
			if i < (sz - 1) {
				if !autodisc {
					crd.shard = &shardInfo{sessionShardID: crd.prevShard.sessionShardID}
				}
				if requests[i+1].Cmd == common.CmdBindNum && requests[i+2].Cmd == common.CmdBindValueMaxSize {
					crd.shard.shardValues = append(crd.shard.shardValues, string(requests[i+3].Payload))
				} else if requests[i+1].Cmd == common.CmdBindValue {
					crd.shard.shardValues = append(crd.shard.shardValues, string(requests[i+1].Payload))
				} else {

					// TODO: Need to rework on error statememt & CAL event type
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, crd.id, "req rejected, no shard value:", len(crd.shard.shardValues))
					}
					evt := cal.NewCalEvent(EvtTypeSharding, EvtNameBadShardKey, cal.TransOK, "")
					evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
					evt.Completed()
					ns := netstring.NewNetstringFrom(common.RcError, []byte(ErrNoShardValue.Error()))
					crd.respond(ns.Serialized)
					return false /*don't hangup*/, ErrNoShardValue

				}
				autodisc = true
			}
		} else {
			if requests[i].Cmd == common.CmdShardKey {
				if crd.shard.sessionShardID != -1 {
					// HERA_SET_SHARD_ID and ShardKey not allowed simultaneously
					evt := cal.NewCalEvent(EvtTypeSharding, EvtNameShardIDAndKey, cal.TransOK, "")
					evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
					evt.Completed()
					return true, errors.New("Unsupported both HERA_SET_SHARD_ID and ShardKey")
				}

				key, vals := crd.parseShardKey(requests[i].Payload)

				if key != GetConfig().ShardKeyName {
					// not primary shard key, not supported
					evt := cal.NewCalEvent(EvtTypeSharding, EvtNameUnkKey, cal.TransOK, "")
					evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
					evt.AddDataStr("key_name", key)
					evt.Completed()
					if !GetConfig().EnableWhitelistTest && GetConfig().UseShardMap {
						if logger.GetLogger().V(logger.Verbose) {
							logger.GetLogger().Log(logger.Verbose, crd.id, "req rejected, no shard key:", len(crd.shard.shardValues))
						}
						evt := cal.NewCalEvent(EvtTypeSharding, EvtNameNoShardKey, cal.TransOK, "")
						evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
						evt.Completed()
						ns := netstring.NewNetstringFrom(common.RcError, []byte(ErrNoShardKey.Error()))
						crd.respond(ns.Serialized)
						return false /*don't hangup*/, ErrNoShardKey
					}
				} else {
					crd.shard.shardValues = vals
				}
				crd.shard.sqlhash = crd.sqlhash
				crd.computeLogicalShards()
				if len(crd.shard.shardRecs) > 1 {
					evt := cal.NewCalEvent(EvtTypeSharding, EvtNameMultiShard, cal.TransOK, "")
					evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
					evt.Completed()
				}
				if logger.GetLogger().V(logger.Verbose) {
					if autodisc {
						logger.GetLogger().Log(logger.Verbose, crd.id, "Auto discovery superceeded by ShardKey")
					} else {
						logger.GetLogger().Log(logger.Verbose, crd.id, "Processed ShardKey")
					}
				}
				autodisc = false

				break
			}
		}
	}

	if autodisc {
		crd.computeLogicalShards()
		crd.shard.sqlhash = crd.sqlhash

		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, fmt.Sprintf("shard info auto discovery: key_name=%s, num_values=%d", GetConfig().ShardKeyName, len(crd.shard.shardValues)))
		}

		if len(crd.shard.shardValues) > 0 {
			// shard_key_auto_discovery
			evt := cal.NewCalEvent(EvtTypeSharding, EvtNameShardKeyAutodisc, cal.TransOK, "")
			evt.AddDataStr("shardkey", GetConfig().ShardKeyName+"|"+crd.shard.shardValues[0])
			evt.AddDataInt("shardid", int64(crd.shard.shardID))
			if len(crd.shard.shardRecs) > 0 {
				evt.AddDataInt("scuttleid", int64(crd.shard.shardRecs[0].bin))
				evt.AddDataInt("flags", int64(crd.shard.shardRecs[0].flags))
			}
			evt.AddDataInt("sqlhash", int64(uint32(crd.sqlhash)))
			evt.Completed()
		}
	}

	if (len(crd.shard.shardValues) == 0) && (crd.shard.sessionShardID == -1) {
		if GetConfig().EnableWhitelistTest || !GetConfig().UseShardMap {
			shardRec := &ShardMapRecord{logical: 0}
			crd.shard.shardRecs = []*ShardMapRecord{shardRec}
			crd.shard.shardID = 0
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, crd.id, "Sharding whitelist enabled or no shard map, defaulting to shard 0")
			}
			evt := cal.NewCalEvent(EvtTypeSharding, EvtNameNoShardKey, cal.TransOK, "")
			evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
			evt.Completed()
		}
	}

	hangup, err := crd.verifyValidShard()
	if err != nil {
		return hangup, err
	}

	if crd.inTransaction {
		err = crd.verifyXShard(crd.prevShard.shardValues, crd.prevShard.shardID, crd.prevShard.sqlhash)
		if err != nil {
			return true, err
		}
	}
	return false, nil
}

// verifyValidShard verifies if the shard info is valid, returning nil if fine. If error is not nil, the second parameter says if it should hangup.
func (crd *Coordinator) verifyValidShard() (bool, error) {
	if ((len(crd.shard.shardValues) > 0) && ((crd.shard.shardRecs[0].flags & ShardMapRecordFlagsBadLogical) != 0)) ||
		((len(crd.shard.shardValues) > 0) && (crd.shard.shardRecs[0].logical >= GetConfig().NumOfShards)) {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "req rejected, no shard key, or multishard, or bad logical:", len(crd.shard.shardValues))
		}
		evt := cal.NewCalEvent(EvtTypeSharding, EvtNameBadMapping, cal.TransOK, "")
		evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
		evt.AddDataStr("shard_key", crd.shard.shardValues[0])
		evt.AddDataInt("logical_shard_id", int64(crd.shard.shardRecs[0].logical))
		evt.Completed()

		if GetConfig().EnableWhitelistTest {
			crd.shard.shardRecs[0] = &ShardMapRecord{logical: 0}
			crd.shard.shardID = 0
		} else {
			ns := netstring.NewNetstringFrom(common.RcError, []byte(ErrNoShardKey.Error()))
			crd.respond(ns.Serialized)
			hangup := ((len(crd.shard.shardValues) > 0) && ((crd.shard.shardRecs[0].flags & ShardMapRecordFlagsBadLogical) != 0))
			return hangup, ErrNoShardKey
		}
	}

	if (len(crd.shard.shardValues) > 1) /*multiple keys*/ ||
		((len(crd.shard.shardValues) == 0) && (crd.shard.sessionShardID == -1) && (len(crd.shard.shardRecs) == 0)) /*no keys*/ {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "req rejected, no shard key, or multishard, or bad logical:", len(crd.shard.shardValues))
		}
		evt := cal.NewCalEvent(EvtTypeSharding, EvtNameNoShardKey, cal.TransOK, "")
		evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
		evt.Completed()

		ns := netstring.NewNetstringFrom(common.RcError, []byte(ErrNoShardKey.Error()))
		crd.respond(ns.Serialized)
		hangup := ((len(crd.shard.shardValues) > 0) && ((crd.shard.shardRecs[0].flags & ShardMapRecordFlagsBadLogical) != 0))
		return hangup, ErrNoShardKey
	}

	if len(crd.shard.shardRecs) > 0 {
		if crd.isRead {
			if (crd.shard.shardRecs[0].flags & ShardMapRecordFlagsReadStatusN) != 0 {
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, crd.id, "req rejected, scuttle is marked down for reading")
				}
				evt := cal.NewCalEvent(EvtTypeSharding, EvtNameScuttleMkdR, cal.TransOK, "")
				evt.AddDataInt("scuttle_id", int64(crd.shard.shardRecs[0].bin))
				evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
				evt.Completed()
				ns := netstring.NewNetstringFrom(common.RcError, []byte(ErrScuttleMarkdownR.Error()))
				crd.respond(ns.Serialized)
				return true, ErrScuttleMarkdownR
			}
		} else {
			if crd.shard.shardRecs[0].flags&ShardMapRecordFlagsWriteStatusN != 0 {
				evt := cal.NewCalEvent(EvtTypeSharding, EvtNameScuttleMkdW, cal.TransOK, "")
				evt.AddDataInt("scuttle_id", int64(crd.shard.shardRecs[0].bin))
				evt.AddDataInt("sql", int64(uint32(crd.sqlhash)))
				evt.Completed()
				ns := netstring.NewNetstringFrom(common.RcError, []byte(ErrScuttleMarkdownW.Error()))
				crd.respond(ns.Serialized)
				return true, ErrScuttleMarkdownW
			}
		}
	}
	return false, nil
}

// verifyXShard checks if the client attempt to run a request on a different shard, while being on a (
// transaction (i.e. already using a worker from the current shard)
func (crd *Coordinator) verifyXShard(oldShardValues []string, oldShardID int, oldSQLhash int32) error {
	if crd.isRead {
		if oldShardID != crd.shard.shardID {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "Verify X shard, old =", oldShardID, ", new shard =", crd.shard.shardID)
			}
		}
	} else {
		if (len(oldShardValues) > 0) && (len(crd.shard.shardValues) > 0) {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "two dmls have different shard_keys in same transactions")
			}

			if oldShardValues[0] != crd.shard.shardValues[0] {
				evt := cal.NewCalEvent(EvtTypeSharding, EvtNameXKeysTxn, cal.TransOK, "")
				evt.AddDataStr("shard_key1", oldShardValues[0])
				evt.AddDataStr("shard_key2", crd.shard.shardValues[0])
				evt.AddDataInt("sql1", int64(uint32(oldSQLhash)))
				evt.AddDataInt("sql2", int64(uint32(crd.sqlhash)))
				evt.AddDataStr("raddr", crd.conn.RemoteAddr().String())
				if crd.corrID != nil {
					evt.AddDataStr("corr_id", string(crd.corrID.Payload))
				}
				evt.Completed()
				if GetConfig().ShardingCrossKeysErr {
					ns := netstring.NewNetstringFrom(common.RcError, []byte(ErrCrossKeysDML.Error()))
					crd.respond(ns.Serialized)
					return ErrCrossKeysDML
				}
			}
		}
		if (len(crd.shard.shardRecs) > 0) && (oldShardID != crd.shard.shardRecs[0].logical) {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "two dmls in different shards in same transactions")
			}
			evt := cal.NewCalEvent(EvtTypeSharding, EvtNameXShardsTxn, cal.TransOK, "")
			evt.AddDataInt("shard1", int64(oldShardID))
			evt.AddDataInt("shard2", int64(crd.shard.shardRecs[0].logical))
			evt.AddDataInt("sql1", int64(uint32(oldSQLhash)))
			evt.AddDataInt("sql2", int64(uint32(crd.sqlhash)))
			if crd.corrID != nil {
				evt.AddDataStr("corr_id", string(crd.corrID.Payload))
			}
			evt.Completed()
			ns := netstring.NewNetstringFrom(common.RcError, []byte(ErrCrossShardDML.Error()))
			crd.respond(ns.Serialized)
			return ErrCrossShardDML
		}
	}
	return nil
}
