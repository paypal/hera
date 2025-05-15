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
	// "time"
	"context"
	"fmt"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
	"math/rand"
	"strconv"
	"strings"
)

type CacheInfo struct {
	key       string // The original key string used to generate the hash
	ttl       uint32 // Time-to-live for the cache entry
	operation string // Operation type (e.g., "GET", "SET")
	cacheByCorrId bool
	isShadowTest bool
}

func parseRequest(request *netstring.Netstring) (hasPrepare bool, hasExec bool, hasFetch bool, parseErr error) {
	foundPrepare := false
	foundExec := false
	foundFetch := false
	if request == nil {
		return false, false, false, ErrCacheBadRequest
	}
	if request.IsComposite() {
		nss, err := netstring.SubNetstrings(request)
		if err != nil {
			return false, false, false, err
		}
		for _, ns := range nss {
			if (ns.Cmd == common.CmdPrepare) || (ns.Cmd == common.CmdPrepareV2) || (ns.Cmd == common.CmdPrepareSpecial) {
				foundPrepare = true
			} else if ns.Cmd == common.CmdExecute {
				foundExec = true
			} else if ns.Cmd == common.CmdFetch {
				foundFetch = true
			}
		}
		return foundPrepare, foundExec, foundFetch, nil
	} else {
		ns := request
		if (ns.Cmd == common.CmdPrepare) || (ns.Cmd == common.CmdPrepareV2) || (ns.Cmd == common.CmdPrepareSpecial) {
			return true, false, false, nil
		} else if ns.Cmd == common.CmdExecute {
			return false, true, false, nil
		} else if ns.Cmd == common.CmdFetch {
			return false, false, true, nil
		}
	}
	return false, false, false, nil
}

// getKey is a utility to construct the cache key based on a request
func getKey(request *netstring.Netstring, corrId string, sqlHash int32, cacheByCorrId bool) ([]byte, string, error) {
	dice := rand.Intn(GetConfig().numCalThreads)
	calThreadGroupName := cal.DefaultTGName + strconv.Itoa(dice)
	// Check if the request is valid for caching
	hasPrepare, hasExec, hasFetch, err := parseRequest(request)
	if err != nil {
		evt := cal.NewCalEvent("getKeyErr", "ErrParseRequest", cal.TransWarning, "", calThreadGroupName)
		evt.AddDataStr("corr_id_", corrId)
		evt.AddDataStr("err", err.Error())
		evt.Completed()
		return nil, "", ErrCacheBadRequest
	}
	if hasPrepare && hasExec && hasFetch {
		var key string
		if cacheByCorrId {
			// Return err if corrid is NotSet
			if corrId == "NotSet" || corrId == "" || corrId == "unset" {
				evt := cal.NewCalEvent("getKeyErr", "ErrCacheCorridNotSet", cal.TransWarning, "", calThreadGroupName)
				evt.AddDataStr("corr_id_", corrId)
				evt.Completed()
				return nil, "", ErrCacheCorridNotSet
			}
			key += corrId + "|"
		}
		reqAfterCorrId := ""
		if request != nil {
			// CorrId
			str := string(request.Payload)
			pos := strings.LastIndex(str, "CorrId=")
			if pos != -1 {
				tmpStr := str[pos:]
				end := strings.Index(tmpStr, ",")
				if end != -1 {
					reqAfterCorrId = tmpStr[end+1:]
				}
			} else {
				reqAfterCorrId = str
			}
			logger.GetLogger().Log(logger.Verbose, "reqAfterCorrId:", reqAfterCorrId)
			if len(reqAfterCorrId) > 0 {
				key += reqAfterCorrId + "|"
			}
		}
		sqlhashStr := fmt.Sprintf("%d", uint32(sqlHash))
		logger.GetLogger().Log(logger.Verbose, "SQLHash:", sqlhashStr)
		key += sqlhashStr + "|"
		binds := parseBinds(request)
		var concatKey string
		for bindName, bindValue := range binds {
			concatKey += fmt.Sprintf("%s^%s|", bindName, bindValue)
		}
		logger.GetLogger().Log(logger.Verbose, "Binds after parsing:", concatKey)
		key += concatKey
		poolName := cal.GetCalClientInstance().GetPoolName()
		key += poolName
		logger.GetLogger().Log(logger.Verbose, "key inside getKey:", key)
		keyHash := utility.GetFNV128a(key)
		return keyHash, key, nil
	}
	evt := cal.NewCalEvent("getKeyErr", "ErrCacheReqNotSupported", cal.TransWarning, "", calThreadGroupName)
	evt.AddDataStr("corr_id_", corrId)
	evt.Completed()
	return nil, "", ErrCacheReqNotSupported
}

// setRecordToCache tries to write the data to cache
func setRecordToCache(request *netstring.Netstring, crdResponse string, ttl uint32, corrId string, sqlHash int32, cacheByCorrId bool, isClientControlledCachingRequest bool, cacheKey string) {
	cli, _ := GetJunoClient()
	var keyHashStr string
	logger.GetLogger().Log(logger.Verbose, "SET junoClientReady:", cli.junoClientReady)
	if cli.junoClientReady && cli != nil {
		dice := rand.Intn(GetConfig().numCalThreads)
		calThreadGroupName := cal.DefaultTGName + strconv.Itoa(dice)
		if isClientControlledCachingRequest {
			keyHashStr = cacheKey // Use the key from the client request
			logger.GetLogger().Log(logger.Verbose, "Trying SET with key:", keyHashStr, "value:", crdResponse)
		} else {
			keyHash, key, keyerr := getKey(request, corrId, sqlHash, cacheByCorrId)
			if keyerr != nil {
				evt := cal.NewCalEvent("setRecordToCache", "getKeyErr", cal.TransWarning, "", calThreadGroupName)
				evt.AddDataStr("corr_id_", corrId)
				evt.AddDataStr("sqlHash", fmt.Sprintf("%d", uint32(sqlHash)))
				evt.AddDataStr("err", keyerr.Error())
				evt.Completed()
				return
			}
			keyHashStr = fmt.Sprintf("%x", keyHash)
			logger.GetLogger().Log(logger.Verbose, "Trying SET with key:", keyHashStr, "value:", crdResponse)
			logger.GetLogger().Log(logger.Verbose, "junoKeyHash:", keyHashStr, "junoKey:", key)
		}
		caltxn := cal.NewCalAtomicTransaction("SET", fmt.Sprintf("%d", uint32(sqlHash)), "0", "", calThreadGroupName)
		caltxn.AddDataStr("corr_id_", corrId)
		err := cli.Set([]byte(keyHashStr), []byte(crdResponse), ttl, corrId, calThreadGroupName)
		caltxn.AddDataStr("junoKeyHash", keyHashStr)
		caltxn.AddDataInt("keySize:", int64(len([]byte(keyHashStr))))
		caltxn.AddDataInt("crdResponseSize", int64(len([]byte(crdResponse))))
		if err != nil {
			logger.GetLogger().Log(logger.Verbose, "Error in setRecordToCache:", err)
			caltxn.SetStatus("2")
			caltxn.AddDataStr("err", err.Error())
			caltxn.Completed()
			return
		}
		caltxn.Completed()
		return
	} else {
		logger.GetLogger().Log(logger.Alert, "GetJunoClient returned nil...")
		return
	}
}


func (crd *Coordinator) PreprocessCaching(request *netstring.Netstring) (bool, error) {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "PreprocessCaching: starting", string(request.Payload))
	}
	defer func() {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "PreprocessCaching: exiting")
		}
	}()
	dice := rand.Intn(GetConfig().numCalThreads)
	calThreadGroupName := cal.DefaultTGName + strconv.Itoa(dice)
	if request == nil {
		return false, ErrCacheBadRequest
	}
	hasPrepare, hasExec, hasFetch, err := parseRequest(request)
	if err != nil {
		evt := cal.NewCalEvent("PreprocessCaching", "ErrParseRequest", cal.TransWarning, "", calThreadGroupName)
		evt.AddDataStr("corr_id_", crd.extractedcorrId)
		evt.AddDataStr("err", err.Error())
		evt.Completed()
		return false, ErrCacheBadRequest
	}
	if !hasPrepare || !hasExec || !hasFetch {
		evt := cal.NewCalEvent("PreprocessCaching", "ErrCacheReqNotSupported", cal.TransWarning, "", calThreadGroupName)
		evt.AddDataStr("corr_id_", crd.extractedcorrId)
		evt.Completed()
		return false, ErrCacheReqNotSupported
	}
	foundKey := false
	foundTTL := false
	foundCacheOp := false
	var nss []*netstring.Netstring
	if request.IsComposite() {
		if crd.nss == nil {
			nss, err = netstring.SubNetstrings(request)
			if err != nil {
				return false, fmt.Errorf("error parsing sub-netstrings: %w", err)
			}
			crd.nss = nss
		} else {
			nss = crd.nss
		}
	} else {
		nss = []*netstring.Netstring{request}
	}
	for _, ns := range nss {
		switch ns.Cmd {
		case common.CmdCacheKey:
			foundKey = true
			crd.cacheInfo.key = string(ns.Payload)
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "Found CmdCacheKey:", crd.cacheInfo.key)
			}
			if len(crd.cacheInfo.key) == 0 {
				return false, ErrCacheKeyNotSet
			}
		case common.CmdCacheTTL:
			foundTTL = true
			ttl, err := strconv.ParseUint(string(ns.Payload), 10, 32)
			if err != nil {
				return false, fmt.Errorf("error parsing CmdCacheTTL: %w", err)
			}
			crd.cacheInfo.ttl = uint32(ttl)
			if crd.cacheInfo.ttl == 0 {
				return false, ErrCacheInvalidTTL
			}
		case common.CmdCacheOp:
			foundCacheOp = true
			crd.cacheInfo.operation = string(ns.Payload)
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "Found CmdCacheOp:", crd.cacheInfo.operation)
			}
			if crd.cacheInfo.operation != "GET" {
				return false, ErrCacheInvalidOperation
			}
		}
	}
	if foundKey && foundCacheOp && foundTTL {
		return true, nil
	} else {
		evt := cal.NewCalEvent("PreprocessCaching", "ErrCacheReqNotSupported", cal.TransWarning, "", calThreadGroupName)
		evt.AddDataStr("corr_id_", crd.extractedcorrId)
		evt.Completed()
		return false, ErrCacheReqNotSupported
	}
}

// getRecordFromCache tries to fetch data from cache. It responds to the client if the lookup is successful. If not, it returns the error.
func (crd *Coordinator) getRecordFromCache(request *netstring.Netstring, respExit <-chan error, shadowTest bool, cacheByCorrId bool) error {
	cli, _ := GetJunoClient()
	logger.GetLogger().Log(logger.Verbose, "GET junoClientReady:", cli.junoClientReady)
	var keyHashStr string
	if cli.junoClientReady && cli != nil {
		dice := rand.Intn(GetConfig().numCalThreads)
		calThreadGroupName := cal.DefaultTGName + strconv.Itoa(dice)
		if crd.isClientControlledCachingRequest {
			keyHashStr = crd.cacheInfo.key // Use the key from the client request
			logger.GetLogger().Log(logger.Verbose, "Trying GET with key:", keyHashStr)
		} else {
			keyHash, key, keyerr := getKey(request, crd.extractedcorrId, crd.sqlhash, cacheByCorrId)
			if keyerr != nil {
				evt := cal.NewCalEvent("getRecordFromCache", "getKeyErr", cal.TransWarning, "", calThreadGroupName)
				evt.AddDataStr("corr_id_", crd.extractedcorrId)
				evt.AddDataStr("sqlHash", fmt.Sprintf("%d", uint32(crd.sqlhash)))
				evt.AddDataStr("clientApp", crd.poolName)
				evt.AddDataStr("err", keyerr.Error())
				evt.Completed()
				return keyerr
			}
			keyHashStr = fmt.Sprintf("%x", keyHash)
			logger.GetLogger().Log(logger.Verbose, "Trying GET with key:", keyHashStr)
			logger.GetLogger().Log(logger.Verbose, "junoKeyHash:", keyHashStr, "junoKey:", key)
		}
		caltxn := cal.NewCalAtomicTransaction("GET", fmt.Sprintf("%d", uint32(crd.sqlhash)), "0", "", calThreadGroupName)
		caltxn.AddDataStr("corr_id_", crd.extractedcorrId)
		resp, err := cli.Get([]byte(keyHashStr), crd.extractedcorrId, calThreadGroupName)
		caltxn.AddDataStr("junoKeyHash:", keyHashStr)
		caltxn.AddDataInt("keySize:", int64(len([]byte(keyHashStr))))
		if err != nil {
			logger.GetLogger().Log(logger.Verbose, "Error in getRecordFromCache:", err)
			caltxn.SetStatus("2")
			caltxn.AddDataStr("err", err.Error())
			caltxn.Completed()
			return err
		}
		logger.GetLogger().Log(logger.Verbose, "Response from cache...", string(resp))
		caltxn.AddDataInt("cacheResponseSize", int64(len(resp)))
		select {
		case err := <-respExit:
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "getRecordFromCache: received err:", err)
			}
			caltxn.SetStatus("3")
			caltxn.AddDataStr("err", err.Error())
			caltxn.Completed()
			return err
		default:
			if len(string(resp)) > 0 {
				if shadowTest {
					caltxn.SetStatus("shadowTestEnabled")
					caltxn.AddDataStr("resp", ErrCacheShadowTest.Error())
					caltxn.Completed()
					logger.GetLogger().Log(logger.Debug, crd.id, "Not responding to the client from cache...ShadowTestEnabled:", shadowTest)
					evt := cal.NewCalEvent("getRecordFromCache", "shadowTest", cal.TransOK, "", calThreadGroupName)
					evt.AddDataStr("resp", ErrCacheShadowTest.Error())
					evt.Completed()
					return ErrCacheShadowTest
				}
				splits := strings.Split(string(resp), CacheSeparator)
				noMoreData := "1:6,"
				lastSplit := splits[len(splits)-2]
				if lastSplit != noMoreData {
					caltxn.SetStatus("skipCacheResponse")
					caltxn.AddDataStr("lastSplit", lastSplit)
					caltxn.AddDataStr("resp", ErrCacheSkipResponse.Error())
					caltxn.Completed()
					logger.GetLogger().Log(logger.Debug, crd.id, "Not responding to the client from cache...Resp did not contain RcNoMoreData. Found:", lastSplit)
					evt := cal.NewCalEvent("dispatchRequest", "cache_response_not_sent", cal.TransOK, "", calThreadGroupName)
					evt.AddDataStr("resp", ErrCacheSkipResponse.Error())
					evt.AddDataStr("lastSplit", lastSplit)
					evt.Completed()
					return ErrCacheSkipResponse
				}
				for idx, split := range splits {
					if len(split) > 0 {
						logger.GetLogger().Log(logger.Debug, crd.id, "Responding to client...")
						if crd.sendResponseMetadata && idx == len(splits)-2 {
							logger.GetLogger().Log(logger.Debug, "Before ResponseMetadata:", split)
							if split == noMoreData {
								// CmdServerRespondedFromCache = 1020
								ns := netstring.NewNetstringFrom(common.RcNoMoreData, []byte(fmt.Sprintf("%d",common.CmdServerRespondedFromCache)))
								split = string(ns.Serialized)
							}
							logger.GetLogger().Log(logger.Debug, "After ResponseMetadata:", split)
						}
						err := crd.respond([]byte(split))
						// _, err = crd.conn.Write([]byte(split))
						caltxn.AddDataInt(fmt.Sprintf("%d", idx), int64(len([]byte(split))))
						if err != nil {
							caltxn.SetStatus("ErrCacheClientWriteFailed")
							caltxn.AddDataStr("err", ErrCacheClientWriteFailed.Error())
							caltxn.Completed()
							logger.GetLogger().Log(logger.Debug, crd.id, "Failed to reply to client")
							evt := cal.NewCalEvent("getRecordFromCache", "client_write_failed", cal.TransWarning, "", calThreadGroupName)
							evt.AddDataStr("err", err.Error())
							evt.Completed()
							return ErrCacheClientWriteFailed
						}
					}
				}
			}
			caltxn.Completed()
			return err
		}
	} else {
		return fmt.Errorf("GetJunoClient returned nil...")
	}
}

// doCacheRequest tries to fetch the data from cache. It also monitors the client channel for timeouts, request cancellations.
func (crd *Coordinator) doCacheRequest(ctx context.Context, request *netstring.Netstring, enableShadowTest bool, enableCacheByCorrId bool) error {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator doCacheRequest: starting")
	}

	defer func() {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator doCacheRequest: exiting")
		}
	}()

	errors := make(chan error, 1)
	quit := make(chan bool, 1)
	safeExit := make(chan bool, 1)
	respExit := make(chan error, 1)
	clientChannel := crd.clientchannel
	done := ctx.Done()

	go func() {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "doCacheRequest: routine starting")
		}
		defer func() {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "doCacheRequest: routine exiting")
			}
		}()
		for {
			select {
			case ns, ok := <-clientChannel:
				if !ok {
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, crd.id, "doCacheRequest: client channel closed")
					}
					evt := cal.NewCalEvent("doCacheRequest", "client_closed", cal.TransOK, "")
					evt.Completed()
					quit <- true
					respExit <- ErrCacheClientClosed
					errors <- ErrCacheClientClosed
					return
				} else {
					if ns != nil {
						// abort the client conn?
						// This is a protocol/internal error. Cannot support multiple netstrings
						if logger.GetLogger().V(logger.Warning) {
							logger.GetLogger().Log(logger.Warning, crd.id, "doCacheRequest: multiple client req", DebugString(ns.Serialized))
						}
						evt := cal.NewCalEvent("doCacheRequest", "multiple_client_req", cal.TransWarning, fmt.Sprintf("cmd=%s", DebugString(ns.Serialized)))
						evt.Completed()
					}
					quit <- true
					respExit <- ErrCacheMultipleClientReq
					errors <- ErrCacheMultipleClientReq
					return
				}
			case <-done:
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, crd.id, "doCacheRequest: request canceled")
				}
				evt := cal.NewCalEvent("doCacheRequest", "client_req_canceled", cal.TransWarning, "")
				evt.Completed()
				quit <- true
				respExit <- ErrCacheClientReqCanceled
				errors <- ErrCacheClientReqCanceled
				return
			case <-safeExit:
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, crd.id, "doCacheRequest: request completed")
				}
				// Disable writing to CAL
				//evt := cal.NewCalEvent("doCacheRequest", "client_req_completed", cal.TransOK, "")
				//evt.Completed()
				return
			}
		}
	}()

	err := crd.getRecordFromCache(request, respExit, enableShadowTest, enableCacheByCorrId)

	select {
	case <-quit:
		return <-errors
	default:
		safeExit <- true
		return err
	}

}

// DispatchCachingSession checks if a SQL is enabled for caching. If yes, it tries to GET the record from cache. If not, the request is sent to the database.
func (crd *Coordinator) DispatchCachingSession(request *netstring.Netstring, reqType string) (uint32, bool, error) {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator DispatchCachingSession for", reqType, ": starting")
	}

	defer func() {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator DispatchCachingSession for", reqType, ": exiting")
		}
	}()

	// var key string
	logger.GetLogger().Log(logger.Verbose, "Incoming request.Serialized:", string(request.Serialized))

	if crd.isClientControlledCachingRequest {
		err := crd.doCacheRequest(crd.ctx, request, crd.cacheInfo.isShadowTest, crd.cacheInfo.cacheByCorrId)
		return crd.cacheInfo.ttl, crd.cacheInfo.cacheByCorrId, err
	} else {
		cacheCfg := getCacheCfg()
		cacheCfg.lock.Lock()
		rec, ok := cacheCfg.cacheCfgRecords[uint32(crd.sqlhash)]
		cacheCfg.lock.Unlock()
		logger.GetLogger().Log(logger.Verbose, uint32(crd.sqlhash), "CachingEnabled for ", reqType, ":", ok)
		if ok {
			logger.GetLogger().Log(logger.Verbose, "cacheRecord:", "sqlHash", rec.sqlHash, "sqlText", rec.sqlText, "ttl", rec.ttl, "cache enabled", rec.cachingEnabled, "cacheByCorrid", rec.cacheByCorrId, "cacheEnabledApps", rec.cacheEnabledClientApps)
			cacheByCorrId := true
			if rec.cachingEnabled == "Y" {
				if reqType == "GET" {
					if rec.cacheEnabledClientApps == "all" || (len(crd.poolName) > 0 && strings.Contains(rec.cacheEnabledClientApps, crd.poolName)) {
						if rec.cacheByCorrId == "N" {
							cacheByCorrId = false
						}
						if rec.enableShadowTest == "Y" {
							err := crd.doCacheRequest(crd.ctx, request, true, cacheByCorrId)
							return rec.ttl, cacheByCorrId, err
						} else {
							err := crd.doCacheRequest(crd.ctx, request, false, cacheByCorrId)
							return rec.ttl, cacheByCorrId, err
						}
					} else {
						logger.GetLogger().Log(logger.Verbose, "Application is not enabled for caching:", rec.sqlHash, rec.cacheEnabledClientApps, crd.poolName)
						return rec.ttl, cacheByCorrId, ErrCacheAppDisabled
					}
				} else {
					err := fmt.Errorf("Unsupported reqType...It must be GET")
					return rec.ttl, cacheByCorrId, err
				}
			} else {
				logger.GetLogger().Log(logger.Verbose, "sqlHash is disabled for caching:", rec.sqlHash, rec.cachingEnabled)
				return rec.ttl, cacheByCorrId, ErrCacheDisabled
			}
		}
		return 0, true, ErrCacheNotEnabled // Default for cacheByCorrId is true. Should be a no/op.
	}
}
