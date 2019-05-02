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
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
	"sync/atomic"
	"time"
)

// TafQueryRuns store one run
type TafQueryRuns struct {
	startTimeUS int64

	didTimeoutCnt int
}

// TafQueries keeps for each SQL statistics if the query timed out.
// This is in order to decide if a query is "naturaly slow" so it can be ignored by taf
type TafQueries struct {
	lock    LockTimeout
	records map[int32]*TafQueryRuns

	CountNormallyFast int64
	debugContentions  int64
}

// TryLockContentionError the error for lock contention
var TryLockContentionError error
var gTAFInit bool
var gTAFQ []TafQueries

// GetTafQueries returns the taf queries statistics
func GetTafQueries(shardID int) *TafQueries {
	if !gTAFInit {
		gTAFQ = make([]TafQueries, GetConfig().NumOfShards)
		for i := 0; i < GetConfig().NumOfShards; i++ {
			gTAFQ[i].records = make(map[int32]*TafQueryRuns)
		}
		gTAFInit = true
		TryLockContentionError = errors.New("trylock contention")
	}
	return &gTAFQ[shardID]
}

// IsNormallySlow looks into the stats for the query whose has is sqlhash and returns if the query is normaly slow
func (tq *TafQueries) IsNormallySlow(sqlhash int32) (bool, error) {
	lrv := tq.lock.TryLock()
	if lrv == 0 {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "trylock contention ins", tq.debugContentions)
		}
		atomic.AddInt64(&(tq.debugContentions), 1)
		return false, TryLockContentionError
	}
	if lrv == -1 {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "clearing abadoned mutex in IsNaturallySlow")
		}
		tq.records = make(map[int32]*TafQueryRuns)
	}
	defer tq.lock.Unlock()

	rec, ok := tq.records[sqlhash]
	if !ok {
		return false, nil
	}

	nowUS := time.Now().UnixNano() / 1000
	expireUS := rec.startTimeUS + int64(GetConfig().TAFBinDuration)*1000*1000
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "IsNaturallySlow start,expire-ms", rec.startTimeUS/1000, expireUS/1000, GetConfig().TAFBinDuration*1000)
	}
	if expireUS < nowUS {
		rec.startTimeUS = nowUS
		rec.didTimeoutCnt = 0
	}

	outBool := (rec.didTimeoutCnt >= GetConfig().TAFNormallySlowCount)
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "IsNaturallySlow ", uint32(sqlhash), outBool, rec.didTimeoutCnt)
	}

	/* Every fast query that goes through increments CountNormallyFast.

	When there’s a slow query, we want to make sure we’ve had enough fast
	queries before we allow a slow one.  If we haven’t had enough fast queries
	(if CountNormallyFast < AllowSlowEveryX), then we return that this query
	should be treated as fast.

	A slow query resets the CountNormallyFast to zero since we don’t want lots
	of slow queries. */
	if !outBool {
		tq.CountNormallyFast++
	} else {
		if tq.CountNormallyFast < int64(GetConfig().TAFAllowSlowEveryX) {
			outBool = false
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "IsNaturallySlow suppressed by slow every x")
			}
			evt := cal.NewCalEvent("TafNormSlowTooMany", fmt.Sprintf("%d", uint32(sqlhash)), cal.TransOK, "")
			evt.Completed()
		}
		tq.CountNormallyFast = 0
	}

	return outBool, nil
}

// RecordTimeout update the statistic for the query whose hash is sqlhash, when the query timed out
func (tq *TafQueries) RecordTimeout(sqlhash int32) (bool, error) {
	lrv := tq.lock.TryLock()
	if lrv == 0 {
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "trylock contention")
		}
		atomic.AddInt64(&(tq.debugContentions), 1)
		return false, TryLockContentionError
	}
	if lrv == -1 {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "clearing abadoned mutex in RecordTimeout")
		}
		tq.records = make(map[int32]*TafQueryRuns)
	}
	defer tq.lock.Unlock()

	nowUS := time.Now().UnixNano() / 1000
	rec, ok := tq.records[sqlhash]
	if !ok {
		rec = &TafQueryRuns{}
		tq.records[sqlhash] = rec
		rec.startTimeUS = nowUS
	}

	if rec.startTimeUS+int64(GetConfig().TAFBinDuration)*1000*1000 < nowUS {
		rec.startTimeUS = nowUS
		rec.didTimeoutCnt = 0
	}
	rec.didTimeoutCnt++
	return true, nil
}
