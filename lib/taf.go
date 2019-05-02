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
	"fmt"
	"github.com/paypal/hera/utility/logger"
	"math/rand"
	"sync/atomic"
)

// TAF keeps a statistic of errors and successes and tells which database whould be used for the next request
type TAF interface {
	// return true if the request should use the primary db, false to use the fallback
	UsePrimary() bool
	// For logging, gets the internal PCT field, which is a measure of the health of the primary database
	GetPct() int
	// To be called by coordinator if the request was ok
	NotifyOK()
	// To be called by coordinator if the request timed out failed with an ORA error
	NotifyError()
}

var giTAF []*taf

// GetTAF returns the TAF entry for the shard
func GetTAF(shard int) TAF {
	return giTAF[shard]
}

const (
	tafMaxPct = uint32(110)
	tafMinPct = uint32(1)
	// pct change for recovery as well as max pct change for failure
	tafPctChange   = uint32(10)
	tafDeltaPctMin = uint32(1)
	tafDeltaPctMax = uint32(64)
)

/**
 * taf implements TAF interface
 */
type taf struct {
	// pct is the probability of going to the primary database on the next request. It is in [1..110] range,
	// it decreases when we see falures in the primary and it increases when we see successes in the primary.
	// 1 is the lower bound because even if the primary is down completely we still send 1% of requests to the primary,
	// as a way of doing health check. 110 is the upper bound because a spurrious error should not cause moving away
	// from the primary, we still send 100% of requests to the primary
	pct uint32
	// deltaPct is by how much we change pct
	deltaPct uint32
	shard    int
}

// InitTAF initializes the TAF structure
func InitTAF(shards int) {
	giTAF = make([]*taf, shards, shards)
	for tf := range giTAF {
		giTAF[tf] = &taf{pct: tafMaxPct, deltaPct: 1, shard: tf}
	}
}

// UsePrimary tells if it should use the primary
func (tf *taf) UsePrimary() bool {
	return uint32(rand.Intn(100)) < atomic.LoadUint32(&(tf.pct))
}

// GetPct returns the current value of PCT
func (tf *taf) GetPct() int {
	return int(tf.pct)
}

// NotifyOK increases pct by 10 and halves deltaPct
func (tf *taf) NotifyOK() {
	// we ignore race condition because we don't have to be precise. so no need for CAS
	pct := atomic.LoadUint32(&(tf.pct))
	deltaPct := atomic.LoadUint32(&(tf.deltaPct))
	if pct < tafMaxPct {
		if pct < tafMaxPct-tafPctChange {
			pct += tafPctChange
		} else {
			pct = tafMaxPct
		}
		atomic.StoreUint32(&(tf.pct), pct)
	}
	if deltaPct > tafDeltaPctMin {
		deltaPct /= 2
		atomic.StoreUint32(&(tf.deltaPct), deltaPct)
	}
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "TAF NotifyOK: pct =", pct, ", deltaPct:", deltaPct, ", shard:", tf.shard)
	}
}

// NotifyError decreases pct by max(10, deltaPct) and doubles deltaPct
func (tf *taf) NotifyError() {
	// we ignore race condition because we don't have to be precise. so no need for CAS
	pct := atomic.LoadUint32(&(tf.pct))
	oldDeltaPct := atomic.LoadUint32(&(tf.deltaPct))
	if pct > tafMinPct {
		var deltaPct uint32
		if oldDeltaPct > tafPctChange {
			deltaPct = tafPctChange
		} else {
			deltaPct = oldDeltaPct
		}
		if pct > tafMinPct+deltaPct {
			pct -= deltaPct
		} else {
			pct = tafMinPct
		}
		atomic.StoreUint32(&(tf.pct), pct)
	}
	if oldDeltaPct < tafDeltaPctMax {
		oldDeltaPct *= 2
		atomic.StoreUint32(&(tf.deltaPct), oldDeltaPct)
	}
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "TAF NotifyError: pct =", pct, ", deltaPct:", oldDeltaPct, ", shard:", tf.shard)
	}
}

func (tf *taf) dump() string {
	return fmt.Sprintf("TAFLB pct=%d%%, deltaPct=%d%%, shard=%d\n", tf.pct, tf.deltaPct, tf.shard)
}
