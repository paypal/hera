// Copyright 2020 PayPal Inc.
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
	"math"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paypal/hera/utility/logger"
)

type BindThrottle struct {
	Name                  string
	Value                 string
	Sqlhash               uint32
	RecentAttempt         atomic.Value // time.Time
	AllowEveryX           int
	AllowEveryXCount      int
	privGapCalculatedTime atomic.Value
}

var gBindEvict atomic.Value

type BindEvict struct {
	// evicted binds get throttled to have overall steady state during bad bind queries
	// nested map uses sqlhash "bindName|bindValue"
	BindThrottle map[uint32]map[string]*BindThrottle
	lock         sync.Mutex
}

func GetBindEvict() *BindEvict {
	cfg := gBindEvict.Load()
	if cfg == nil {
		out := BindEvict{BindThrottle: make(map[uint32]map[string]*BindThrottle)}
		gBindEvict.Store(&out)
		return &out
	}
	return cfg.(*BindEvict)
}
func (this *BindEvict) Copy() *BindEvict {
	out := BindEvict{BindThrottle: make(map[uint32]map[string]*BindThrottle)}
	for k, v := range this.BindThrottle {
		out.BindThrottle[k] = v
	}
	return &out
}

var nbnEndingNum *regexp.Regexp

func NormalizeBindName(bindName0 string) string {
	if nbnEndingNum == nil {
		nbnEndingNum = regexp.MustCompile("[0-9]*$")
	}
	out := nbnEndingNum.ReplaceAllString(bindName0, "#")
	if out == "p#" || out == ":p#" {
		return bindName0
	}
	return out
}

func (entry *BindThrottle) decrAllowEveryX(y int) {
	if y >= 2 && logger.GetLogger().V(logger.Warning) {
		info := fmt.Sprintf("hash:%d bindName:%s val:%s allowEveryX:%d-%d", entry.Sqlhash, entry.Name, entry.Value, entry.AllowEveryX, y)
		logger.GetLogger().Log(logger.Warning, "bind throttle decr", info)
	}
	entry.AllowEveryX -= y
	if entry.AllowEveryX > 0 {
		return
	}
	entry.AllowEveryX = 0
	GetBindEvict().lock.Lock()
	defer GetBindEvict().lock.Unlock()
	// delete entry
	if len(GetBindEvict().BindThrottle[entry.Sqlhash]) == 1 {
		updateCopy := GetBindEvict().Copy()
		delete(updateCopy.BindThrottle, entry.Sqlhash)
		gBindEvict.Store(updateCopy)
	} else {
		// copy everything except bindKV (skipping it is deleting it)
		bindKV := fmt.Sprintf("%s|%s", entry.Name, entry.Value)
		updateCopy := make(map[string]*BindThrottle)
		for k, v := range GetBindEvict().BindThrottle[entry.Sqlhash] {
			if k == bindKV {
				continue
			}
			updateCopy[k] = v
		}
		GetBindEvict().BindThrottle[entry.Sqlhash] = updateCopy
	}
}
func (entry *BindThrottle) incrAllowEveryX() {
	if logger.GetLogger().V(logger.Warning) {
		info := fmt.Sprintf("hash:%d bindName:%s val:%s prev:%d", entry.Sqlhash, entry.Name, entry.Value, entry.AllowEveryX)
		logger.GetLogger().Log(logger.Warning, "bind throttle incr", info)
	}
	entry.AllowEveryX = 3*entry.AllowEveryX + 1
	if entry.AllowEveryX > 10000 {
		entry.AllowEveryX = 10000
	}
}

func (be *BindEvict) ShouldBlock(sqlhash uint32, bindKV map[string]string, heavyUsage bool, throttleRecoveryFactor float64) (bool, *BindThrottle) {
	GetBindEvict().lock.Lock()
	sqlBinds := GetBindEvict().BindThrottle[sqlhash]
	GetBindEvict().lock.Unlock()
	for k0, v := range bindKV /*parseBinds(request)*/ {
		k := NormalizeBindName(k0)
		concatKey := fmt.Sprintf("%s|%s", k, v)
		entry, ok := sqlBinds[concatKey]
		if !ok {
			continue
		}
		/* matched bind name and value
		we stop searching and should return something */
		now := time.Now()
		// update based on usage
		if heavyUsage {
			entry.incrAllowEveryX()
			//disable the gap-based throttle reduction when usage is heavy,
			//ensuring reductions only happen during sustained low usage.
			if entry.privGapCalculatedTime.Load() != nil {
				entry.privGapCalculatedTime.Store((*time.Time)(nil))
			}
		} else {
			entry.decrAllowEveryX(2)
			if val := entry.privGapCalculatedTime.Load(); val == nil {
				entry.privGapCalculatedTime.Store(&now)
			} else {
				// check if not used in a while
				//This GAP will calculate every one second and decrese throttle for every 1 seconds
				//with multiplicative value
				privGapCalTime, ok := val.(*time.Time)
				if ok {
					gapInSeconds := now.Sub(*privGapCalTime).Seconds()
					if gapInSeconds >= 1.0 {
						//This calculation helps if sustained low usage around 60 seconds with 40% higher than threshold then
						//The recovery will 1 x 10 + 40 per second, in a minute it is going to reduce = 3000.
						//This makes from peak value 10000 it takes 2.5 minutes to full recovery from bind throttle.
						gap := gapInSeconds*GetConfig().BindEvictionDecrPerSec + math.Ceil(throttleRecoveryFactor)
						entry.decrAllowEveryX(int(gap))
						entry.privGapCalculatedTime.Store(&now)
					}
				}
			}
		}

		if entry.AllowEveryX == 0 {
			return false, nil
		}

		entry.RecentAttempt.Store(&now)
		entry.AllowEveryXCount++
		if entry.AllowEveryXCount < entry.AllowEveryX {
			return true /*block*/, entry
		}
		entry.AllowEveryXCount = 0

		return false, nil
	}
	// nothing matched
	return false, nil
}
