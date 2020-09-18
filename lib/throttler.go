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
	"sync"
	"time"

	"github.com/paypal/hera/cal"
)

// Throttler is used to throttle some activity based on a rate
type Throttler interface {
	CanRun()
}

type throttler struct {
	lastTime int64
	cnt      int64
	max      int64
	mtx      sync.Mutex
	name     string
}

// NewThrottler creates a throttler
func NewThrottler(max uint32, name string) Throttler {
	return &throttler{lastTime: time.Now().Unix(), max: int64(max), name: name}
}

// CanRun blocks
func (t *throttler) CanRun() {
	loop := true
	for loop {
		now := time.Now().Unix()
		t.mtx.Lock()
		if now <= t.lastTime {
			if t.cnt < t.max {
				t.cnt++
				loop = false
			}
		} else {
			t.lastTime = now
			t.cnt = 1
			loop = false
		}
		t.mtx.Unlock()
		if loop {
			evt := cal.NewCalEvent("OCCMUX", "Throttle_" + t.name, cal.TransOK, "")
			evt.Completed()
			time.Sleep(time.Second)
		}
	}
}
