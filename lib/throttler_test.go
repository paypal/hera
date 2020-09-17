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
	"sync/atomic"
	"testing"
	"time"
)

func TestThrottler(t *testing.T) {
	t.Log("Running TestThrottler")
	th := NewThrottler(5, "testThr")
	start := time.Now().Unix()
	cnt := [20]int64{}
	var wg sync.WaitGroup
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func(id int) {
			th.CanRun()
			sec := time.Now().Unix() - start
			atomic.AddInt64(&cnt[sec%20], 1)
			wg.Done()
		}(i)
	}
	wg.Wait()
	if cnt[1] == 0 {
		t.Error("Did not start ")
	}
	if cnt[9] == 0 {
		t.Error("Finnished too early")
	}
	if cnt[13] != 0 {
		t.Error("Finnished too late")
	}
	var total int64
	for i := 0; i < 20; i++ {
		total += cnt[i]
	}
	if total != 50 {
		t.Error("Not all ran")
	}
}
