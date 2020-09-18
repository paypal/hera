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
	"encoding/hex"
	"os"
	"sync"
	"testing"
	//"time"
)

func TestPoolDempotency(t *testing.T) {
	var useheratxt = false
	var err error
	if useheratxt {
		err = InitConfig()
		if err != nil {
			t.Errorf("config initialization failure %s", err.Error())
			return
		}
	} else {
		gAppConfig = &Config{BacklogTimeoutMsec: 1, LifoScheduler: true, numWorkersCh: make(chan int, 1)}
		gOpsConfig = &OpsConfig{numWorkers: 3}
		gAppConfig.numWorkersCh <- int(gOpsConfig.numWorkers)
	}
	os.Remove("state.log")
	// Create the state.log file
	_, err = os.Create("state.log")
	GetStateLog()
	t.Log("--------config SatRecoverThresholdMs, SatRecoverThrottleRate, SatRecoverFreqMs, gAppConfig.SatRecoverThrottleCnt, gAppConfig.SoftEvictionEffectiveTimeMs, gAppConfig.SoftEvictionProbability", GetSatRecoverThresholdMs(), GetSatRecoverThrottleRate(), GetSatRecoverFreqMs(0), GetSatRecoverThrottleCnt(0), gAppConfig.SoftEvictionEffectiveTimeMs, gAppConfig.SoftEvictionProbability)

	pool := &WorkerPool{}
	pool.Type = wtypeRW
	pool.activeQ = NewQueue()
	pool.poolCond = sync.NewCond(&sync.Mutex{})
	pool.InstID = 0
	pool.ShardID = 0
	pool.currentSize = 0
	pool.desiredSize = 6
	pool.moduleName = ""
	pool.checkoutTickets = make(map[interface{}]string)
	pool.workers = make([]*WorkerClient, pool.desiredSize)
	for i := 0; i < pool.desiredSize; i++ {
		pool.currentSize++
	}
	pool.checkoutTickets = make(map[interface{}]string)
	pool.aqmanager = &adaptiveQueueManager{}
	err = pool.aqmanager.init(pool)
	if err != nil {
		t.Errorf("aqmanager failure %s", err.Error())
		return
	}
	go pool.checkWorkerLifespan()

	wa := NewWorker(0, wtypeRW, 0, 0, "cloc", nil)
	wb := NewWorker(1, wtypeRW, 0, 0, "cloc", nil)
	wc := NewWorker(2, wtypeRW, 0, 0, "cloc", nil)
	wd := NewWorker(3, wtypeRW, 0, 0, "cloc", nil)
	we := NewWorker(4, wtypeRW, 0, 0, "cloc", nil)
	wf := NewWorker(5, wtypeRW, 0, 0, "cloc", nil)
	wa.setState(wsAcpt)
	wb.setState(wsAcpt)
	wc.setState(wsAcpt)
	wd.setState(wsAcpt)
	we.setState(wsAcpt)
	wf.setState(wsAcpt)
	pool.WorkerReady(wa)
	pool.WorkerReady(wb)
	pool.WorkerReady(wc)
	pool.WorkerReady(wd)
	pool.WorkerReady(we)
	pool.WorkerReady(wf)
	if pool.activeQ.Len() != 6 {
		t.Error("workerpool initialization failure")
	}
	t.Log("--------workerinit", pool.activeQ.Len())

	worker, ticket, err := pool.GetWorker(0)
	t.Log("--------worker", worker, "ticket", hex.Dump([]byte(ticket)))
	if pool.activeQ.Len() != 5 {
		t.Error("getworker failure", err, pool.activeQ.Len())
	}
	worker3, ticket3, _ := pool.GetWorker(0)
	t.Log("--------worker3", worker3, "ticket3", hex.Dump([]byte(ticket3)))
	if pool.activeQ.Len() != 4 {
		t.Error("getworker3 failure", pool.activeQ.Len())
	}

	err = pool.ReturnWorker(worker, ticket)
	if err != nil || pool.activeQ.Len() != 5 {
		t.Error("returnworker failure", err, pool.activeQ.Len())
	} else {
		t.Log("--------passing return worker")
	}

	err = pool.ReturnWorker(worker, ticket)
	if err == nil {
		t.Error("returnworker twice", err, pool.activeQ.Len())
	} else {
		t.Log("--------passing return worker twice,", err)
	}

	err = pool.ReturnWorker(worker3, ticket)
	if err == nil {
		t.Error("returnworker with empty ticket", err, pool.activeQ.Len())
	} else {
		t.Log("--------passing return worker with wrong ticket,", err)
	}

	err = pool.ReturnWorker(worker3, "")
	if err == nil {
		t.Error("returnworker with empty ticket", err, pool.activeQ.Len())
	} else {
		t.Log("--------passing return worker with empty ticket,", err)
	}

	err = pool.ReturnWorker(worker3, ticket3)
	if err != nil || pool.activeQ.Len() != 6 {
		t.Error("returnworker failure", err, pool.activeQ.Len())
	} else {
		t.Log("--------passing return worker")
	}

}
