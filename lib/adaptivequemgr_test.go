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
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setup() (rc int) {
	if err := os.Chdir("../test/config"); err != nil {
		log.Println("Chdir error:", err)
		return 1
	}
	return 0
}

func teardown() (rc int) {
	return 0
}

/**
 * @TODO need to setup to create cal_client.txt, default.txt, and hera.txt.
 */
func TestAdaptiveQueue(t *testing.T) {
	var useheratxt = false
	var err error
	if useheratxt {
		err = InitConfig()
		if err != nil {
			t.Errorf("config initialization failure %s", err.Error())
			return
		}
	} else {
		gOpsConfig = &OpsConfig{numWorkers: 6}
		gAppConfig = &Config{numWorkersCh: make(chan int, 1)}
		gAppConfig.numWorkersCh <- int(gOpsConfig.numWorkers)
		gAppConfig.BacklogTimeoutMsec = 1
		gAppConfig.BacklogTimeoutUnit = 1
		gAppConfig.LifoScheduler = true

		atomic.StoreUint32(&(gOpsConfig.satRecoverThresholdMs), 300)
		atomic.StoreUint32(&(gOpsConfig.satRecoverThrottleRate), 30)
		gAppConfig.SoftEvictionEffectiveTimeMs = 10000
		gAppConfig.SoftEvictionProbability = 50
	}

	os.Remove("state.log")
	// Create the state.log file
	_, err = os.Create("state.log")
	GetStateLog()
	t.Log("--------config GetSatRecoverThresholdMs(), GetSatRecoverThrottleRate(), GetSatRecoverFreqMs(), GetSatRecoverThrottleCnt(), gAppConfig.SoftEvictionEffectiveTimeMs, gAppConfig.SoftEvictionProbability", GetSatRecoverThresholdMs(), GetSatRecoverThrottleRate(), GetSatRecoverFreqMs(0), GetSatRecoverThrottleCnt(0), gAppConfig.SoftEvictionEffectiveTimeMs, gAppConfig.SoftEvictionProbability)

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

	wa := NewWorker(0, wtypeRW, 0, 0, "cloc")
	wb := NewWorker(1, wtypeRW, 0, 0, "cloc")
	wc := NewWorker(2, wtypeRW, 0, 0, "cloc")
	wd := NewWorker(3, wtypeRW, 0, 0, "cloc")
	we := NewWorker(4, wtypeRW, 0, 0, "cloc")
	wf := NewWorker(5, wtypeRW, 0, 0, "cloc")
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
	pool.numHealthyWorkers = 6
	if pool.activeQ.Len() != 6 {
		t.Error("workerpool initialization failure")
	}
	t.Log("--------workerinit", pool.activeQ.Len())

	//
	// empty worker pool
	//
	var worker = make([]*WorkerClient, pool.desiredSize)
	var ticket = make([]string, pool.desiredSize)
	for i := 0; i < pool.desiredSize; i++ {
		worker[i], ticket[i], err = pool.GetWorker(0)
		worker[i].sqlStartTimeMs = uint32((time.Now().UnixNano() - GetStateLog().GetStartTime()) / int64(time.Millisecond))
		worker[i].setState(wsBusy)
		t.Log("--------worker", worker, "ticket", hex.Dump([]byte(ticket[i])))
		if pool.activeQ.Len() != (pool.desiredSize - i - 1) {
			t.Error("getworker failure", err, pool.activeQ.Len())
		}
		//
		// put eor for sat recovery. recovery will drain extra eor in channel.
		//
		worker[i].outCh = make(chan *workerMsg, bfChannelSize)
		worker[i].outCh <- &workerMsg{data: nil, free: true, inTransaction: false}
		worker[i].outCh <- &workerMsg{data: nil, free: true, inTransaction: false}
		worker[i].outCh <- &workerMsg{data: nil, free: true, inTransaction: false}
		worker[i].outCh <- &workerMsg{data: nil, free: true, inTransaction: false}
		worker[i].outCh <- &workerMsg{data: nil, free: true, inTransaction: false}
		//
		// sat recovery order based on sqlstarttime.
		//
		time.Sleep(time.Millisecond * 10)
	}

	//
	// listen to sat recover message of worker[0] and return worker
	//
	go func(pool *WorkerPool, worker *WorkerClient, ticket string) {
		for {
			msg, ok := <-worker.ctrlCh
			if !ok {
				t.Log("satrecover ctrlch closed")
			}
			if msg.abort {
				t.Log("satrecover return worker", ticket)
				worker.Recover(pool, ticket, &strandedCalInfo{raddr: "raddr", laddr: "laddr"})
			}
		}
	}(pool, worker[0], ticket[0])

	//
	// this will make backlogcnt == 1.
	// wait for saturation recovery to return a worker.
	// (set lastemptytime and clears sqlevicthash).
	//
	go func(pool *WorkerPool) {
		_, _, err = pool.GetWorker(0, 1000)
		if err == nil {
			t.Log("getworker picked up a sat recover wroker")
		} else {
			t.Log("getworker backlog", err)
		}
	}(pool)
	time.Sleep(time.Millisecond * 500)

	//
	// get worker[1].
	//
	go func(pool *WorkerPool) {
		_, _, err := pool.GetWorker(0, 1000)
		if err != nil {
			t.Error("getworker failure", err)
		} else {
			t.Log("--------passing getworker")
		}
	}(pool)
	//
	// this will return worker having a wrong ticket since worker[0] was pick by sat recovery.
	//
	err = pool.ReturnWorker(worker[0], ticket[0])
	if err == nil {
		t.Error("return worker wrong ticket failure")
	} else {
		t.Log("--------passing returnworker[0]", err)
	}
	err = pool.ReturnWorker(worker[1], ticket[1])
	if err != nil {
		t.Error("return worker failure")
	} else {
		t.Log("--------passing returnworker[1]", err)
	}
	//
	// make sure the previous getworker gets worker[1].
	//
	time.Sleep(time.Millisecond * 100)

	//
	// reset sqlevicthash. 2 getworker to avoid clearevicthash.
	//
	go func(pool *WorkerPool) {
		pool.GetWorker(0, 1000)
	}(pool)
	go func(pool *WorkerPool) {
		pool.GetWorker(0, 1000)
	}(pool)
	time.Sleep(time.Millisecond * 1100)

	for i := 0; i < 80; i++ {
		go func(pool *WorkerPool, cnt int) {
			_, _, err := pool.GetWorker(0)
			if err == nil {
				t.Error("getworker backlog failure", cnt)
			} else {
				t.Log("--------passing getworker backlog,", cnt, err)
			}
		}(pool, i)
	}

	time.Sleep(time.Second * 3)
}
