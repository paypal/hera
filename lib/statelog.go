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
	"bytes"
	"context"
	"errors"
	"fmt"
	otel_logger "github.com/paypal/hera/utility/logger/otel"
	otelconfig "github.com/paypal/hera/utility/logger/otel/config"
	"go.opentelemetry.io/otel"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
)

// ConnState is a possible state
type ConnState int

// ConnState constants
const (
	//
	// following are connection states reported by proxies
	//
	Assign       ConnState = 0 // count by proxy
	Idle                   = 1 // count by proxy
	Backlog                = 2 // count by shard and proxy
	Stranded               = 3 // count by shard and proxy
	Close                  = 4 // this one is not reported, but used to track connection.
	MaxConnState           = 5 // count of connection states
)

// StateNames contains names used to print header line. first 7 worker states, rest proxy connection states
var StateNames = [MaxWorkerState + MaxConnState]string{
	"init", "acpt", "wait", "busy", "schd", "fnsh", "quce", "asgn", "idle", "bklg", "strd", "cls"}

// These get a prefix added to them in init
var typeTitlePrefix = [wtypeTotalCount]string{
	".w", ".r", ".taf"}

// WorkerStateInfo is a container holding worker state information
type WorkerStateInfo struct {
	state HeraWorkerStatus
	req   int64
	resp  int64
}

// ConnStateInfo is a container holding connection state information
type ConnStateInfo struct {
	//
	// array of MaxConnState with count of each connstate
	//
	perStateCnt []int
}

// StateLog is exposed as a singleton. all stateful resources are protected behind a
// message channel that sychronizes incoming messages. user should not call any of
// the internal functions that are not threadsafe.
type StateLog struct {
	//
	// array of maps for different workertypes with each value holding a two dimension
	// array of workerstateinfo[instance][workid].
	// standby has instance==3. rw has instance==2.
	//
	mWorkerStates [](map[HeraWorkerType]([][]*WorkerStateInfo))

	//
	// array of maps for connstate of different workertypes with each value holds an
	// array of connstateinfo[instance]. unlike c++ stateinfo that counts per workertype
	// this one counts per instance
	//
	mConnStates [](map[HeraWorkerType]([]*ConnStateInfo))

	//
	// loaded once from configuration during init and used later elsewhere.
	//
	maxShardSize  int
	maxStndbySize int

	//
	// title printed in workertype column (leftmost).
	//
	mTypeTitles [](map[HeraWorkerType]([]string))

	//OTEL statelog occ-worker dimension titles
	workerDimensionTitle map[string]string
	//
	// header row (state)
	//
	mStateHeader string

	//
	// mWriteHeaderInterval loaded from configuration. mWriteHeader is a running counter.
	//
	mWriteHeaderInterval int
	mWriteHeader         int

	//
	//
	//
	mLastReqCnt [](map[HeraWorkerType]([]int64))
	mLastRspCnt [](map[HeraWorkerType]([]int64))

	//
	// logger to "state.log"
	//
	fileLogger *log.Logger

	//
	// channel to synchronize state changes (statelog.go own its entire lifecycle)
	//
	mEventChann chan StateEvent

	//
	// start time since epoch in ns
	//
	mServerStartTime int64

	//worker pool configurations
	workerPoolCfg []map[HeraWorkerType]*WorkerPoolCfg
}

// StateEventType is an event published by proxy when state changes.
type StateEventType int

// StateEventType constants
const (
	WorkerStateEvt = iota
	ConnStateEvt
	WorkerResizeEvt
	StateEventTypeSize
)

// StateEvent keeps the state information
type StateEvent struct {
	eType     StateEventType
	shardID   int
	wType     HeraWorkerType
	instID    int
	workerID  int
	newWState HeraWorkerStatus
	oldCState ConnState
	newCState ConnState
	newWSize  int
}

var gStateLogInstance *StateLog
var statelogOnce sync.Once

// GetStateLog gets the state log object
func GetStateLog() *StateLog {
	//
	// no retry. if intialization fails, caller gets nil and should act accordingly.
	//
	statelogOnce.Do(func() {
		gStateLogInstance = &StateLog{}
		err := gStateLogInstance.init()
		if err != nil {
			gStateLogInstance = nil
		}
	})
	return gStateLogInstance
}

// PublishStateEvent sends the event to the channel, so it will be processed by the state log routine
func (sl *StateLog) PublishStateEvent(_evt StateEvent) error {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "publish state event", _evt.eType)
	}
	// missing event could cause unbalanced statelog output.
	sl.mEventChann <- _evt
	return nil
}

// SetStartTime emits the CAL event
func (sl *StateLog) SetStartTime(t time.Time) {
	e := cal.NewCalEvent(cal.EventTypeMessage, "STATE", "mux_start_time", "")
	var ts = t.UnixNano() / int64(time.Second)
	var us = (t.UnixNano() % int64(time.Second)) / int64(time.Microsecond)
	e.AddDataInt("tv_sec", ts)
	e.AddDataInt("tv_usec", us)
	e.Completed()

	sl.mServerStartTime = t.UnixNano()
}

// GetStartTime gets the app start time
func (sl *StateLog) GetStartTime() int64 {
	return sl.mServerStartTime
}

// HasActiveWorker is a best effort, without thread locking, telling if at least a worker is active
func (sl *StateLog) HasActiveWorker() bool {
	shdCnt := sl.maxShardSize
	if GetConfig().EnableWhitelistTest {
		shdCnt = 1
	}

	for s := 0; s < shdCnt; s++ {
		rwpool, era := GetWorkerBrokerInstance().GetWorkerPool(wtypeRW, 0, s)
		if era != nil {
			// wow, is this possible?
			if logger.GetLogger().V(logger.Alert) {
				logger.GetLogger().Log(logger.Alert, "no RW pool")
			}
			return false
		}
		if rwpool.GetHealthyWorkersCount() > 0 {
			return true
		}

		if GetConfig().EnableTAF {
			stdbyPool, erb := GetWorkerBrokerInstance().GetWorkerPool(wtypeStdBy, 0, s)
			if erb == nil {
				return stdbyPool.GetHealthyWorkersCount() > 0
			}
		}

		roPool, erc := GetWorkerBrokerInstance().GetWorkerPool(wtypeRO, 0, s)
		if erc == nil {
			return roPool.GetHealthyWorkersCount() > 0
		}
	}

	return false
}

// GetTotalConnections is a best effort, without thread locking, to give the total number of connections
func (sl *StateLog) GetTotalConnections() int {
	var counter = 0
	for s := 0; s < sl.maxShardSize; s++ {
		counter += sl.mConnStates[s][wtypeRW][0].perStateCnt[Assign]
		counter += sl.mConnStates[s][wtypeRW][0].perStateCnt[Idle]
		for t := 0; t < int(wtypeTotalCount); t++ {
			instCnt := len(sl.mWorkerStates[s][HeraWorkerType(t)])
			for n := 0; n < instCnt; n++ {
				//
				// do not count close state.
				//
				for c := Backlog; c < Close; c++ {
					counter += sl.mConnStates[s][HeraWorkerType(t)][n].perStateCnt[c]
				}
			} // instance
		} // wtype
	} // sharding
	return counter
}

// GetStrandedWorkerCountForPool is a best effort function to get the count of backlog in a worker pool without thread locking.
func (sl *StateLog) GetStrandedWorkerCountForPool(shardID int, wType HeraWorkerType, instID int) int {
	var cnt = 0
	//
	// if any of the shareId, instID, and wType is invalid, return 0.
	//
	if (shardID >= 0) && (shardID < sl.maxShardSize) {
		if (wType >= wtypeRW) && (wType < wtypeTotalCount) {
			instCnt := len(sl.mWorkerStates[shardID][wType])
			if (instID >= 0) && (instID < instCnt) {
				for n := 0; n < instCnt; n++ {
					workerCnt := len(sl.mWorkerStates[shardID][wType][n])
					if workerCnt == 0 {
						continue
					}
					for w := 0; w < workerCnt; w++ {
						workerState := sl.mWorkerStates[shardID][wType][n][w].state
						if workerState == wsQuce {
							cnt++
						}
					}
				}
			}
		}
	}
	//logger.GetLogger().Log(logger.Verbose, "(strandcnt, shard, inst, wt)=", cnt, shardId, instID, wType)
	return cnt
}

func (sl *StateLog) GetWorkerCountForPool(workerState HeraWorkerStatus, shardID int, wType HeraWorkerType, instID int) int {
	var cnt = 0
	//
	// if any of the shareId, instID, and wType is invalid, return 0.
	//
	if (shardID >= 0) && (shardID < sl.maxShardSize) {
		if (wType >= wtypeRW) && (wType < wtypeTotalCount) {
			instCnt := len(sl.mWorkerStates[shardID][wType])
			if (instID >= 0) && (instID < instCnt) {
				for n := 0; n < instCnt; n++ {
					workerCnt := len(sl.mWorkerStates[shardID][wType][n])
					if workerCnt == 0 {
						continue
					}
					for w := 0; w < workerCnt; w++ {
						if workerState == sl.mWorkerStates[shardID][wType][n][w].state {
							cnt++
						}
					}
				}
			}
		}
	}
	//logger.GetLogger().Log(logger.Verbose, "(strandcnt, shard, inst, wt)=", cnt, shardId, instID, wType)
	return cnt
}

// ProxyHasCapacity checks if there is enough capacity
func (sl *StateLog) ProxyHasCapacity(_wlimit int, _rlimit int) (bool, int) {
	shdCnt := sl.maxShardSize
	if GetConfig().EnableWhitelistTest {
		shdCnt = 1
	}

	//
	// if any shard has free worker, we can not bounce since the request could go to free shard
	//
	if sl.hasFreeWorker(shdCnt) {
		return true, 0
	}

	var wbacklog = 0
	var rbacklog = 0
	var readerCnt = 0
	for s := 0; s < shdCnt; s++ {
		instCnt := len(sl.mWorkerStates[s][wtypeRW])
		for n := 0; n < instCnt; n++ {
			wbacklog += sl.mConnStates[s][wtypeRW][n].perStateCnt[Backlog]
		}
		if GetConfig().EnableTAF {
			instCnt = len(sl.mWorkerStates[s][wtypeStdBy])
			for n := 0; n < instCnt; n++ {
				wbacklog += sl.mConnStates[s][wtypeStdBy][n].perStateCnt[Backlog]
			}
		}

		//logger.GetLogger().Log(logger.Verbose, "proxyhascap wba ", wbacklog, _wlimit)
		instCnt = len(sl.mWorkerStates[s][wtypeRO])
		for n := 0; n < instCnt; n++ {
			readerCnt += len(sl.mWorkerStates[s][wtypeRO][n])
			rbacklog += sl.mConnStates[s][wtypeRO][n].perStateCnt[Backlog]
		}
		//logger.GetLogger().Log(logger.Verbose, "proxyhascap wba ", wbacklog, _wlimit, readerCnt, rbacklog, _rlimit)
	}
	return (wbacklog <= _wlimit) && ((rbacklog <= _rlimit) || (readerCnt == 0)), wbacklog + rbacklog
}

func (sl *StateLog) numFreeWorker(shardId int, wType HeraWorkerType) int {
	out := 0
	instCnt := len(sl.mWorkerStates[shardId][wType])
	for n := 0; n < instCnt; n++ {
		workerCnt := len(sl.mWorkerStates[shardId][wType][n])
		if workerCnt == 0 {
			continue
		}
		for w := 0; w < workerCnt; w++ {
			workerState := sl.mWorkerStates[shardId][wType][n][w].state
			if workerState == wsAcpt {
				out++
			}
		}
	}
	return out
}

func (sl *StateLog) hasFreeWorker(shdCnt int) bool {
	for s := 0; s < shdCnt; s++ {
		instCnt := len(sl.mWorkerStates[s][wtypeRW])
		for n := 0; n < instCnt; n++ {
			workerCnt := len(sl.mWorkerStates[s][wtypeRW][n])
			if workerCnt == 0 {
				continue
			}
			for w := 0; w < workerCnt; w++ {
				workerState := sl.mWorkerStates[s][wtypeRW][n][w].state
				if workerState == wsAcpt {
					return true
				}
			}
		}
		instCnt = len(sl.mWorkerStates[s][wtypeRO])
		for n := 0; n < instCnt; n++ {
			wCnt := len(sl.mWorkerStates[s][wtypeRO][n])
			if wCnt == 0 {
				continue
			}
			for w := 0; w < wCnt; w++ {
				workerState := sl.mWorkerStates[s][wtypeRO][n][w].state
				if workerState == wsAcpt {
					return true
				}
			}
		}
		if GetConfig().EnableTAF {
			instCnt = len(sl.mWorkerStates[s][wtypeStdBy])
			for n := 0; n < instCnt; n++ {
				sbwCnt := len(sl.mWorkerStates[s][wtypeStdBy][n])
				if sbwCnt == 0 {
					continue
				}
				for w := 0; w < sbwCnt; w++ {
					workerState := sl.mWorkerStates[s][wtypeStdBy][n][w].state
					if workerState == wsAcpt {
						return true
					}
				}
			}
		}
	}
	return false
}

func getTime() string {
	t := time.Now()
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	return fmt.Sprintf("%02d/%02d/%d %02d:%02d:%02d: ", month, day, year, hour, min, sec)
}

func (sl *StateLog) init() error {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "init statelog")
	}
	sl.maxShardSize = GetConfig().NumOfShards
	if sl.maxShardSize == 0 || !(GetConfig().EnableSharding) {
		sl.maxShardSize = 1
	}
	sl.maxStndbySize = GetConfig().NumStdbyDbs
	if sl.maxStndbySize > 10 {
		sl.maxStndbySize = 10
	} else if sl.maxStndbySize == 0 {
		sl.maxStndbySize = 1
	}

	//
	// filelog to state.log
	//

	currentDir, absperr := filepath.Abs(filepath.Dir(os.Args[0]))
	if absperr != nil {
		currentDir = "./"
	} else {
		currentDir = currentDir + "/"
	}

	filename := currentDir + "state.log"

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	// format backward compatible with C++
	sl.fileLogger = log.New(file, "" /*log.Ldate|log.Ltime*/, 0)

	//
	// poolconfig object in workerbroker has the exact worker counts for this deployment
	//
	if GetWorkerBrokerInstance() == nil {
		return errors.New("broker not initialized")
	}
	sl.workerPoolCfg = GetWorkerBrokerInstance().GetWorkerPoolCfgs()

	//
	// allocate array for each shard
	//
	sl.mWorkerStates = make([]map[HeraWorkerType][][]*WorkerStateInfo, sl.maxShardSize)
	sl.mConnStates = make([]map[HeraWorkerType][]*ConnStateInfo, sl.maxShardSize)
	sl.mTypeTitles = make([]map[HeraWorkerType][]string, sl.maxShardSize)
	sl.workerDimensionTitle = make(map[string]string)
	sl.mLastReqCnt = make([]map[HeraWorkerType][]int64, sl.maxShardSize)
	sl.mLastRspCnt = make([]map[HeraWorkerType][]int64, sl.maxShardSize)
	//
	// for each shard, initialize map
	//
	var totalWorkersCount int //Use this value to initialize bufferred channel for statelog metrics
	//
	// for each shard, initialize map
	//
	for s := 0; s < sl.maxShardSize; s++ {
		sl.mWorkerStates[s] = make(map[HeraWorkerType][][]*WorkerStateInfo, wtypeTotalCount)
		sl.mConnStates[s] = make(map[HeraWorkerType][]*ConnStateInfo, wtypeTotalCount)
		sl.mTypeTitles[s] = make(map[HeraWorkerType][]string, wtypeTotalCount)
		sl.mLastReqCnt[s] = make(map[HeraWorkerType][]int64, wtypeTotalCount)
		sl.mLastRspCnt[s] = make(map[HeraWorkerType][]int64, wtypeTotalCount)
		//
		// for each workertype, initialize two dimension array
		//
		for t := 0; t < int(wtypeTotalCount); t++ {
			instCnt := sl.workerPoolCfg[s][HeraWorkerType(t)].instCnt
			workerCnt := sl.workerPoolCfg[s][HeraWorkerType(t)].maxWorkerCnt
			totalWorkersCount += workerCnt
			sl.mWorkerStates[s][HeraWorkerType(t)] = make([][]*WorkerStateInfo, instCnt)
			sl.mConnStates[s][HeraWorkerType(t)] = make([]*ConnStateInfo, instCnt)
			sl.mTypeTitles[s][HeraWorkerType(t)] = make([]string, instCnt)
			sl.mLastReqCnt[s][HeraWorkerType(t)] = make([]int64, instCnt)
			sl.mLastRspCnt[s][HeraWorkerType(t)] = make([]int64, instCnt)
			//
			// for each "standby" instance, initialize array for each worker
			//
			for i := 0; i < instCnt; i++ {
				sl.mWorkerStates[s][HeraWorkerType(t)][i] = make([]*WorkerStateInfo, workerCnt)
				sl.mConnStates[s][HeraWorkerType(t)][i] = &ConnStateInfo{}
				sl.mConnStates[s][HeraWorkerType(t)][i].perStateCnt = make([]int, MaxConnState)

				sl.mLastReqCnt[s][HeraWorkerType(t)][i] = 0
				sl.mLastRspCnt[s][HeraWorkerType(t)][i] = 0
				//
				// for each worker, initialize stateinfo.
				//
				for w := 0; w < workerCnt; w++ {
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, "init statelog", s, t, i, w)
					}
					sl.mWorkerStates[s][HeraWorkerType(t)][i][w] = &WorkerStateInfo{state: HeraWorkerStatus(wsInit)}
				}
			}
		}
	}
	//
	// prepare horizontal (state) and vertical (workertype) titles.
	//
	var shardEnabled = GetConfig().EnableSharding && (GetConfig().NumOfShards >= 1)
	var buf bytes.Buffer
	buf.WriteString("-----------")
	for i := 0; i < (MaxWorkerState + MaxConnState - 1); i++ {
		buf.WriteString(fmt.Sprintf("%6s", StateNames[i]))
	}
	sl.mStateHeader = buf.String()

	for idx, val := range typeTitlePrefix {
		typeTitlePrefix[idx] = GetConfig().StateLogPrefix + val
	}
	if GetConfig().ReadonlyPct == 0 {
		typeTitlePrefix[wtypeRW] = GetConfig().StateLogPrefix
	}
	for s := 0; s < sl.maxShardSize; s++ {
		for t := wtypeRW; t < wtypeTotalCount; t++ {
			var suffix = ".sh" + strconv.Itoa(s)
			instCnt := sl.workerPoolCfg[s][HeraWorkerType(t)].instCnt

			for i := 0; i < instCnt; i++ {
				sl.mTypeTitles[s][t][i] = typeTitlePrefix[t]
				if instCnt > 1 {
					sl.mTypeTitles[s][t][i] += strconv.Itoa(i + 1)
				}
				if shardEnabled {
					sl.mTypeTitles[s][t][i] += suffix
				}
				sl.workerDimensionTitle[sl.mTypeTitles[s][t][i]] = strings.Replace(sl.mTypeTitles[s][t][i], GetConfig().StateLogPrefix, otelconfig.OTelConfigData.PoolName, 1)
			}
		}
	}

	//
	// @TODO
	//
	sl.mWriteHeaderInterval = 20
	sl.mWriteHeader = 0

	sl.mEventChann = make(chan StateEvent, 3000)

	if otelconfig.OTelConfigData.Enabled {
		// Initialize statelog_metrics to send metrics information currently we are ignoring registration object returned from this call
		stateStartErr := otel_logger.StartMetricsCollection(context.Background(), totalWorkersCount,
			otel_logger.WithMetricProvider(otel.GetMeterProvider()),
			otel_logger.WithAppName(otelconfig.OTelConfigData.PoolName))

		if stateStartErr != nil {
			logger.GetLogger().Log(logger.Alert, "failed to start metric collection agent for statelogs", stateStartErr)
		}
	}
	//
	// start periodical reporting
	//
	go func() {
		statelogInterval := GetConfig().StateLogInterval // in sec
		waitTime := time.Second * time.Duration(statelogInterval)
		//reportTimer := time.After(waitTime)
		reportTimer := time.NewTimer(waitTime)
		defer reportTimer.Stop()
		//
		// forever waiting for state event or timeout every second to genreport.
		//
		for {
			select {
			//case <- reportTimer:
			case <-reportTimer.C:
				sl.genReport()
				reportTimer.Reset(waitTime)
			case evt, ok := <-sl.mEventChann:
				if ok {
					switch evt.eType {
					case WorkerStateEvt:
						sl.setWorkerState(evt.shardID, evt.wType, evt.instID, evt.workerID, evt.newWState)
					case ConnStateEvt:
						sl.updateConnectionState(evt.shardID, evt.wType, evt.instID, evt.oldCState, evt.newCState)
					case WorkerResizeEvt:
						sl.resizeWorkers(evt.shardID, evt.wType, evt.instID, evt.newWSize)
					default:
						if logger.GetLogger().V(logger.Info) {
							logger.GetLogger().Log(logger.Info, "unknow stateevent type", evt.eType)
						}
					}
				}
			}
		}
	}()

	evt := cal.NewCalEvent(cal.EventTypeMessage, "STATELOG", cal.TransOK, "Created.")
	evt.Completed()

	return nil
}

/**
 * client should not call these "private" none-threadsafe functions directly.
 * use PublishStateEvent instead.
 *
 * @TODO test
 *
 * if resize fails, genreport prints with old config. it will not cause index outofbound.
 */
func (sl *StateLog) resizeWorkers(s int, t HeraWorkerType, i int, newSize int) error {
	//
	// if not initialized with s/t/i, it is an error.
	//
	var wtStates = sl.mWorkerStates[s][HeraWorkerType(t)]
	var connStates = sl.mConnStates[s][HeraWorkerType(t)]
	if wtStates == nil || wtStates[i] == nil || connStates == nil || connStates[i] == nil {
		return errors.New("cannot resize nonexisting worker instance")
	}

	currentSize := len(wtStates[i])
	if currentSize == newSize {
		return nil
	}
	//
	// @TODO works for size increase, not sure about decrease.
	// how do we know which worker(s) are removed. for now, just truncate at the tail.
	//
	if currentSize < newSize {
		for a := currentSize; a < newSize; a++ {
			wtStates[i] = append(wtStates[i], &WorkerStateInfo{state: HeraWorkerStatus(wsInit)})
		}
	} else {
		wtStates[i] = wtStates[i][0:newSize]
	}
	return nil
}

/**
 * shardId does not matter. while setting closed->idle and idle->closed in connectionhandler
 * we do not know which shard it is going to be. so, idle and assign counts are shared by all the
 * different shards. other connection counts are shard specific.
 */
func (sl *StateLog) updateConnectionState(_shardID int, _type HeraWorkerType, _instID int, _old ConnState, _new ConnState) {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "statelog updateconnectionstate", _shardID, _type, _instID, _old, _new)
	}
	//
	// sanity check newstate.
	//
	if (_new == _old) || (_new < 0) || (_new >= MaxConnState) ||
		(_old < 0) || (_old >= MaxConnState) ||
		(_shardID >= sl.maxShardSize) || (_type >= wtypeTotalCount) ||
		(_instID >= sl.maxStndbySize) {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "statelog sanityfail", _shardID, _type, _instID, _old, _new)
		}
		return
	}
	connState := sl.mConnStates[_shardID][HeraWorkerType(_type)][_instID]
	//
	// idle and assign are global states shared by all instances
	//
	if _new == Idle || _new == Assign {
		for s := 0; s < sl.maxShardSize; s++ {
			for t := 0; t < int(wtypeTotalCount); t++ {
				instCnt := len(sl.mConnStates[s][HeraWorkerType(t)])
				for n := 0; n < instCnt; n++ {
					sl.mConnStates[s][HeraWorkerType(t)][n].perStateCnt[_new]++
				}
			}
		}
	} else {
		connState.perStateCnt[_new]++
	}

	if _old == Idle || _old == Assign {
		for s := 0; s < sl.maxShardSize; s++ {
			for t := 0; t < int(wtypeTotalCount); t++ {
				instCnt := len(sl.mConnStates[s][HeraWorkerType(t)])
				for n := 0; n < instCnt; n++ {
					sl.mConnStates[s][HeraWorkerType(t)][n].perStateCnt[_old]--
				}
			}
		}
	} else {
		connState.perStateCnt[_old]--
	}
}

// setWorkerState changes the worker state. It also helps keeping the counters for the number of requests and responses,
// basically by counting the transitions to and from the BUSY state
func (sl *StateLog) setWorkerState(_shardID int, _type HeraWorkerType, _instID int, _workerID int, _newState HeraWorkerStatus) {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "statelog setworkerstate", _shardID, _type, _instID, _workerID, _newState)
	}

	if (_newState < 0) || (_newState >= MaxWorkerState) {
		return
	}

	//
	// during worker resize, an event with original worker index could still arrive.
	//
	var wtStates = sl.mWorkerStates[_shardID][HeraWorkerType(_type)]
	if _workerID >= len(wtStates[_instID]) {
		return
	}

	workerState := sl.mWorkerStates[_shardID][HeraWorkerType(_type)][_instID][_workerID]

	if workerState.state == wsBusy {
		workerState.resp++
	}
	if _newState == wsBusy {
		workerState.req++
	}
	workerState.state = _newState
}

// genReport builds the state log report and outputs to the state log and to CAL
func (sl *StateLog) genReport() {
	if sl.fileLogger == nil {
		return
	}

	sl.mWriteHeader--
	if sl.mWriteHeader <= 0 {
		sl.fileLogger.Println(getTime() + sl.mStateHeader)
		sl.mWriteHeader = sl.mWriteHeaderInterval
	}

	//totalConnections := sl.GetTotalConnections()
	for s := 0; s < sl.maxShardSize; s++ {
		for t := 0; t < int(wtypeTotalCount); t++ {
			instCnt := len(sl.mWorkerStates[s][HeraWorkerType(t)])
			//
			// one line for each worker pool (rw, ro, cache, stndby1, stndby2, stndby3).
			//
			for n := 0; n < instCnt; n++ {
				workerCnt := len(sl.mWorkerStates[s][HeraWorkerType(t)][n])
				if workerCnt == 0 {
					continue
				}

				workerStateInfoData := otel_logger.WorkerStateInfo{
					StateTitle: sl.workerDimensionTitle[sl.mTypeTitles[s][HeraWorkerType(t)][n]],
					ShardId:    s,
					WorkerType: t,
					InstanceId: n,
				}
				// Initialize statedata object
				workerStatesData := otel_logger.WorkersStateData{
					WorkerStateInfo: &workerStateInfoData,
					StateData:       make(map[string]int64),
				}

				//
				// count all request/response for all workers under the instance
				//
				var reqCnt int64
				var respCnt int64
				//
				// array to collect state counts for one statusline.
				//
				stateCnt := make([]int, MaxWorkerState+MaxConnState)

				//
				// buffer to construct a string for one statusline.
				//
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("%-11s", sl.mTypeTitles[s][HeraWorkerType(t)][n]))
				//
				// collect worker states.
				//
				for w := 0; w < workerCnt; w++ {
					workerState := sl.mWorkerStates[s][HeraWorkerType(t)][n][w]
					stateCnt[workerState.state]++
					reqCnt += workerState.req
					respCnt += workerState.resp
				}
				//
				// collect conn states.
				//

				for c := 0; c < MaxConnState; c++ {
					stateCnt[MaxWorkerState+c] = sl.mConnStates[s][HeraWorkerType(t)][n].perStateCnt[c]
					//logger.GetLogger().Log(logger.Verbose,"gen report",c,stateCnt[MaxWorkerState + c])
					//
					// internally we have a waterproof accounting that may temporarily
					// having one state count showing negative. but it is eventually
					// zero-summed. to avoid the eyesore, keep negative as 0.
					//
					if stateCnt[MaxWorkerState+c] < 0 {
						stateCnt[MaxWorkerState+c] = 0
					}
				}

				//Send statelog data to OTEL statsdata channel
				if otelconfig.OTelConfigData.Enabled {
					for i := 0; i < (MaxWorkerState + MaxConnState - 1); i++ {
						buf.WriteString(fmt.Sprintf("%6d", stateCnt[i]))
						workerStatesData.StateData[StateNames[i]] = int64(stateCnt[i])
					}
					//Adding req and response metrics to OTEL
					workerStatesData.StateData["req"] = reqCnt - sl.mLastReqCnt[s][HeraWorkerType(t)][n]
					workerStatesData.StateData["resp"] = respCnt - sl.mLastRspCnt[s][HeraWorkerType(t)][n]

					//Total workers
					workerStatesData.StateData["totalConnections"] = int64(sl.workerPoolCfg[s][HeraWorkerType(t)].maxWorkerCnt)
					totalConectionData := otel_logger.GaugeMetricData{
						WorkerStateInfo: &workerStateInfoData,
						StateData:       workerStatesData.StateData["totalConnections"],
					}
					go otel_logger.AddDataPointToOTELStateDataChan(&workerStatesData)
					go otel_logger.AddDataPointToTotalConnectionsDataChannel(&totalConectionData)
				} else {
					for i := 0; i < (MaxWorkerState + MaxConnState - 1); i++ {
						buf.WriteString(fmt.Sprintf("%6d", stateCnt[i]))
					}
				}

				if !otelconfig.OTelConfigData.Enabled || (otelconfig.OTelConfigData.Enabled && !otelconfig.OTelConfigData.SkipCalStateLog) {
					// write collection into calheartbeat(cased out) and log (oneline).
					//If enable_otel_metrics_only not enabled then it sends CAL heart beat event or else send data to file and OTEL agent
					hb := cal.NewCalHeartBeat("STATE", sl.mTypeTitles[s][HeraWorkerType(t)][n], cal.TransOK, "")
					for i := 0; i < (MaxWorkerState + MaxConnState - 1); i++ {
						hb.AddDataInt(StateNames[i], int64(stateCnt[i]))
					}
					hb.AddDataInt("req", int64(reqCnt-sl.mLastReqCnt[s][HeraWorkerType(t)][n]))
					hb.AddDataInt("resp", int64(respCnt-sl.mLastRspCnt[s][HeraWorkerType(t)][n]))
					hb.Completed()
				}
				sl.fileLogger.Println(getTime() + buf.String())

				sl.mLastReqCnt[s][HeraWorkerType(t)][n] = reqCnt
				sl.mLastRspCnt[s][HeraWorkerType(t)][n] = respCnt
			} // instance// instance
		} // wtype
	} // sharding
}
