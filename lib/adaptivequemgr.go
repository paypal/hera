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
	"math/rand"
	"sync/atomic"
	"strings"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

/**
 * backlogs are stoppped inside each calling routine that tries to acquire a workerclient. as a result,
 * golang does not require a physical adaptive queue. instead, we use a manager with attributes like last_empty_time,
 * recover_frequency, long_timeout, short_timeout, evicted_sql, etc. before a request enters the
 * backlog, we check the adaptive status and saturation recovery criteria to perform different
 * actions. since these actions are performed on a goroutine that is about to be put into sleep
 * anyway, we are not blocking processing path of the mux. the entire logic in this manager
 * could have been scattered inside workerpool's getworker, but we collected them in this separate
 * module for better read. that is why workerpool and aqmanager each has a reference of the other.
 *
 * there is a separate go routine in this manager to do periodical saturation recovery. the
 * go routine uses the same lock in its owner workerpool to steps through the running workers.
 * we block other get/return workers calls during the short recovery window. Sql response time
 * in each dispatched workerclient are tracked inside each workerclient. the workerclient will track a
 * more coarse window between sending to worker an ns having a prepare command and receiving an eor in workerclient.
 * evicted sqlhash is registered before an ns is sent to worker. if external client sends another
 * sql before receiving all the results from the ongoing sql, we could register a wrong sqlhash.
 * but we assume external client follows standard hera ns protocol to not sending new sql before
 * all the results are received.
 *
 * this struct is not thread safe. the way it works in mux is that each workerpool has one
 * adaptivequeuemanager that is protected by the same poolCond lock used to guard workerclients.
 * using a single lock can avoid deadlocks caused by crossed up locking on multiple locks.
 */
type adaptiveQueueManager struct {
	//
	// one adaptive queue per worker pool. workerpool who owns this manager
	//
	wpool *WorkerPool
	//
	// last time backlog becomes empty, e.g. last entry leaves backlog. (ms since epoch).
	//
	lastEmptyTimeMs int64
	//
	// long timeout in adaptive queue (200 ms by default)
	//
	longTimeoutMs int
	//
	// short timeout in adaptive queue (5 ms by default)
	//
	shortTimeoutMs int
	//
	// collection of evicted sqlhash. value is eviction time (epoch in millisecond)
	//
	evictedSqlhash map[int32]int64
	//
	// map of dispatched workerclients to ticket. it is used to to find worker response
	// time and force worker to break out of long query during saturation recovery.
	// no thread safe. should only be called using the same lock for getting returning workers.
	//
	dispatchedWorkers map[*WorkerClient]string
}

func (mgr *adaptiveQueueManager) init(wpool *WorkerPool) error {
	if wpool == nil {
		return errors.New("adaptivequeuemgr received a nil workerpool")
	}
	mgr.wpool = wpool

	mgr.longTimeoutMs = GetConfig().BacklogTimeoutMsec
	mgr.shortTimeoutMs = GetConfig().ShortBacklogTimeoutMsec

	mgr.lastEmptyTimeMs = (time.Now().UnixNano() / int64(time.Millisecond))
	mgr.evictedSqlhash = make(map[int32]int64)
	mgr.dispatchedWorkers = make(map[*WorkerClient]string)

	threhold := GetSatRecoverThresholdMs()
	if (threhold > 2000000000) && logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "starting with hugh sat recovery threshold", threhold)
	}

	//if GetSatRecoverThrottleRate() > 0	{
	go mgr.runSaturationRecovery()
	//}
	// TODO assuming somewhere else rand is already seeded.
	//rand.Seed(time.Now().UTC().UnixNano())

	return nil
}

// used in doBindEviction() to find hot bind values
type BindCount struct {
	Sqlhash uint32
	Name    string
	Value   string                   // should be long enough >=6-9 to avoid status fields
	Workers map[string]*WorkerClient // lookup by ticket
}

func bindEvictNameOk(bindName string) (bool) {
	commaNames := GetConfig().BindEvictionNames
	if len(commaNames) == 0 {
		// for tests, allow all names to be subject to bind eviction
		return true
	}
	commaNames = strings.ToLower(commaNames)
	bindName = strings.ToLower(bindName)
	for _, okSubname := range strings.Split(commaNames,",") {
		if strings.Contains(bindName, okSubname) {
			return true
		}
	}
	return false
}

/* A bad query with multiple binds will add independent bind throttles to all
bind name and values */
func (mgr *adaptiveQueueManager) doBindEviction() (int) {
	throttleCount := 0
	for _,keyValues := range GetBindEvict().BindThrottle {
		throttleCount += len(keyValues)
	}
	if throttleCount > GetConfig().BindEvictionMaxThrottle {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "already too many bind throttles, skipping bind eviction and throttle")
		}
		return 0
	}

	bindCounts := make(map[string]*BindCount)
	mgr.wpool.poolCond.L.Lock()
	defer mgr.wpool.poolCond.L.Unlock()
	for worker, ticket := range mgr.dispatchedWorkers {
		if worker == nil {
			continue
		}
		usqlhash := uint32(worker.sqlHash)
		sqlhash := atomic.LoadUint32(&(usqlhash))
		_, ok := GetBindEvict().BindThrottle[sqlhash]
		if ok {
			continue // don't repeatedly bind evict something already evicted
		}
		request, ok := worker.sqlBindNs.Load().(*netstring.Netstring)
		if !ok {
			if logger.GetLogger().V(logger.Alert) {
				logger.GetLogger().Log(logger.Alert, "bad req netstring, skipping bind evict eval, pid", worker.pid)
			}
			continue
		}
		contextBinds := parseBinds(request)
		for bindName0, bindValue := range contextBinds {
			/* avoid too short status values
			D=deleted, P=pending, C=confirmed
			US Zip Codes: 90210, 95131
			we want account id's, phone number, or full emails
			easiest just to check length */
			if len(bindValue) <= 7 {
				continue
			}

			/* select * from .. where id in ( :bn1, :bn2, bn3.. )
			bind names are all normalized to bn#
			bind values may repeat */
			bindName := NormalizeBindName(bindName0)
			if !bindEvictNameOk(bindName) {
				continue
			}
			concatKey := fmt.Sprintf("%d|%s|%s", sqlhash, bindName, bindValue)

			entry, ok := bindCounts[concatKey]
			if !ok {
				entry = &BindCount{
					Sqlhash: sqlhash,
					Name:    bindName,
					Value:   bindValue,
					Workers: make(map[string]*WorkerClient),
					}
				bindCounts[concatKey] = entry
			}

			entry.Workers[ticket] = worker
		}
	} // end for worker search

	evictedTicket := make(map[string]string)

	numDispatchedWorkers := len(mgr.dispatchedWorkers)
	evictCount := 0
	for _, entry := range bindCounts {
		sqlhash := entry.Sqlhash
		bindName := entry.Name
		bindValue := entry.Value

		if len(entry.Workers) < int( float64(GetConfig().BindEvictionThresholdPct)/100.*float64(numDispatchedWorkers) ) {
			continue
		}
		// evict sqlhash, bindvalue
		//for idx := 0; idx < len(entry.Workers); idx++  {
		for ticket, worker := range entry.Workers {
			_, ok := evictedTicket[ticket]
			if ok {
				continue
			}
			evictedTicket[ticket] = ticket

			if mgr.dispatchedWorkers[worker] != ticket ||
				worker.Status == wsFnsh ||
				worker.isUnderRecovery == 1 /* Recover() uses compare & swap */ {

				continue
			}

			// do eviction
			select {
			case worker.ctrlCh <- &workerMsg{data: nil, free: false, abort: true, bindEvict: true}:
			default:
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "failed to publish abort msg (bind eviction)", worker.pid)
				}
			}
			et := cal.NewCalEvent("BIND_EVICT", fmt.Sprintf("%d", entry.Sqlhash),
				"1", fmt.Sprintf("pid=%d&k=%s&v=%s", worker.pid, entry.Name, entry.Value))
			et.Completed()
			evictCount++
		}

		// setup allow-every-x
		sqlBind, ok := GetBindEvict().BindThrottle[sqlhash]
		if !ok {
			sqlBind = make(map[string]*BindThrottle)
			GetBindEvict().BindThrottle[sqlhash] = sqlBind
		}
		concatKey := fmt.Sprintf("%s|%s", bindName, bindValue)
		throttle, ok := sqlBind[concatKey]
		if ok {
			throttle.incrAllowEveryX()
		} else {
			throttle := BindThrottle{
				Name:          bindName,
				Value:         bindValue,
				Sqlhash:       sqlhash,
				AllowEveryX:   3*len(entry.Workers) + 1,
			}
			now := time.Now()
			throttle.RecentAttempt.Store(&now)
			sqlBind[concatKey] = &throttle
		}
	}
	return evictCount
}

/**
 * saturation recovery loop wake up every second (default).
 */
func (mgr *adaptiveQueueManager) runSaturationRecovery() {
	for {
		//
		// reload sleep every loop to pick up any runtime config change.
		//
		sleep := GetSatRecoverFreqMs(mgr.wpool.ShardID)
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "saturation recover enter (ms)", sleep)
		}
		if sleep == int(^uint(0)>>1) {
			//
			// need to periodically check whether recovery config is activated.
			//
			for sleep == int(^uint(0)>>1) {
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, "saturation_recover check (ms)", sleep)
				}
				time.Sleep(time.Second * 30)
				sleep = GetSatRecoverFreqMs(mgr.wpool.ShardID)
			}
		}
		//
		// recovery change willnot be picked up until the old sleeping time has done.
		//
		time.Sleep(time.Millisecond * time.Duration(sleep))

		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "saturation recover active (ms)", sleep)
		}
		if mgr.shouldRecover() && mgr.doBindEviction() == 0 {
			//
			// once we decided to recover a worker and send an abort msg through worker.ctrlCh,
			// one of three things could happen
			// 1) coordinator sits in dosession and takes our msg to exit with ErrSaturationKill. it
			//    will close external client to complete the session. it will also return worker
			//    a SaturationKill ns is sent to client
			// 2) dosession just finished and we block it from returning worker. abort message
			//    is left in ctrlcha. it will be drained by returnworker or the next getworker.
			//    if not, it will be consumed by the next client, essentially we saturation recover
			//    the next client which preserves most of the old behaviors. no chain reaction.
			// 3) if coordinator returned a worker (happy or unhappy path) before we have the
			//    lock, the returned worker should already be removed from our dispatchedWorkers map
			//    before we get the lock to loop through dispatchedWorkers.
			//
			mgr.wpool.poolCond.L.Lock()
			workerclient, ok := mgr.getWorkerToRecover()
			if ok {
				if workerclient != nil && workerclient.Status != wsFnsh { // Dont recover the worker which has already completed the work
					//
					// can not use worker.outCh since doRead thread could close outCha anytime.
					// ctrlCh is destroyed in RestartWorker, which requies lock. so we will not
					// write into a closed channel
					//
					select {
					case workerclient.ctrlCh <- &workerMsg{data: nil, free: false, abort: true}:
					default:
						if logger.GetLogger().V(logger.Warning) {
							logger.GetLogger().Log(logger.Warning, "failed to publish abort msg", workerclient.pid)
						}
					}
					mgr.addEvictedSqlhash(atomic.LoadInt32(&(workerclient.sqlHash)))
					mgr.wpool.poolCond.L.Unlock()

					et := cal.NewCalEvent("HARD_EVICTION", fmt.Sprintf("%d", uint32(workerclient.sqlHash)),
						"1", fmt.Sprintf("pid=%d", workerclient.pid))
					et.Completed()

					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "heraproxy saturation recover: sql will be terminated in child", workerclient.pid, "while assigned to client, close client connection", workerclient.Type)
					}
				} else {
					if workerclient != nil && workerclient.Status == wsFnsh {
						logger.GetLogger().Log(logger.Warning, "Skipping worker for recovery, pid:", workerclient.pid, " as the state is :", workerclient.Status)
					} else {
						//
						// should not happen since getTicketToRecover already checks nil worker
						//
						et := cal.NewCalEvent(cal.EventTypeMessage, "SatRecoverNilWorker", cal.TransOK, "sqltime within threshold, ignore sat recover")
						et.Completed()
					}
					mgr.wpool.poolCond.L.Unlock()
				}
			} else {
				et := cal.NewCalEvent(cal.EventTypeMessage, "SatRecoverBypass", cal.TransOK, "sqltime within threshold, ignore sat recover")
				et.Completed()
				mgr.wpool.poolCond.L.Unlock()
			}
		}
	}
}

func (mgr *adaptiveQueueManager) shouldRecover() bool {
	//
	// throttle rate disabled.
	//
	if GetSatRecoverThrottleRate() == 0 {
		return false
	}
	//
	// no backlog
	//
	bgcnt := int(atomic.LoadInt32(&(mgr.wpool.backlogCnt)))
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "shouldRecover bgcnt >0", bgcnt)
	}
	if bgcnt == 0 {
		return false
	}
	//
	// too many stranded worker already
	//
	swcnt := GetStateLog().GetStrandedWorkerCountForPool(mgr.wpool.ShardID, mgr.wpool.Type, mgr.wpool.InstID)
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "shouldRecover swcnt <=", swcnt, GetSatRecoverThrottleCnt(mgr.wpool.ShardID))
	}
	if swcnt > GetSatRecoverThrottleCnt(mgr.wpool.ShardID) {
		return false
	}
	//
	// not enough backlog.
	//
	bglimit := GetConfig().GetBacklogLimit(mgr.wpool.Type, mgr.wpool.ShardID)
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "shouldRecover bglimit >=+", bgcnt, bglimit, swcnt)
	}
	if bgcnt < (bglimit + swcnt) {
		return false
	}

	return true
}

/**
 * find the oldest tenant in dispatchedworkers.
 */
func (mgr *adaptiveQueueManager) getWorkerToRecover() (*WorkerClient, bool) {
	//
	// both nowms and worker.sqlStartTimeMs are offset from getstatelog().getstarttime().
	//
	nowms := uint32((time.Now().UnixNano() - GetStateLog().GetStartTime()) / int64(time.Millisecond))
	threhold := GetSatRecoverThresholdMs()
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "getWorkerToRecover nowms", nowms, threhold)
	}

	var sqltime uint32
	var outworker *WorkerClient
	var found = false
	for worker, ticket := range mgr.dispatchedWorkers {
		var runtime uint32
		if worker != nil && worker.Status != wsFnsh { // Dont recover the worker which has already completed the work
			stime := atomic.LoadUint32(&(worker.sqlStartTimeMs))
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "getWorkerToRecover stime", stime)
			}
			if stime != 0 {
				if nowms < stime {
					//
					// TODO should we use this c++ behavior or just skip this worker
					//
					runtime = (^uint32(0)) - (stime - nowms) + 1
				} else {
					runtime = nowms - stime
				}
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, "getWorkerToRecover", runtime, threhold, sqltime)
				}
				if runtime > threhold && runtime > sqltime {
					sqltime = runtime
					outworker = worker
					found = true
				}
			}
		} else {
		if worker != nil && worker.Status == wsFnsh  {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "worker.pid state is in FNSH, so skipping", worker.pid)
			}
		} else {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "removing nil worker in aq for ticket", ticket)
			}
			//
			// deleting nil from map works as usual.
			//
			delete(mgr.dispatchedWorkers, worker)
		}
		}
	}
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "getWorkerToRecover return", outworker, found)
	}
	//
	// this worker will be recovered anyway, reset sqlstarttime to 0 to prevent recover again.
	//
	if outworker != nil && outworker.sqlStartTimeMs != 0 { // // Dont recover the worker which has already completed the work
		atomic.StoreUint32(&(outworker.sqlStartTimeMs), 0)
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "getWorkerToRecover return", outworker, found)
		}
	} else {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "getWorkerToRecover skipping worker :", outworker)
		}
		outworker = nil
		found = false
	}

	return outworker, found
}

/**
 * before adding a request to the backlog, check to see if it
 * is one of the long running queries that needs to be bounced right away.
 */
func (mgr *adaptiveQueueManager) shouldSoftEvict(sqlhash int32) bool {
	if len(mgr.evictedSqlhash) == 0 {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "shouldsoftevict emptyhash true")
		}
		return false
	}

	//
	// random threshold.
	//
	probability := GetConfig().SoftEvictionProbability
	if probability == 0 {
		return false
	}
	if probability < 100 {
		random := rand.Intn(100)
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "shouldsoftevict probability >", probability, random)
		}
		if random >= probability {
			return false
		}
	}
	//
	// not on blacklist
	//
	lastEvictTime := mgr.evictedSqlhash[sqlhash]
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "shouldsoftevict hashmatch >0", lastEvictTime)
	}
	if lastEvictTime == 0 {
		return false
	}
	//
	// sqlhash has been on blacklist for too long.
	//
	jailtime := int64(GetConfig().SoftEvictionEffectiveTimeMs)
	var now = time.Now().UnixNano() / int64(time.Millisecond)
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "shouldsoftevict jailtime >-", lastEvictTime, now, jailtime)
	}
	if lastEvictTime < (now - jailtime) {
		//
		// remove from blacklist. next saturation recovery will pick and add a new slow sql.
		//
		delete(mgr.evictedSqlhash, sqlhash)
		return false
	}

	return true
}

/**
 * add hash of an sql just evicted by saturation recovery.
 */
func (mgr *adaptiveQueueManager) addEvictedSqlhash(sqlhash int32) {
	mgr.evictedSqlhash[sqlhash] = time.Now().UnixNano() / int64(time.Millisecond)
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "addevictsqlhash ", uint32(sqlhash), mgr.evictedSqlhash[sqlhash])
	}
}

func (mgr *adaptiveQueueManager) clearAllEvictedSqlhash() {
	//
	// gc will clean up the old map
	//
	mgr.evictedSqlhash = make(map[int32]int64)
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "clearallevictsqlhash")
	}
}

/**
 * add worker with ticket to dispatchedWorkers.
 */
func (mgr *adaptiveQueueManager) registerDispatchedWorker(ticket string, worker *WorkerClient) {
	mgr.dispatchedWorkers[worker] = ticket
}

/**
 * delete worker from dispatchedworkers.
 */
func (mgr *adaptiveQueueManager) unregisterDispatchedWorker(worker *WorkerClient) {
	delete(mgr.dispatchedWorkers, worker)
}

/**
 * check whether ticket is already used (for the same or a different worker. corrpution either way)
 * check whether worker is already dispatched under a different ticket.
 */
func (mgr *adaptiveQueueManager) alreadyDispatched(ticket string, worker *WorkerClient) bool {
	if mgr.dispatchedWorkers[worker] != "" {
		return true
	}
	//
	// small map, should be fast.
	// same ticket was used by a different worker.
	// @TODO we permenantly loose a workerclient. should we ignore same ticket for different worker
	//
	for _, t := range mgr.dispatchedWorkers {
		if ticket == t {
			return true
		}
	}
	return false
}

/**
 * decide whether to use the long timeout or the short timeout.
 *
 * @return timeout value and whether it is long timeout or short timeout.
 */
func (mgr *adaptiveQueueManager) getBacklogTimeout() (int, bool) {
	//
	// if backlog is empty, return long timeout.
	//
	blgsize := atomic.LoadInt32(&(mgr.wpool.backlogCnt))
	if blgsize == 0 {
		return mgr.longTimeoutMs, true
	}
	var now = time.Now().UnixNano() / int64(time.Millisecond)
	//
	// adaptive logic. lastEmptyTimeMs is protected by lock inside workerpool.
	//
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "adaptiveQueueManager getBacklogTimeout. lastEmptyTimeMs", mgr.lastEmptyTimeMs, " now", now)
	}
	if mgr.lastEmptyTimeMs < (now - int64(mgr.longTimeoutMs)) {
		return mgr.shortTimeoutMs, false
	}
	return mgr.longTimeoutMs, true
}
