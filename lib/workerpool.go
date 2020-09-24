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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
)

var bcklgEvtPrefix = [wtypeTotalCount]string{
	"BKLG", "BKLG_R", "BKLG_S"}
var bcklgTimeoutEvtPrefix = [wtypeTotalCount]string{
	"bklg", "bklg_r", "bklg_s"}
var poolNamePrefix = [wtypeTotalCount]string{
	"write", "readonly", "standby"}

// WorkerPool represents a pool of workers of the same kind
// the implementation uses a C++-ish mutex/condition variable/queue rather than a Golang-ish channel + timer
// because the policy for using the worker is LIFO (for better usage of cache) while the channels are FIFO
type WorkerPool struct {
	//
	// a locking condition used to wait/notify on empty/replenish pool
	// this lock and the two worker queues are supposed to be a private member fields
	//
	poolCond *sync.Cond
	activeQ  Queue // a queue with active worker client ready to serve traffic

	ShardID int            // the shard the workers are connected to
	Type    HeraWorkerType // the worker type like write, read
	InstID  int

	// interval for checking on worker lifespan
	lifeSpanCheckInterval int

	currentSize int // the number of workers in the pool
	desiredSize int // the desired number of workers in the pool, usually equal to currentSize, different for a
	// brief period when the pool is dynamically resized

	moduleName string // basically the application name as it comes from the command line
	// the number of worker not in INIT state, atomically maintained
	numHealthyWorkers int32

	//
	// number of requests in the backlog. we could lock operation to publish state event, but
	// status updates after the publishing call inside state log is not inside lock.
	// use atomic to synchronize this number.
	//
	backlogCnt int32

	//
	// caller receives a ticket when getting a workerclient. caller returns workerclient
	// together with the ticket to ensure workerclient is returned by the same caller
	// only once.
	//
	checkoutTickets map[interface{}]string
	//
	// adaptive queue manager to decide on long/short timeouts and saturation recovery.
	//
	aqmanager *adaptiveQueueManager

	// the actual list of workers
	workers []*WorkerClient
	// Throtle workers lifecycle
	thr Throttler
}

// Init creates the pool by creating the workers and making all the initializations
func (pool *WorkerPool) Init(wType HeraWorkerType, size int, instID int, shardID int, moduleName string) error {
	pool.Type = wType
	pool.activeQ = NewQueue()
	//pool.poolCond = &sync.Cond{L: &sync.Mutex{}}
	pool.poolCond = sync.NewCond(&sync.Mutex{})
	pool.lifeSpanCheckInterval = GetConfig().lifeSpanCheckInterval
	pool.InstID = instID
	pool.ShardID = shardID
	pool.currentSize = 0
	pool.desiredSize = size
	pool.moduleName = moduleName
	pool.workers = make([]*WorkerClient, size)
	pool.thr = NewThrottler(uint32(GetConfig().MaxDbConnectsPerSec), fmt.Sprintf("%d_%d_%d", wType, shardID, instID))
	for i := 0; i < size; i++ {
		err := pool.spawnWorker(i)
		if err != nil {
			return err
		}
		pool.currentSize++
	}
	pool.checkoutTickets = make(map[interface{}]string)
	pool.aqmanager = &adaptiveQueueManager{}
	err := pool.aqmanager.init(pool)
	go pool.checkWorkerLifespan()
	return err
}

// spawnWorker starts a worker and spawn a routine waiting for the "ready" message
func (pool *WorkerPool) spawnWorker(wid int) error {
	worker := NewWorker(wid, pool.Type, pool.InstID, pool.ShardID, pool.moduleName, pool.thr)
	er := worker.StartWorker()
	if er != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "failed starting worker: ", er)
		}
		// called from Init will see error
		// called from RestartWorkerPool can retry when workers don't come up
		return er
	}
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "worker started type ", pool.Type, " id", worker.ID, " instid", pool.InstID, " shardid", pool.ShardID)
	}
	//
	// after establishing uds with the worker, it will be add to active queue
	//
	// oracle connect errors show up in attach worker
	go worker.AttachToWorker()
	return nil
}

// RestartWorker is called after a worker exited to perform the necessary cleanup and re-start a new worker.
// In the rare situation where the pool need to be down-sized a new worker is not restarted.
func (pool *WorkerPool) RestartWorker(worker *WorkerClient) (err error) {
	if worker == nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "WorkerReady nil, size=", pool.activeQ.Len(), "type=", pool.Type)
		}
		return nil
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "RestartWorker(): ", pool.Type, pool.desiredSize, pool.currentSize, worker.pid, worker.ID)
	}
	pool.poolCond.L.Lock()
	//
	// release terminated workerclient (and fd inside) if we havenot done it yet.
	//
	delete(pool.checkoutTickets, worker)
	pool.aqmanager.unregisterDispatchedWorker(worker)

	if worker.ID >= pool.desiredSize /*we resize by terminating worker with higher ID*/ {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "Pool type=", pool.Type, ", worker=", worker.pid, "exited, new one not started because pool was resized:", pool.currentSize, "->", pool.desiredSize)
		}
		pool.currentSize--
		if pool.desiredSize == pool.currentSize {
			//
			// let statlog resets the worker size
			//
			GetStateLog().PublishStateEvent(StateEvent{eType: WorkerResizeEvt, shardID: pool.ShardID, wType: pool.Type, instID: pool.InstID, newWSize: pool.currentSize})
		}
		pool.activeQ.Remove(worker)
		pool.poolCond.L.Unlock()
		return
	}
	pool.activeQ.Remove(worker)
	pool.poolCond.L.Unlock()

	er := pool.spawnWorker(worker.ID)
	//
	// @TODO retry instead of stop the whole startup process
	//
	if er != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "failed starting worker: ", er)
		}
		pool.poolCond.L.Lock()
		pool.currentSize--
		pool.poolCond.L.Unlock()
		return er
	}
	return nil
}

// WorkerReady is called after the worker started and become available. It puts the worker into the internal list
// of workers as well as in the list of available workers
func (pool *WorkerPool) WorkerReady(worker *WorkerClient) (err error) {
	if worker == nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "WorkerReady nil, size=", pool.activeQ.Len(), "type=", pool.Type)
		}
		return nil
	}

	pool.poolCond.L.Lock()
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Pool::WorkerReady", worker.pid, worker.Type, worker.instID)
	}

	pool.activeQ.Push(worker)
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "poolsize(ready)", pool.activeQ.Len(), " type ", pool.Type, " instance ", pool.InstID)
	}
	pool.workers[worker.ID] = worker

	pool.poolCond.L.Unlock()
	//
	// notify one waiting agent on the availability of a new worker in the pool
	//
	pool.poolCond.Signal()
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "poolsize (after signal)", pool.activeQ.Len(), " type ", pool.Type)
	}
	return nil
}

// GetWorker gets the active worker if available. backlog with timeout if not.
//
// @param sqlhash to check for soft eviction against a blacklist of slow queries.
//        if getworker needs to exam the incoming sql, there does not seem to be another elegant
//        way to do this except to pass in the sqlhash as a parameter.
// @param timeoutMs[0] timeout in milliseconds. default to adaptive queue timeout.
func (pool *WorkerPool) GetWorker(sqlhash int32, timeoutMs ...int) (worker *WorkerClient, t string, err error) {
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Pool::GetWorker(start) type:", pool.Type, ", instance:", pool.InstID, ", active: ", pool.activeQ.Len(), "healthy:", pool.GetHealthyWorkersCount())
	}
	defer func() {
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "Pool::GetWorker(end) type:", pool.Type, ", instance:", pool.InstID, ", active: ", pool.activeQ.Len(), "healthy:", pool.GetHealthyWorkersCount())
		}
	}()
	pool.poolCond.L.Lock()

	var workerclient = pool.getActiveWorker()
	for workerclient == nil {
		if pool.GetHealthyWorkersCount() == 0 {
			msg := fmt.Sprintf("REJECT_DB_DOWN_%s%d", poolNamePrefix[pool.Type], pool.InstID)
			e := cal.NewCalEvent(cal.EventTypeWarning, msg, cal.TransOK, "")
			e.AddDataInt("sql_hash", int64(uint32(sqlhash)))
			e.Completed()
			pool.poolCond.L.Unlock()
			return nil, "", ErrRejectDbDown
		}

		timeout, longTo := pool.aqmanager.getBacklogTimeout()
		if len(timeoutMs) > 0 {
			timeout = timeoutMs[0]
		}

		if timeout == 0 {
			// no bklg events!
			pool.poolCond.L.Unlock()
			return nil, "", errors.New("no worker available")
		}
		//
		// check if we need to evict sql with hash=sqlhash.
		//
		if pool.aqmanager.shouldSoftEvict(sqlhash) {
			pool.poolCond.L.Unlock()
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "soft sql eviction, sql_hash=", uint32(sqlhash))
			}
			e := cal.NewCalEvent("SOFT_EVICTION", fmt.Sprint(uint32(sqlhash)), cal.TransOK, "")
			e.Completed()
			return nil, "", ErrSaturationSoftSQLEviction
		}
		//
		// c++ has a REJECT_DB_DOWN check which is mostly an attempt to prevent backlog
		// overflow. but bouncer's connection check should have done that already.
		// as a result, we do not implment REJECT_DB_DOWN in golang.
		//
		// client connection can not get an active worker. put it in backlog
		//
		blgsize := atomic.LoadInt32(&(pool.backlogCnt))
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "add to backlog. type:", pool.Type, ", instance:", pool.InstID, " timeout:", timeout, ", blgsize:", blgsize)
		}
		if blgsize == 0 {
			pool.aqmanager.lastEmptyTimeMs = (time.Now().UnixNano() / int64(time.Millisecond))
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "setlastempty(enter)", pool.aqmanager.lastEmptyTimeMs)
			}
		}
		atomic.AddInt32(&(pool.backlogCnt), 1)
		GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: pool.ShardID, wType: pool.Type, instID: pool.InstID, oldCState: Idle, newCState: Backlog})

		//
		// go refused to add a wait timeout https://github.com/golang/go/issues/9578
		// wakeup chann return the time this thread spent in the backlog doghouse.
		//
		wakeupchann := make(chan int64)
		go func(cond *sync.Cond) {
			startTime := time.Now().UnixNano()
			//
			// if caller times out and goes away, we need to unlock after waking up.
			//
			cond.Wait()
			cond.L.Unlock()
			//
			// if backlog times out on this channel, noone will be listening on the other
			// end, which blocks a write to wakeupchann. to avoid dangling thread, use unblocking.
			// if notify wakes up such a thread, other thread in backlog will need another notify
			// to wake up even there is already one free worker in the pool. worker are getting
			// returned consistenly, so we are not worried about this little delay. cond.broadcast
			// may resolve the delay in this corner case, but letting all waiting threads to race
			// for one free worker, with one winner and the rest loopin back into wait is the cost.
			//
			select {
			case wakeupchann <- ((time.Now().UnixNano() - startTime) / int64(time.Millisecond)):
			default:
			}
			close(wakeupchann)
		}(pool.poolCond)

		select {
		case <-time.After(time.Millisecond * time.Duration(timeout)):
			//
			// lock to protect accessing clearAllEvictedSqlhash
			//
			pool.poolCond.L.Lock()
			pool.resetIfLastBacklogEntry("timeout")
			pool.decBacklogCnt()
			pool.poolCond.L.Unlock()
			//
			// backlog timeout. change connstate to idle, and return error.
			// caller will close client connection that takes connstate
			// further from idle to close.
			//
			GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: pool.ShardID, wType: pool.Type, instID: pool.InstID, oldCState: Backlog, newCState: Idle})

			//
			// log a backlog timeout event.
			//
			msg := fmt.Sprintf("timeout %d no idle child & req %s%d backlog timed out, close client connection", timeout, poolNamePrefix[pool.Type], pool.InstID)
			var ename string
			if longTo {
				if GetConfig().EnableSharding {
					ename = fmt.Sprintf("%s%d_shd%d_timeout", bcklgTimeoutEvtPrefix[pool.Type], pool.InstID, pool.ShardID)
				} else {
					ename = fmt.Sprintf("%s%d_timeout", bcklgTimeoutEvtPrefix[pool.Type], pool.InstID)
				}
			} else {
				if GetConfig().EnableSharding {
					ename = fmt.Sprintf("%s%d_shd%d_eviction", bcklgTimeoutEvtPrefix[pool.Type], pool.InstID, pool.ShardID)
				} else {
					ename = fmt.Sprintf("%s%d_eviction", bcklgTimeoutEvtPrefix[pool.Type], pool.InstID)
				}
			}
			e := cal.NewCalEvent(cal.EventTypeWarning, ename, cal.TransOK, msg)
			e.Completed()
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "backlog timeout. type:", pool.Type, ", instance:", pool.InstID)
			}
			//
			// we are bailing out. but the waiting routine is still sleeping.
			//
			pool.poolCond.Signal() // try to jostle the waiting routine free
			if longTo {
				return nil, "", ErrBklgTimeout
			}
			return nil, "", ErrBklgEviction
		case sleepingtime, _ := <-wakeupchann:
			pool.poolCond.L.Lock() // relock after wakeup routine unlocks on its exit
			//
			// log a backlog wakeup event.
			//
			var etype string
			if GetConfig().EnableSharding {
				etype = fmt.Sprintf("%s%d_shd%d", bcklgEvtPrefix[pool.Type], pool.InstID, pool.ShardID)
			} else {
				etype = fmt.Sprintf("%s%d", bcklgEvtPrefix[pool.Type], pool.InstID)
			}
			if longTo {
				etype += "_long"
			} else {
				etype += "_short"
			}
			ename := fmt.Sprintf("%d", (sleepingtime / GetConfig().BacklogTimeoutUnit))
			e := cal.NewCalEvent(etype, ename, cal.TransOK, strconv.Itoa(int(sleepingtime)))
			e.Completed()
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "exiting backlog. type:", pool.Type, ", instance:", pool.InstID)
			}

			workerclient = pool.getActiveWorker()
			//
			// we still have the lock. if there are other connections also woke up but lost the
			// race to acquire the lock, backlog stats still have them counted.
			// if backlog count is 1
			//    if workerclient != nil, we are the only one racing and will be the last one
			//    exiting backlog
			//    if workerclient == nil, we did not win the race and are going back to backlog
			//
			if workerclient != nil {
				pool.resetIfLastBacklogEntry("acquire")
			}
			//
			// reduce even if workerclient is null since we add to blgcnt going back to the top.
			//
			pool.decBacklogCnt()
			//
			// a connection was waken up from backlog, reset backlog and idle count.
			// it is possible some other thread races ahead and get the worker
			// just being returned. if that happens, we get a nil worker and return
			// back into backlog at the top of the for loop.
			//
			GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: pool.ShardID, wType: pool.Type, instID: pool.InstID, oldCState: Backlog, newCState: Idle})
		}
	}

	ticket := fmt.Sprintf("%d", rand.Uint64())
	//
	// error causes coordinator to disconnect external client
	//
	if pool.aqmanager.alreadyDispatched(ticket, workerclient) {
		msg := fmt.Sprintf("pid=%d;pooltype=%d", workerclient.pid, pool.Type)
		e := cal.NewCalEvent(cal.EventTypeWarning, "double_dispatch", cal.TransOK, msg)
		e.Completed()
		pool.poolCond.L.Unlock()
		return nil, "", errors.New("double_dispatch")
	}
	pool.checkoutTickets[workerclient] = ticket
	pool.aqmanager.registerDispatchedWorker(ticket, workerclient)

	pool.poolCond.L.Unlock()

	GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: pool.ShardID, wType: pool.Type, instID: pool.InstID, oldCState: Idle, newCState: Assign})

	//another best effort, in case ReturnWorker() lost the race
	wchLen := len(workerclient.channel())
	// drain the channel if data late
	if wchLen > 0 {
		workerclient.DrainResponseChannel(0 /*no wait to minimize the latency*/)
	}

	return workerclient, ticket, nil
}

// ReturnWorker puts the worker into the list of available workers. It is called usually after a coordinator
// used it for requests and no longer needs it.
// If the pool is about to be downsize, the worker is instead terminated instead of being put in the available list.
// It the worker lifetime expired, the worker is instead terminated instead of being put in the available list.
func (pool *WorkerPool) ReturnWorker(worker *WorkerClient, ticket string) (err error) {
	now := time.Now().Unix()
	//
	// has to lock before checking QUCE. otherwise, we check and pass QUCE, someone else lock,
	// we block, someone else set QUCE to prevent worker return, someone else unlock, we lock,
	// we already passed QUCE and return worker by mistake.
	//
	pool.poolCond.L.Lock()

	if (worker == nil) || (worker.Status == wsQuce) {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "trying to return an invalid worker (bailing), size=", pool.activeQ.Len(), "type=", pool.Type, ", instance:", pool.InstID)
		}
		pool.poolCond.L.Unlock()
		return nil
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Pool::ReturnWorker(start)", worker.pid, worker.Type, worker.instID, "healthy:", pool.GetHealthyWorkersCount())
	}

	if (len(ticket) == 0) || (pool.checkoutTickets[worker] != ticket) {
		msg := fmt.Sprintf("pid=%d;pooltype=%d", worker.pid, pool.Type)
		e := cal.NewCalEvent(cal.EventTypeWarning, "rtrn_worker_using_wrong_ticket", cal.TransOK, msg)
		e.Completed()

		pool.poolCond.L.Unlock()
		return errors.New("returning a worker using wrong ticket")
	}
	delete(pool.checkoutTickets, worker)
	pool.aqmanager.unregisterDispatchedWorker(worker)

	if (worker.channel() != nil) && (len(worker.channel()) > 0) {
		e := cal.NewCalEvent(cal.EventTypeWarning, "rtrn_worker_having_unprocessed_msg", cal.TransOK, strconv.Itoa(len(worker.channel())))
		e.Completed()
		worker.DrainResponseChannel(time.Microsecond * 10)
	}

	worker.setState(wsAcpt)
	if (pool.desiredSize < pool.currentSize) && (worker.ID >= pool.desiredSize) {
		go func(w *WorkerClient) {
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "Pool resized, terminate worker: pid =", worker.pid, ", pool_type =", worker.Type, ", inst =", worker.instID)
			}
			w.Terminate()
		}(worker)
		//pool.currentSize--	// restartworker actually does the size reduction.
		pool.poolCond.L.Unlock()
		return nil
	}

	skipRecycle := false
	// check for the lifespan
	if (worker.exitTime != 0) && (worker.exitTime <= now) {
		if pool.GetHealthyWorkersCount() == int32(pool.desiredSize) {
			//
			// reset exit time to prevent checkWorkerLifespan from terminating this worker again.
			//
			worker.exitTime = 0
			go func(w *WorkerClient) {
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, "Lifespan exceeded, terminate worker: pid =", worker.pid, ", pool_type =", worker.Type, ", inst =", worker.instID)
				}
				w.Terminate()
			}(worker)
			pool.poolCond.L.Unlock()
			return nil
		} else {
			skipRecycle = true
		}
	}

	//
	// check for max requests which can change at runtime.
	//
	maxReqs := GetMaxRequestsPerChild()
	if (maxReqs > (worker.maxReqCount + worker.maxReqCount/4)) ||
		(maxReqs < (worker.maxReqCount - worker.maxReqCount/4)) {
		if maxReqs >= 4 {
			worker.maxReqCount = maxReqs - uint32(rand.Intn(int(maxReqs/4)))
		}
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "Max requests change pickedup pid =", worker.pid, "cnt", worker.reqCount, "max", worker.maxReqCount)
		}
	}
	if worker.maxReqCount != 0 {
		//worker.reqCount++	// count in dorequest for each statement instead of for each session.
		if worker.reqCount >= worker.maxReqCount {
			if pool.GetHealthyWorkersCount() == int32(pool.desiredSize) {
				go func(w *WorkerClient) {
					if logger.GetLogger().V(logger.Info) {
						logger.GetLogger().Log(logger.Info, "Max requests exceeded, terminate worker: pid =", worker.pid, ", pool_type =", worker.Type, ", inst =", worker.instID, "cnt", worker.reqCount, "max", worker.maxReqCount)
					}
					w.Terminate()
				}(worker)
				pool.poolCond.L.Unlock()
				return nil
			} else {
				skipRecycle = true
			}
		}
	}
	if skipRecycle {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "Non Healthy Worker found in pool, module_name=",pool.moduleName,"shard_id=",pool.ShardID, "HEALTHY worker Count=",pool.GetHealthyWorkersCount(),"TotalWorkers:=", pool.desiredSize)
		}
		calMsg := fmt.Sprintf("Recycle(worker_pid)=%d, module_name=%s,shard_id=%d", worker.pid, worker.moduleName, worker.shardID)
		evt := cal.NewCalEvent("ERROR","RECYCLE_WORKER", cal.TransOK, calMsg)
		evt.Completed()
	}

	var pstatus = false
	if GetConfig().LifoScheduler {
		pstatus = pool.activeQ.PushFront(worker)
	} else {
		pstatus = pool.activeQ.Push(worker)
	}

	blgsize := atomic.LoadInt32(&(pool.backlogCnt))
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "poolsize (after return)", pool.activeQ.Len(), " type ", pool.Type, ", instance:", pool.InstID, ", pushstatus:", pstatus, ", bklg:", blgsize, worker.pid)
	}

	pool.poolCond.L.Unlock()

	//
	// notify one waiting agent on the availability of a new worker in the pool
	//
	pool.poolCond.Signal()
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Pool::ReturnWorker(end after signal)", pool.activeQ.Len(), " type ", pool.Type, "healthy:", pool.GetHealthyWorkersCount(), worker.pid)
	}

	return nil
}

/**
 * caller has lock
 */
func (pool *WorkerPool) getActiveWorker() (worker *WorkerClient) {
	var workerclient *WorkerClient
	var cnt = pool.activeQ.Len()
	for cnt > 0 {
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "poolsize (before get)", pool.activeQ.Len(), " type ", pool.Type, ", instance:", pool.InstID)
		}
		workerclient = pool.activeQ.Poll().(*WorkerClient)
		if workerclient.Status > wsInit {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "Pool::SelectWorker", workerclient.pid, workerclient.Type, pool.InstID)
			}
			return workerclient
		}
		cnt--
		pool.activeQ.Push(workerclient) // put it at the end of queue
	}
	return nil
}

// Resize resize the worker pool when the corresponding dynamic configuration changed.
// When the size is increased, the increase is immediate by spawning the necessary number of new workers.
// When the size is decreased, it removes the workers whose id is bigger then the number of workers. If
// the workers to be removed are free, they are terminated immediately, otherwise the termination is delayed
// until the worker eventually calls ReturnWorker to make itself available
func (pool *WorkerPool) Resize(newSize int) {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Resizing pool:", pool.Type, pool.currentSize, "->", newSize)
	}
	pool.poolCond.L.Lock()
	defer pool.poolCond.L.Unlock()
	if newSize == pool.desiredSize {
		return
	}
	pool.desiredSize = newSize
	if pool.desiredSize > pool.currentSize {
		// worker increase
		//
		workers := make([]*WorkerClient, pool.desiredSize)
		copy(workers, pool.workers)
		pool.workers = workers
		// let statlog resets the worker size
		//
		GetStateLog().PublishStateEvent(StateEvent{eType: WorkerResizeEvt, shardID: pool.ShardID, wType: pool.Type, instID: pool.InstID, newWSize: newSize})
		for i := pool.currentSize; i < newSize; i++ {
			worker := NewWorker(i, pool.Type, pool.InstID, pool.ShardID, pool.moduleName, pool.thr)
			er := worker.StartWorker()
			if er != nil {
				if logger.GetLogger().V(logger.Alert) {
					logger.GetLogger().Log(logger.Alert, "failed starting worker: ", er)
				}
				return
			}
			go worker.AttachToWorker()
		}
		pool.currentSize = pool.desiredSize
	} else {
		// remove the idle/free workers now. workers not free with ID > pool.desiredSize are terminated in ReturnWorker
		remove := func(item interface{}) bool {
			worker := item.(*WorkerClient)
			if worker.ID >= pool.desiredSize {
				// run in go routine so it doesn't block
				go func(w *WorkerClient) {
					if logger.GetLogger().V(logger.Info) {
						logger.GetLogger().Log(logger.Info, "Pool resized, terminate worker: pid =", worker.pid, ", pool_type =", worker.Type, ", inst =", worker.instID)
					}
					w.Terminate()
				}(worker)
				return true
			}
			return false
		}
		pool.activeQ.ForEachRemove(remove)
	}
}

// Healthy checks if the number of workers connected to the database is greater than 20%
func (pool *WorkerPool) Healthy() bool {
	pool.poolCond.L.Lock()
	size := pool.currentSize
	pool.poolCond.L.Unlock()
	numHealthyWorkers := atomic.LoadInt32(&(pool.numHealthyWorkers))
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Healthy check pool type =", pool.Type, ", id =", pool.InstID, ", healthy = ", numHealthyWorkers, ", size =", size)
	}
	return (numHealthyWorkers * 100) >= (int32(size) * 20)
}

// IncHealthyWorkers called to increment the number of workers conected to the database
func (pool *WorkerPool) IncHealthyWorkers() {
	atomic.AddInt32(&(pool.numHealthyWorkers), 1)
}

// DecHealthyWorkers called to decrement the number of workers conected to the database
func (pool *WorkerPool) DecHealthyWorkers() {
	atomic.AddInt32(&(pool.numHealthyWorkers), -1)
}

// GetHealthyWorkersCount returns the number of workers conected to the database
func (pool *WorkerPool) GetHealthyWorkersCount() int32 {
	return atomic.LoadInt32(&(pool.numHealthyWorkers))
}

// RacMaint is called when rac maintenance is needed. It marks the workers for restart, spreading
// to an interval in order to avoid connection storm to the database
func (pool *WorkerPool) RacMaint(racReq racAct) {
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "Rac maint processing, shard =", pool.ShardID, ", inst =", racReq.instID, ", time=", racReq.tm)
	}
	now := time.Now().Unix()
	window := GetConfig().RacRestartWindow
	dbUname := ""
	cnt := 0
	pool.poolCond.L.Lock()
	for i := 0; i < pool.currentSize; i++ {
		if (pool.workers[i] != nil) && (racReq.instID == 0 || pool.workers[i].racID == racReq.instID) && (pool.workers[i].startTime < int64(racReq.tm)) {
			statusTime := now
			// requested time is in the past, restart starts from now
			// requested time is in the future, set restart time starting from it
			if now < int64(racReq.tm) {
				statusTime = int64(racReq.tm)
			}

			if racReq.delay {
				pool.workers[i].exitTime = statusTime + int64(window*i/pool.currentSize)
			} else {
				pool.workers[i].exitTime = statusTime
			}

			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "Rac maint activating, worker", i, pool.workers[i].pid, "exittime=", pool.workers[i].exitTime, now, window, pool.currentSize)
			}
			cnt++
			if len(dbUname) == 0 {
				dbUname = pool.workers[i].dbUname
			}
		}
	}
	pool.poolCond.L.Unlock()
	// TODO: C++ worker logs one event for each worker, in the worker, so
	// we keep the same. Think about changing it
	for i := 0; i < cnt; i++ {
		evt := cal.NewCalEvent("RAC_ID", fmt.Sprintf("%d", racReq.instID), cal.TransOK, "")
		evt.Completed()
		evt = cal.NewCalEvent("DB_UNAME", dbUname, cal.TransOK, "")
		evt.Completed()
	}
}

// checkWorkerLifespan is called periodically to check if any worker lifetime has expired and terminates it
func (pool *WorkerPool) checkWorkerLifespan() {
	var skipcnt uint32
	var cutofftime uint32
	for {
		if skipcnt < 90 {
			skipcnt = skipcnt + 1
		} else {
			skipcnt = 0
			//
			// bigger one
			//
			idleto := uint32(GetTrIdleTimeoutMs())
			dummy := uint32(GetIdleTimeoutMs())
			if dummy > idleto {
				idleto = dummy
			}
			//
			// terminate worker if it stays dispatched more than 3 times the idle timeout ago.
			//
			idleto = 3 * idleto
			//
			// worker.sqlStartTimeMs is measured since the start of mux.
			//
			muxnow := uint32((time.Now().UnixNano() - GetStateLog().GetStartTime()) / int64(time.Millisecond))
			cutofftime = muxnow - idleto
		}

		var workers []*WorkerClient
		now := time.Now().Unix()
		pool.poolCond.L.Lock()
		for i := 0; i < pool.currentSize; i++ {
			if (pool.workers[i] != nil) && (pool.workers[i].exitTime != 0) && (pool.workers[i].exitTime <= now) {
				if pool.GetHealthyWorkersCount() < (int32(pool.desiredSize*GetConfig().MaxDesiredHealthyWorkerPct/100)) { // Should it be a config value
					if logger.GetLogger().V(logger.Alert) {
						logger.GetLogger().Log(logger.Alert, "Non Healthy Worker found in pool, module_name=",pool.moduleName,"shard_id=",pool.ShardID, "HEALTHY worker Count=",pool.GetHealthyWorkersCount(),"TotalWorkers:", pool.desiredSize)
					}
					calMsg := fmt.Sprintf("checkworkerlifespan()  module_name=%s,shard_id=%d", pool.moduleName, pool.ShardID)
					evt := cal.NewCalEvent("ERROR","RECYCLE_WORKER", cal.TransOK, calMsg)
					evt.Completed()
					break
				}
				if pool.activeQ.Remove(pool.workers[i]) {
					workers = append(workers, pool.workers[i])
					//
					// reset exit time to prevent return worker from terminating this worker again.
					//
					pool.workers[i].exitTime = 0
					if len(workers) > pool.desiredSize*10/100 { // Should it be a config value
						break //Always recycle 10% of workers at a time
					}
				} else {
					if GetConfig().EnableDanglingWorkerRecovery {
						//
						// if disabled, ignore (the worker is in use, it will be checked when freed)
						// otherwise check every 15 min to see if worker.sqlstarttimems is greater
						// than 3 x idletimeout. catch dangling worker not returned by coordinator
						//
						if skipcnt == 0 {
							stime := atomic.LoadUint32(&(pool.workers[i].sqlStartTimeMs))
							//
							// could be worker is dispatched but coordinator has not set stime yet.
							//
							if stime != 0 {
								if stime < cutofftime {
									workers = append(workers, pool.workers[i])
									pool.workers[i].exitTime = 0
									evt := cal.NewCalEvent(cal.EventTypeWarning, "terminate_dispatched_worker", cal.TransOK, fmt.Sprintf("%d", pool.workers[i].pid))
									evt.Completed()
								}
							}
						}
					}
				}
			}
		}
		pool.poolCond.L.Unlock()
		for _, w := range workers {
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "checkworkerlifespan - Lifespan exceeded, terminate worker: pid =", w.pid, ", pool_type =", w.Type, ", inst =", w.instID ,"HEALTHY worker Count=",pool.GetHealthyWorkersCount(),"TotalWorkers:", pool.desiredSize)
			}
			w.Terminate()
		}
		time.Sleep(time.Duration(pool.lifeSpanCheckInterval) * time.Second)
	}
}

/**
 * check to see if backlog will become empty
 * @param loc who is calling us.
 */
func (pool *WorkerPool) resetIfLastBacklogEntry(loc string) {
	blgsize := atomic.LoadInt32(&(pool.backlogCnt))
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "resetIfLastBacklogEntry blgsize", blgsize, loc)
	}
	if blgsize == 1 {
		now := time.Now().UnixNano() / int64(time.Millisecond)
		duration := now - pool.aqmanager.lastEmptyTimeMs

		var ename string
		if GetConfig().EnableSharding {
			ename = fmt.Sprintf("aq%s%d_shd%d", bcklgTimeoutEvtPrefix[pool.Type], pool.InstID, pool.ShardID)
		} else {
			ename = fmt.Sprintf("aq%s%d", bcklgTimeoutEvtPrefix[pool.Type], pool.InstID)
		}
		evt := cal.NewCalEvent("QUEUE", ename, cal.TransOK, fmt.Sprintf("%d", duration))
		evt.AddDataStr("stime", fmt.Sprintf("%d&etime=%d %s", pool.aqmanager.lastEmptyTimeMs, now, loc))
		evt.Completed()

		pool.aqmanager.lastEmptyTimeMs = now
		pool.aqmanager.clearAllEvictedSqlhash()
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "setlastempty(exit)", loc, pool.aqmanager.lastEmptyTimeMs)
		}
	}
}

func (pool *WorkerPool) decBacklogCnt() {
	if atomic.LoadInt32(&(pool.backlogCnt)) > 0 {
		atomic.AddInt32(&(pool.backlogCnt), -1)
	} else {
		logger.GetLogger().Log(logger.Warning, "invalid backlogCnt (acquire)")
		e := cal.NewCalEvent(cal.EventTypeWarning, "negative bcklgCnt", cal.TransOK, "")
		e.Completed()
		atomic.StoreInt32(&(pool.backlogCnt), 0)
	}
}
