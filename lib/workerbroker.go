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
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/paypal/hera/utility/logger"
)

// HeraWorkerType defines the possible worker type
type HeraWorkerType int

// constants for HeraWorkerType
const (
	wtypeRW HeraWorkerType = iota
	wtypeRO                // @TODO
	wtypeStdBy
	wtypeTotalCount
)

// WorkerPoolCfg is a configuration structure to keep setups for each type of worker pool
type WorkerPoolCfg struct {
	maxWorkerCnt int // each instance of a worker type has the same max worker count.
	instCnt      int // number of instances (e.g. standbys) in a type of worker pool.
}

// WorkerBroker is managing the workers, starting the worker pools, and restarting workers when needed
type WorkerBroker struct {
	//
	// array of maps with each map representing one shard of deployment.
	// inside each map,
	//    key = HeraWorkerType,
	//    value = array of WorkerPools, with each pool corresponds to one instance
	//            of that HeraWorkerType. wtypeStdBy has multiples instances known
	//            as multiple standbys. RW will have two instances for primary/failover.
	// workerpools [shard](map[HeraWorkerType][inst]*WorkerPool)
	//
	workerpools []map[HeraWorkerType][]*WorkerPool
	poolCfgs    []map[HeraWorkerType]*WorkerPoolCfg
	//
	// a pid->workerclient map to maintain active workers. when an worker exits either
	// through recycle or unexpectedly, we receive a sigchld event and will trace down
	// and restart the stopped workers.
	//
	pidworkermap map[int32]*WorkerClient
	lock         sync.Mutex

	//
	// loaded from cfg once and used later.
	//
	maxShardSize int

	// used to signal when the signal handler method finishes
	stopped chan struct{}
}

var sBrokerInstance *WorkerBroker
var once sync.Once

// GetWorkerBrokerInstance returns the singleton broker instance where different request handler goroutines can use to get a free worker
func GetWorkerBrokerInstance() *WorkerBroker {
	//
	// no retry. if intialization fails, main() should bail out.
	//
	once.Do(func() {
		sBrokerInstance = &WorkerBroker{}
		err := sBrokerInstance.init()
		if err != nil {
			sBrokerInstance = nil
		}
	})
	return sBrokerInstance
}

/**
 * private method to set up different worker pools
 *
 * @TODO pull types and sizes from config
 */
func (broker *WorkerBroker) init() error {
	broker.stopped = make(chan struct{})
	broker.maxShardSize = GetConfig().NumOfShards
	if (broker.maxShardSize == 0) || !(GetConfig().EnableSharding) {
		broker.maxShardSize = 1
	}
	//
	// MAX_NUM_STANDBY = 10
	//
	maxStndbySize := GetConfig().NumStdbyDbs
	if maxStndbySize > 10 {
		maxStndbySize = 10
	}
	MaxWorkerSize := <-GetConfig().NumWorkersCh()
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "num_standby_dbs", maxStndbySize, "max_worker", MaxWorkerSize)
	}

	//
	// initialize workerpools and poolCfgs for all shards.
	//
	broker.workerpools = make([](map[HeraWorkerType][]*WorkerPool), broker.maxShardSize)
	broker.poolCfgs = make([](map[HeraWorkerType]*WorkerPoolCfg), broker.maxShardSize)
	var workercnt int
	for s := 0; s < broker.maxShardSize; s++ {
		//
		// setup broker configuration, inst and worker size can be loaded from cdb
		//
		broker.poolCfgs[s] = make(map[HeraWorkerType]*WorkerPoolCfg, wtypeTotalCount)
		//
		broker.poolCfgs[s][wtypeRO] = new(WorkerPoolCfg)
		broker.poolCfgs[s][wtypeRO].maxWorkerCnt = GetNumRWorkers(s)
		if broker.poolCfgs[s][wtypeRO].maxWorkerCnt > 0 {
			broker.poolCfgs[s][wtypeRO].instCnt = 1
		}

		broker.poolCfgs[s][wtypeRW] = new(WorkerPoolCfg)
		broker.poolCfgs[s][wtypeRW].maxWorkerCnt = GetNumWWorkers(s)
		broker.poolCfgs[s][wtypeRW].instCnt = 1

		broker.poolCfgs[s][wtypeStdBy] = new(WorkerPoolCfg)
		if GetConfig().EnableTAF {
			broker.poolCfgs[s][wtypeStdBy].maxWorkerCnt = GetNumStdByWorkers(s)
			broker.poolCfgs[s][wtypeStdBy].instCnt = 1
		} else {
			broker.poolCfgs[s][wtypeStdBy].maxWorkerCnt = 0
			broker.poolCfgs[s][wtypeStdBy].instCnt = 0
		}

		//
		// populate worker pools with attached workerclients base on poolcfg template
		//
		broker.workerpools[s] = make(map[HeraWorkerType][]*WorkerPool, wtypeTotalCount)
		for t := 0; t < int(wtypeTotalCount); t++ {
			poolcfg := broker.poolCfgs[s][HeraWorkerType(t)]
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "init pool ", poolcfg)
			}
			workercnt += (poolcfg.instCnt * poolcfg.maxWorkerCnt)
			broker.workerpools[s][HeraWorkerType(t)] = make([]*WorkerPool, poolcfg.instCnt)
			for i := 0; i < poolcfg.instCnt; i++ {
				broker.workerpools[s][HeraWorkerType(t)][i] = &WorkerPool{}
			}
		}
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "workercnt ", workercnt)
	}
	//
	// start worker monitor thread that will reap and restart any defuncted workers
	//
	broker.pidworkermap = make(map[int32]*WorkerClient, workercnt)
	return nil
}

// RestartWorkerPool (re)starts all the worker pools
// workerpool.init calls statelog.init, which in turn calls back GetWorkerBrokerInstance
// this causes a deadlock during workerbroker initialization since golang lock is not
// reentrant. taking out workerpool.init from broker.init and calling it separately.
func (broker *WorkerBroker) RestartWorkerPool(_moduleName string) error {
	var err error
	for s := 0; s < broker.maxShardSize; s++ {
		for t := 0; t < int(wtypeTotalCount); t++ {
			poolcfg := broker.poolCfgs[s][HeraWorkerType(t)]
			for i := 0; i < poolcfg.instCnt; i++ {
				err = broker.workerpools[s][HeraWorkerType(t)][i].Init(HeraWorkerType(t), poolcfg.maxWorkerCnt, i, s, _moduleName)
				if err != nil {
					if logger.GetLogger().V(logger.Alert) {
						logger.GetLogger().Log(logger.Alert, "failed to start workerpool", err)
					}
					return err
				}
			}
		}
	}
	err = broker.startWorkerMonitor()
	return err
}

// GetWorkerPoolCfgs returns the worker pool configuration
func (broker *WorkerBroker) GetWorkerPoolCfgs() (pCfgs []map[HeraWorkerType]*WorkerPoolCfg) {
	return broker.poolCfgs
}

// GetWorkerPool get the worker pool object for the type and id
// ids holds optional paramenters.
//
//	ids[0] == instance id; ids[1] == shard id.
//
// if a particular id is not set, it defaults to 0.
// TODO: interchange sid <--> instId since instId is not yet used
func (broker *WorkerBroker) GetWorkerPool(wType HeraWorkerType, ids ...int) (workerbroker *WorkerPool, err error) {
	//
	// default sid and instId to 0 if user doesnot bother to send one
	//
	var instID int
	var sid int
	var size = len(ids)
	if size == 1 {
		instID = ids[0]
	} else if size > 1 {
		instID = ids[0]
		sid = ids[1]
	}

	if broker.workerpools != nil {
		if broker.workerpools[sid] != nil && len(broker.workerpools[sid]) > 0 {
			if broker.workerpools[sid][wType] != nil && len(broker.workerpools[sid][wType]) > 0 {
				if broker.workerpools[sid][wType][instID] != nil {
					return broker.workerpools[sid][wType][instID], nil
				}
			}
		}
	}
	return nil, errors.New("uninitialized worker pool")
}

// AddPidToWorkermap add the worker to the map pid -> worker
func (broker *WorkerBroker) AddPidToWorkermap(worker *WorkerClient, pid int) {
	broker.lock.Lock()
	defer broker.lock.Unlock()
	broker.pidworkermap[int32(pid)] = worker
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Added", pid, ", pwmap:", broker.pidworkermap)
	}
}

func (broker *WorkerBroker) startWorkerMonitor() (err error) {
	//
	// set up sig channel for worker exiting event
	//
	schannel := make(chan os.Signal, 1)
	signal.Notify(schannel, syscall.SIGCHLD, syscall.SIGTERM)

	go func(sigchannel chan os.Signal) {
		//
		// forever loop to react on worker exit event or opscfg worker change
		//
		cfgWorkerChange := GetConfig().NumWorkersCh()
		for {
			select {
			case <-cfgWorkerChange:
				broker.changeMaxWorkers()
			//
			// Block until a signal is received.
			//
			case signal := <-sigchannel:
				switch signal {
				case syscall.SIGCHLD:
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, "worker exit signal:", signal)
					}
					//
					// no one have called waitpid on recycled or self-retired workers.
					// we can get all the pids in this call. double the size in case we
					// get none-hera defunct processes. +1 in case racing casue mapsize=0.
					//
					defunctPids := make([]int32, 0)
					for {
						var status syscall.WaitStatus

						//Reap exited children in non-blocking mode
						pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
						if pid > 0 {
							if logger.GetLogger().V(logger.Verbose) {
								logger.GetLogger().Log(logger.Verbose, "received worker exit signal for pid:", pid, " status: ", status)
							}
							defunctPids = append(defunctPids, int32(pid))
						} else if pid == 0 {
							break
						} else {
							if errors.Is(err, syscall.ECHILD) {
								break
							} else {
								logger.GetLogger().Log(logger.Verbose, "error in wait signal: ", err)
							}
						}
					}

					if len(defunctPids) > 0 {
						if logger.GetLogger().V(logger.Debug) {
							logger.GetLogger().Log(logger.Debug, "worker exit signal received from pids :", defunctPids)
						}
						broker.lock.Lock()
						for _, pid := range defunctPids {
							var workerclient = broker.pidworkermap[pid]
							if workerclient != nil {
								delete(broker.pidworkermap, pid)
								pool, err := GetWorkerBrokerInstance().GetWorkerPool(workerclient.Type, workerclient.instID, workerclient.shardID)
								if err != nil {
									if logger.GetLogger().V(logger.Alert) {
										logger.GetLogger().Log(logger.Alert, "Can't get pool for", workerclient, ":", err)
									}
								} else {
									//
									// a worker could be terminated while serving a request.
									// in these cases, doRead() in workerclient will get an
									// EOF and exit. doSession() in coordinator will get the
									// worker outCh closed event and exit, at which point
									// coordinator itself calls returnworker to set connstate
									// from assign to idle.
									// no need to publish the following event again.
									//
									//if (workerclient.Status == WAIT) || (workerclient.Status == BUSY) {
									//	GetStateLog().PublishStateEvent(StateEvent{eType:ConnStateEvt, shardId:workerclient.shardId, wType:workerclient.Type, instId:workerclient.instId, oldCState:Assign, newCState:Idle})
									//}
									if logger.GetLogger().V(logger.Debug) {
										logger.GetLogger().Log(logger.Debug, "worker (id=", workerclient.ID, "pid=", workerclient.pid, ") received signal. transits from state ", workerclient.Status, " to terminated.")
									}
									workerclient.setState(wsUnset) // Set the state to UNSET to make sure worker does not stay in FNSH state so long
									pool.RestartWorker(workerclient)
								}
							} else {
								if logger.GetLogger().V(logger.Alert) {
									logger.GetLogger().Log(logger.Alert, "Exited worker pid =", pid, " not found")
								}
							}
						}
						broker.lock.Unlock()
					}
				case syscall.SIGTERM:
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, "Got SIGTERM")
					}
					var wg sync.WaitGroup
					wg.Add(len(broker.pidworkermap))
					for pid, worker := range broker.pidworkermap {
						go func(w *WorkerClient) {
							w.Terminate()
						}(worker)
						go func(p int) {
							proc, err := os.FindProcess(p)
							if err == nil {
								proc.Wait()
								if logger.GetLogger().V(logger.Debug) {
									logger.GetLogger().Log(logger.Debug, "pid =", p, " worker process reaped")
								}
							}
							wg.Done()
						}(int(pid))
					}
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, "Waiting for workers to exit")
					}
					wg.Wait()
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, "Mux done")
					}
					// signal the main loop to exit
					close(broker.stopped)
					return
				} // switch signal
			} // select
		} // for
	}(schannel)
	return nil
}

/*
resizePool calls workerpool.Resize to resize a worker pool when the dynamic configuration of
the number of workers changed
*/
func (broker *WorkerBroker) resizePool(wType HeraWorkerType, maxWorkers int, shardID int) {
	broker.poolCfgs[0][wType].maxWorkerCnt = maxWorkers
	pool, err := broker.GetWorkerPool(wType, 0, shardID)
	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "Can't pool of type", wType, ", shard", shardID, ",error:", err)
		}
	} else {
		pool.Resize(maxWorkers)
	}
}

/*
changeMaxWorkers is called when the dynamic config changed, it calls resizePool() for all the pools
*/
func (broker *WorkerBroker) changeMaxWorkers() {
	wW := GetNumWWorkers(0)
	rW := GetNumRWorkers(0)
	sW := GetNumStdByWorkers(0)

	for i := 0; i < GetConfig().NumOfShards; i++ {
		broker.resizePool(wtypeRW, wW, i)
		if rW != 0 {
			broker.resizePool(wtypeRO, rW, i)
		}

		// if TAF enabled, handle stdby as well
		if GetConfig().EnableTAF {
			broker.resizePool(wtypeStdBy, sW, i)
		}

		if GetConfig().EnableWhitelistTest {
			// only resize shard 0
			break
		}
	}
}

// Stopped is called when we are done, it sends a message to the "stopped" channel, which is read by the main mux routine
func (broker *WorkerBroker) Stopped() <-chan struct{} {
	return broker.stopped
}
