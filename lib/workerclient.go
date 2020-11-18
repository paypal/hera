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
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

// HeraWorkerStatus defines the posible states the worker can be in
type HeraWorkerStatus int

// constants for HeraWorkerStatus
const (
	wsUnset        HeraWorkerStatus = -1
	wsInit                          = 0 // initial state before the worker connects to the db and become availabla
	wsAcpt                          = 1 // "accepting" - the worker is free to take requests
	wsWait                          = 2 // the worker is waiting for the next request but not doing work, usually holding a db transaction open
	wsBusy                          = 3 // the worker is busy executing a request
	wsSchd                          = 4 //
	wsFnsh                          = 5 // the worker just finished the requests, it will move to init state
	wsQuce                          = 6 // repurposed for a state when worker is restarting or recovering
	MaxWorkerState                  = 7
)

const bfChannelSize = 30

// workerMsg is used to communicate with the coordinator, it contains the control message metadata plus the actual payload
// which is the response to be sent to the client
type workerMsg struct {
	data []byte
	// if any EOR_... is received
	eor bool
	// if EOR FREE is received
	free bool
	// EOR IN_TRANSACTION or EOR IN_CURSOR_IN_TRANSACTION is received
	inTransaction bool
	// tell coordinator to abort dosession with an ErrWorkerFail. call will recover worker.
	abort     bool
	bindEvict bool
	// the request counter / Id
	rqId uint32
	// the actual message to be sent to the client
	ns *netstring.Netstring
}

func (msg *workerMsg) GetNetstring() *netstring.Netstring {
	if msg.ns == nil {
		msg.ns, _ = NetstringFromBytes(msg.data)
	}
	return msg.ns
}

type BindPair struct {
	name  string
	value string
}

// WorkerClient represents a worker process
type WorkerClient struct {
	ID            int              // the worker identifier, from 0 to max worker count
	Type          HeraWorkerType   // the type of worker (ex write, read); all workers from the same type are grouped in a pool
	Status        HeraWorkerStatus // the worker state, like init, accept, etc
	workerConn    net.Conn         // the connection over which it communicates with the worker process
	workerOOBConn net.Conn         // the connection over which it sends out-of-band messages
	pid           int              // worker pid, needed to check terminated worker before recycling a new one
	instID        int              // currently 0 or 1
	shardID       int              //
	racID         int              // for RAC maintenance, the rac ID where the worker connected
	dbUname       string           // the database name where the worker connected

	//
	// sending data message from worker to coordinator (owner == doRead thread)
	// only a doRead thread can write to, close and replace this channel
	// buffered channel with capacity = bfChannelSize to ensure workerclient never
	// blocks on writing if there is no more than bfChannelSize write to inchannel
	// this channel is closed when doRead thread exits.
	//
	outCh chan *workerMsg
	//
	// sending control message from worker to coordinator (owner == workerclient)
	// reassigned in NewWorker and the old instance is gc later. restart and resize calls NewWorker
	// they both require locks. acquire lock before write to ctrlCh to avoid writing to closed chan.
	//
	ctrlCh chan *workerMsg
	//
	// module name written to in v$session.
	//
	moduleName string
	//
	// hashcode of the sql that is currently being executed.
	//
	sqlHash int32
	//
	// for bind eviction
	sqlBindNs atomic.Value // *netstring.Netstring
	//
	// time since hera_start in ms when the current prepare statement is sent to worker.
	// reset to 0 after eor meaning no sql running (same as start_time_offset_ms in c++).
	//
	sqlStartTimeMs uint32

	// time when the worker started
	startTime int64
	// time when this worker must exit because of lifetime exceeded, randomized value of "max_lifespan_per_child" ops config value.
	// it can be also set sooner when doing rac maintenance
	exitTime int64
	// the number of requests the worker handled
	reqCount uint32
	// the maximum number of requests the worker is allowed, randomized value of "max_requests_per_child" ops config value
	maxReqCount uint32
	// request counter / identifier used when the mux interrupts an executing worker request
	rqId uint32

	//
	// under recovery. 0: no; 1: yes. use atomic.CompareAndSwapInt32 to check state.
	//
	isUnderRecovery int32

	// Throtle workers lifecycle
	thr Throttler
}

type strandedCalInfo struct {
	nameSuffix string
	raddr      string // remote address
	laddr      string // remote address
	//TODO: Add prefix later; for now we only recover because of tmo, so no need of prefix
}

var dbUserName string
var dbPassword string
var dbPassword2 string
var dbPassword3 string

/**
 * Update or insert into the environment
 */
func envUpsert(attr *syscall.ProcAttr, key string, val string) {
	// Future: envUpsertPrependUnique for PATH
	keyEq := key + "="
	keyEqVal := fmt.Sprintf("%s=%s", key, val)
	for idx, val := range attr.Env {
		if strings.HasPrefix(val, keyEq) {
			attr.Env[idx] = keyEqVal
			return
		}
	}
	attr.Env = append(attr.Env, keyEqVal)
}

// NewWorker creates a new workerclient instance (pointer)
func NewWorker(wid int, wType HeraWorkerType, instID int, shardID int, moduleName string, thr Throttler) *WorkerClient {
	worker := &WorkerClient{ID: wid, Type: wType, Status: wsUnset, instID: instID, shardID: shardID, moduleName: moduleName, thr: thr}
	maxReqs := GetMaxRequestsPerChild()
	if maxReqs >= 4 {
		worker.maxReqCount = maxReqs - uint32(rand.Intn(int(maxReqs/4)))
	}
	worker.startTime = time.Now().Unix()
	lifespan := GetMaxLifespanPerChild()
	if lifespan >= 4 {
		worker.exitTime = worker.startTime + int64(lifespan) - int64(rand.Intn(int(lifespan/4)))
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, fmt.Sprintf("workerId=%d max_requests_per_child=%d max_lifespan_per_child=%d exitTime=%d", worker.ID, worker.maxReqCount, worker.exitTime-worker.startTime, worker.exitTime))
	}
	// TODO
	worker.racID = -1
	worker.isUnderRecovery = 0
	if worker.ctrlCh != nil {
		close(worker.ctrlCh)
	}
	//
	// adaptiveqmgr use the same lock in workerpool to protect from writing to a close/nil channel
	// it is possible coordinator (the only listener of ctrlch) could exit before consume the ctrl
	// msg. if adaptiveqmgr blocks on a non-buffered channel, there is a deadlock when return worker
	//
	worker.ctrlCh = make(chan *workerMsg, 5)
	return worker
}

// StartWorker fork exec a new worker.
// Note: the routine is not "reentrant", it changes the global environment. For now is fine.
func (worker *WorkerClient) StartWorker() (err error) {
	attr := syscall.ProcAttr{Dir: "./", Env: os.Environ(), Files: nil, Sys: nil}
	var dbHostName string
	logDbHostName := os.Getenv("LOG_DB_HOST_NAME")
	if len(logDbHostName) > 0 {
		dbHostName = logDbHostName
	} else {
		if len(worker.moduleName) > 4 {
			dbHostName = strings.ToUpper(worker.moduleName[4:])
			pos := strings.Index(dbHostName, "-")
			if pos != -1 {
				dbHostName = dbHostName[:pos]
			}
		} else {
			return errors.New("Invalid module name, must be like hera-<name> ")
		}
	}

	var twoTask string
	switch worker.Type {
	case wtypeStdBy:
		if GetConfig().EnableSharding {
			envUpsert(&attr, envCalClientSession, fmt.Sprintf("CLIENT_SESSION_TAF_%d", worker.shardID))
			if GetConfig().EnableTAF {
				envUpsert(&attr, envDbHostName, fmt.Sprintf("%s_TAF_%d", dbHostName, worker.shardID))
			} else {
				envUpsert(&attr, envDbHostName, fmt.Sprintf("%s_R_%d", dbHostName, worker.shardID))
			}
			envUpsert(&attr, envLogPrefix, fmt.Sprintf("S0-WORKER shd%d %d", worker.shardID, worker.ID))
		} else {
			envUpsert(&attr, envCalClientSession, "CLIENT_SESSION_TAF")
			if GetConfig().EnableTAF {
				envUpsert(&attr, envDbHostName, fmt.Sprintf("%s_TAF", dbHostName))
			} else {
				envUpsert(&attr, envDbHostName, fmt.Sprintf("%s_R", dbHostName))
			}
			envUpsert(&attr, envLogPrefix, fmt.Sprintf("S0-WORKER %d", worker.ID))
		}
		envUpsert(&attr, envHeraName, fmt.Sprintf("%s_taf", worker.moduleName))

		twoTaskEnv := fmt.Sprintf("TWO_TASK_STANDBY0_%d", worker.shardID)
		twoTask = os.Getenv(twoTaskEnv)
		if twoTask == "" {
			if worker.shardID != 0 {
				logger.GetLogger().Log(logger.Alert, twoTaskEnv, "is not defined")
				et := cal.NewCalEvent(cal.EventTypeError, twoTaskEnv, cal.TransOK, "")
				et.Completed()
				return errors.New(twoTaskEnv + " is not defined")
			}
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, twoTaskEnv, "is not defined, fallback")
			}
			twoTaskEnv = "TWO_TASK_STANDBY0"
			twoTask = os.Getenv(twoTaskEnv)
		}
		if twoTask != "" {
			for idx, val := range attr.Env {
				if (len(val) >= 9) && (val[:9] == "TWO_TASK=") {
					attr.Env[idx] = fmt.Sprintf("TWO_TASK=%s", twoTask)
				}
			}
		} else {
			return errors.New(twoTaskEnv + " is not defined")
		}

	case wtypeRO:
		if GetConfig().EnableSharding {
			envUpsert(&attr, envCalClientSession, fmt.Sprintf("CLIENT_SESSION_R_%d", worker.shardID))
			envUpsert(&attr, envDbHostName, fmt.Sprintf("%s_R_%d", dbHostName, worker.shardID))
			envUpsert(&attr, envLogPrefix, fmt.Sprintf("R-WORKER shd%d %d", worker.shardID, worker.ID))
		} else {
			envUpsert(&attr, envCalClientSession, "CLIENT_SESSION_R")
			envUpsert(&attr, envDbHostName, fmt.Sprintf("%s_R", dbHostName))
			envUpsert(&attr, envLogPrefix, fmt.Sprintf("R-WORKER %d", worker.ID))
		}
		envUpsert(&attr, envHeraName, worker.moduleName)

		twoTaskEnv := fmt.Sprintf("TWO_TASK_READ_%d", worker.shardID)
		twoTask = os.Getenv(twoTaskEnv)
		if twoTask == "" {
			if worker.shardID != 0 {
				logger.GetLogger().Log(logger.Alert, twoTaskEnv, "is not defined")
				et := cal.NewCalEvent(cal.EventTypeError, twoTaskEnv, cal.TransOK, "")
				et.Completed()
				return errors.New(twoTaskEnv + " is not defined")
			}
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, twoTaskEnv, "is not defined, fallback")
			}
			twoTaskEnv = "TWO_TASK_READ"
			twoTask = os.Getenv(twoTaskEnv)
		}
		if twoTask != "" {
			envUpsert(&attr, envTwoTask, twoTask)
		} else {
			if os.Getenv(envTwoTask) == "" {
				logger.GetLogger().Log(logger.Alert, "TWO_TASK is not defined for READ worker")
				et := cal.NewCalEvent(cal.EventTypeError, twoTaskEnv, cal.TransOK, "")
				et.Completed()
				return errors.New("TWO_TASK is not defined for READ worker")
			}
			// else it falls back to TWO_TASK.
		}

	default /*RW*/ :
		if GetConfig().EnableSharding {
			envUpsert(&attr, envCalClientSession, fmt.Sprintf("CLIENT_SESSION_%d", worker.shardID))
			envUpsert(&attr, envDbHostName, fmt.Sprintf("%s_%d", dbHostName, worker.shardID))
			envUpsert(&attr, envLogPrefix, fmt.Sprintf("WORKER shd%d %d", worker.shardID, worker.ID))
		} else {
			envUpsert(&attr, envCalClientSession, "CLIENT_SESSION")
			envUpsert(&attr, envDbHostName, dbHostName)
			envUpsert(&attr, envLogPrefix, fmt.Sprintf("WORKER %d", worker.ID))
		}
		envUpsert(&attr, envHeraName, worker.moduleName)

		twoTaskEnv := fmt.Sprintf("TWO_TASK_%d", worker.shardID)
		twoTask = os.Getenv(twoTaskEnv)
		if twoTask == "" {
			if worker.shardID != 0 {
				logger.GetLogger().Log(logger.Alert, twoTaskEnv, "is not defined")
				et := cal.NewCalEvent(cal.EventTypeError, twoTaskEnv, cal.TransOK, "")
				et.Completed()
				return errors.New(twoTaskEnv + " is not defined")
			}
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, twoTaskEnv, "is not defined, fallback")
			}
			twoTaskEnv = envTwoTask
			twoTask = os.Getenv(twoTaskEnv)
		} else {
			envUpsert(&attr, envTwoTask, twoTask)
		}
		if twoTask == "" {
			logger.GetLogger().Log(logger.Alert, "TWO_TASK is not defined")
			et := cal.NewCalEvent(cal.EventTypeError, twoTaskEnv, cal.TransOK, "")
			et.Completed()
			return errors.New("TWO_TASK is not defined")
		}
	}

	_, ok := os.LookupEnv("username")
	if !ok {
		envUpsert(&attr, "username", dbUserName)
	}
	_, ok = os.LookupEnv("password")
	if !ok {
		envUpsert(&attr, "password", dbPassword)
	}
	envUpsert(&attr, "password2", dbPassword2)
	envUpsert(&attr, "password3", dbPassword3)
	envUpsert(&attr, "mysql_datasource", twoTask)

	socketPair, err := syscall.Socketpair(syscall.AF_LOCAL, syscall.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	socketPair2, err := syscall.Socketpair(syscall.AF_LOCAL, syscall.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	attr.Files = make([]uintptr, 5)
	attr.Files[0] = 0
	attr.Files[1] = 1
	attr.Files[2] = 2
	attr.Files[3] = uintptr(socketPair[1])
	attr.Files[4] = uintptr(socketPair2[1])

	// !use a net.Conn instead of os.File, although either would work since they implement Read() interface.
	// os.File uses syscalls.Read which at this time (go 1.10) locks the OS thread while net.Conn uses netpoll
	fileData := os.NewFile(uintptr(socketPair[0]), fmt.Sprintf("worker_%d", worker.ID))
	if fileData != nil {
		defer fileData.Close()
	}
	worker.workerConn, err = net.FileConn(fileData)
	if err != nil {
		return err
	}
	fileOOB := os.NewFile(uintptr(socketPair2[0]), fmt.Sprintf("workeroob_%d", worker.ID))
	if fileOOB != nil {
		defer fileOOB.Close()
	}
	worker.workerOOBConn, err = net.FileConn(fileOOB)
	if err != nil {
		return err
	}
	//ea := syscall.SetsockoptInt(socketPair[0], syscall.SOL_SOCKET, syscall.SO_SNDBUF, 10*1024*1024)
	//eb := syscall.SetsockoptInt(socketPair[0], syscall.SOL_SOCKET, syscall.SO_RCVBUF, 10*1024*1024)
	//ec := syscall.SetsockoptInt(socketPair[1], syscall.SOL_SOCKET, syscall.SO_SNDBUF, 10*1024*1024)
	//ed := syscall.SetsockoptInt(socketPair[1], syscall.SOL_SOCKET, syscall.SO_RCVBUF, 10*1024*1024)
	//if logger.GetLogger().V(logger.Info) {
	//	logger.GetLogger().Log(logger.Info, "socketpair", socketPair[1], socketPair[0], "err", ea, eb, ec, ed)
	//}
	var workerPath string
	currentdir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err == nil {
		workerPath = currentdir + "/" + GetConfig().ChildExecutable
	} else {
		workerPath = "./" + GetConfig().ChildExecutable
	}

	//
	// arguments
	//
	var argv []string
	argv = make([]string, 3, 3)
	argv[0] = workerPath
	argv[1] = "--config"
	argv[2] = "hera.txt"

	var buf bytes.Buffer
	buf.WriteString("new_worker_child_")
	buf.WriteString(strconv.Itoa(int(worker.Type)))
	if worker.shardID > 0 {
		buf.WriteString("_shard_")
		buf.WriteString(strconv.Itoa(worker.shardID))
	}
	evt := cal.NewCalEvent(EvtTypeMux, buf.String(), cal.TransOK, "")
	evt.Completed()

	// TODO: change to use "exec"
	pid, er := syscall.ForkExec(workerPath, argv, &attr)
	syscall.Close(socketPair[1])
	syscall.Close(socketPair2[1])
	if er != nil {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "start worker failure ", er.Error(), " worker_path ", workerPath, " id", worker.ID)
		}
		et := cal.NewCalEvent(cal.EventTypeWarning, "spawn_error", cal.TransOK, fmt.Sprintf("execl errored out with %s", er.Error()))
		et.Completed()
		time.Sleep(10 * time.Second)
		return er
	}
	GetWorkerBrokerInstance().AddPidToWorkermap(worker, pid)
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "Started ", workerPath, ", pid=", pid)
	}
	worker.pid = pid
	worker.setState(wsInit)
	return nil
}

// AttachToWorker is called immediately after a worker process was created, it is a wrapper over the function
// doing the initialization work - attachToWorker. In case attachToWorker fails it does the cleanup.
func (worker *WorkerClient) AttachToWorker() (err error) {
	err = worker.attachToWorker()
	if err != nil {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "Fail to attach to worker pid =", worker.pid, ", id =", worker.ID, ":", err)
		}
		if worker.workerConn != nil {
			worker.workerConn.Close()
			worker.workerConn = nil
		}
		if worker.workerOOBConn != nil {
			worker.workerOOBConn.Close()
			worker.workerOOBConn = nil
		}
	}
	return err
}

// attachToWorker wait for the control message from the worker which tell it is ready.
// After receiveing the ready message it performs the initializations.
func (worker *WorkerClient) attachToWorker() (err error) {
	defer func() {
		worker.Terminate()
		worker.Close()
	}()

	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Waiting for control message from worker (", worker.ID, ", ", worker.pid, ")")
	}
	// wait for control message
	ns, err := netstring.NewNetstring(worker.workerConn)
	if err != nil {
		return err
	}

	if ns.Cmd != common.CmdControlMsg {
		return fmt.Errorf("Expected control message (%d) instead got (%d)", common.CmdControlMsg, ns.Cmd)
	}
	ln := len(ns.Payload)
	if ln > 0 {
		worker.racID = 0
		// extract rac ID and db uname
		for i := 0; i < ln; i++ {
			ch := ns.Payload[i]
			if ch == ' ' {
				worker.dbUname = string(ns.Payload[i:])
				break
			} else {
				n := ch - '0'
				if (n >= 0) && (n <= 9) {
					worker.racID = worker.racID*10 + int(n)
				} else {
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "Failed to parse the control message:", ns.Payload)
					}
					break
				}
			}
		}
	}
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "Got control message from worker (", worker.ID, ",", worker.pid, ",", worker.racID, ",", worker.dbUname, ")")
	}

	worker.setState(wsAcpt)

	pool, err := GetWorkerBrokerInstance().GetWorkerPool(worker.Type, worker.instID, worker.shardID)
	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "Can't get pool for", worker, ":", err)
		}
	} else {
		pool.WorkerReady(worker)
	}
	pool.IncHealthyWorkers()
	worker.doRead()
	pool.DecHealthyWorkers()
	return nil
}

// Close close the connections to the worker
func (worker *WorkerClient) Close() {
	if worker.workerConn != nil {
		worker.workerConn.Close()
	}
	if worker.workerOOBConn != nil {
		worker.workerOOBConn.Close()
	}
}

/**
 * Sends the recover signal to the worker
 */
func (worker *WorkerClient) initiateRecover(param int) {
	buff := []byte{byte(param), byte((worker.rqId & 0xFF000000) >> 24), byte((worker.rqId & 0x00FF0000) >> 16),
		byte((worker.rqId & 0x0000FF00) >> 8), byte((worker.rqId & 0x000000FF))}
	ns := netstring.NewNetstringFrom(common.CmdInterruptMsg, buff)
	worker.workerOOBConn.Write(ns.Serialized)
}

/**
 * logs to CAL the STRANDED event
 */
func (worker *WorkerClient) callogStranded(evtName string, info *strandedCalInfo) {
	calname := evtName
	if info != nil {
		calname += info.nameSuffix
	}
	et := cal.NewCalEvent("STRANDED", calname, cal.TransOK, "")
	et.AddDataInt("chld_pid", int64(worker.pid))
	et.AddDataInt("worker_id", int64(worker.ID))
	et.AddDataStr("fwk", "golang")
	if info != nil {
		et.AddDataStr("raddr", info.raddr)
		et.AddDataStr("laddr", info.laddr)
	}
	et.Completed()
}

// Recover interrupts a worker busy executing a request, usually because a client went away.
// It sends a break to the worker and expect the worker to respond with EOR free. If worker is not
// free-ing in two seconds, the worker is stopped with SIGKILL
func (worker *WorkerClient) Recover(p *WorkerPool, ticket string, info *strandedCalInfo, param ...int) {
	if atomic.CompareAndSwapInt32(&worker.isUnderRecovery, 0, 1) {
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "begin recover worker: ", worker.pid)
		}
	} else {
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "worker already underrecovery: ", worker.pid)
		}
		//
		// defer will not be called.
		//
		return
	}
	defer func() {
		if atomic.CompareAndSwapInt32(&worker.isUnderRecovery, 1, 0) {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "done recover worker: ", worker.pid)
			}
		} else {
			//
			// not possible. log in case.
			//
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "exit recover worker (isUnderRecovery was 0 during a recovery): ", worker.pid)
			}
		}
	}()
	if worker.Status == wsAcpt {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "will not recover an idle worker", worker.pid)
		}
		return
	}
	worker.setState(wsQuce)
	killparam := common.StrandedClientClose
	if len(param) > 0 {
		killparam = param[0]
	}
	worker.callogStranded("RECOVERING", info) // TODO: should we have this?
	worker.initiateRecover(killparam)
	workerRecoverTimeout := time.After(time.Millisecond * time.Duration(GetConfig().StrandedWorkerTimeoutMs))
	for {
		select {
		case <-workerRecoverTimeout:
			worker.thr.CanRun()
			worker.setState(wsInit) // Set the worker state to INIT when we decide to Terminate the worker
			worker.Terminate()
			worker.callogStranded("RECYCLED", info)
			return
		case msg, ok := <-worker.channel():
			if !ok {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, "Recover: worker closed, exiting")
				}
				worker.callogStranded("EXITED", info)
				return
			}
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "Recover (<<<worker pid = ", worker.pid, "): ", msg.free, len(msg.data))
			}
			//
			// to avoid infinite loop ignore if worker asks for a restart again.
			//
			if msg.free {
				if msg.rqId != worker.rqId {
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, "worker pid <<<<", worker.pid, ">>>>req id of worker:", msg.rqId, " and  req id of mux:", worker.rqId, " does not match, Skip the EOR")
					}
					evname := "rrqId"
					if (msg.rqId > worker.rqId) && ((worker.rqId > 10000) || (msg.rqId < 10000) /*rqId can wrap around to 0, this test checks that it did not just wrap*/) {
						// this is not expected, so log with different name
						evname = "rrqId_Error"
					}
					e := cal.NewCalEvent("WARNING", evname, cal.TransOK, "")
					e.AddDataInt("mux", int64(worker.rqId))
					e.AddDataInt("wk", int64(msg.rqId))
					e.Completed()
					// don't return yet, we expect another EOR message, matching the rqId
				} else {
					if logger.GetLogger().V(logger.Info) {
						logger.GetLogger().Log(logger.Info, "stranded conn recovered", worker.Type, worker.pid)
					}
					worker.callogStranded("RECOVERED", info)

					worker.setState(wsFnsh)
					p.ReturnWorker(worker, ticket)
					//
					// donot set state to ACPT since worker could already be picked up by another
					// client into wsBusy, if that worker ends up recovering, it will not finish
					// recovery because of ACPT state. that worker will never get back to the pool
					//
					//worker.setState(ACPT)
					return
				}
			}
		}
	}
}

// Terminate sends SIGTERM to worker first (allow worker to gracefully shutdown)
// wait for 2000 ms before sending SIGKILL if necessary.
// Note: this function will block ~ 100 - 2000 ms
func (worker *WorkerClient) Terminate() error {
	defer func() {
		worker.DrainResponseChannel(time.Microsecond * 10)
		//worker.Close()
	}()
	pid := worker.pid
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "workerclient pid=", pid, " to be terminated, sending SIGTERM first for gracefull termination")
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		// right now on Unix erp is always nil
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "workerclient pid=", pid, ", find process error", err.Error())
		}
		syscall.Kill(pid, syscall.SIGKILL)
		return nil
	}
	err = process.Signal(syscall.SIGTERM)
	if err != nil {
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "workerclient pid=", pid, "is gone already: ", err.Error())
		}
		return nil
	}

	slept := 0
	for slept < 2000 {
		time.Sleep(time.Millisecond * 100)
		slept += 100
		err = process.Signal(syscall.Signal(0))
		if err != nil {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "workerclient pid=", pid, "is gone: ", err.Error())
			}
			break
		}
	}
	if slept >= 2000 {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "workerclient pid=", pid, " sending SIGKILL")
		}
		syscall.Kill(pid, syscall.SIGKILL)
	}

	return nil
}

// DrainResponseChannel removes any messages that might be in the channel. This is used when the worker is recovered.
func (worker *WorkerClient) DrainResponseChannel(sleep time.Duration) {
outer:
	for {
		select {
		case _, ok := <-worker.ctrlCh:
			if !ok {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, "draining: ctrl Channel closed", worker.pid)
				}
				break outer
			} else {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, "draining: ctrl Channel", worker.pid)
				}
			}
		default:
			break outer
		}
	}
	calMsg := ""
	for {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "Drain the response channel from worker (", worker.ID, ", ", worker.pid, ")")
		}
		select {
		//
		// reading a nil channel blocks, which jumps to default and returns.
		//
		case msg, ok := <-worker.channel():
			if !ok {
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, "draining: Channel closed for from worker (", worker.ID, ", ", worker.pid, ")")
				}
				return
			}
			if logger.GetLogger().V(logger.Verbose) {
				if msg != nil && msg.data != nil {
					logger.GetLogger().Log(logger.Verbose, "draining:", DebugString(msg.data))
				}
			}

			if msg.ns == nil {
				// calMsg += "<nil>;"
			} else {
				calMsg += fmt.Sprintf("cmd:%d,payloadLen:%d;", msg.ns.Cmd, len(msg.ns.Payload))
			}

			//
			// allow doread to reload outCh in case there are more from worker.
			//
			time.Sleep(sleep)
		default:
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "draining: outCh empty")
			}

			if len(calMsg) > 0 {
				e := cal.NewCalEvent(EvtTypeMux, "data_late", cal.TransOK, fmt.Sprintf("pid=%d&id=%d&pkts=", worker.pid, worker.ID)+calMsg)
				e.Completed()
			}
			return
		}
	}
}

/*
 * Sits in an infinite loop. Reads requests from worker.inchannel, forwards via
 * the UDS pipe to the worker, and sends back the responses to the worker.outchannel
 */
func (worker *WorkerClient) doRead() {
	worker.outCh = make(chan *workerMsg, bfChannelSize)
	defer close(worker.outCh)

	var payload []byte
	for {
		//
		// blocking call. if something goes wrong, recycle will close uds from worker
		// side to unblock this call.
		//
		ns, err := netstring.NewNetstring(worker.workerConn)
		if err != nil {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "workerclient pid=", worker.pid, " read error:", err.Error())
			}
			if len(payload) > 0 {
				worker.outCh <- &workerMsg{data: payload, eor: false, free: false, inTransaction: false}
				payload = nil
			}
			return
		}
		//
		// unblocked write to outchannel up to bfChannelSize messages
		//
		switch ns.Cmd {
		case common.CmdEOR:
			newPayload := ns.Payload[5:]
			if len(payload) == 0 {
				payload = newPayload
			} else {
				if len(newPayload) > 0 {
					payload = append(payload, newPayload...)
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, "Appended payload", len(payload), len(newPayload))
					}
				}
			}
			eor := int(ns.Payload[0] - '0')
			rqId := (uint32(ns.Payload[1]) << 24) + (uint32(ns.Payload[2]) << 16) + (uint32(ns.Payload[3]) << 8) + uint32(ns.Payload[4])
			atomic.StoreUint32(&(worker.sqlStartTimeMs), 0) // Reset the sqlStartTimeMs to avoid being picked up during saturation/recover event
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "workerclient (<<< pid =", worker.pid, ",wrqId:", worker.rqId, "): EOR code:", eor, ", rqId: ", rqId, ", data:", DebugString(payload))
			}
			if eor == common.EORFree {
				worker.setState(wsFnsh)
				/*worker.sqlStartTimeMs = 0
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, "workerclient sqltime=", worker.sqlStartTimeMs)
				}*/
			} else {
				worker.setState(wsWait)
			}
			if eor != common.EORMoreIncomingRequests {
				worker.outCh <- &workerMsg{data: payload, eor: true, free: (eor == common.EORFree), inTransaction: ((eor == common.EORInTransaction) || (eor == common.EORInCursorInTransaction)), rqId: rqId}
				payload = nil
			} else {
				// buffer data to avoid race condition
				// the data will be sent after the EOR that we expect to be sent by the worker when responding to the next request
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, "EORMoreIncomingRequests, buffering data", len(payload))
				}
			}

		case common.CmdControlMsg:
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "workerclient (<<< pid =", worker.pid, "): got control message, ", ns.Payload)
			}
			if len(payload) > 0 {
				worker.outCh <- &workerMsg{data: payload, eor: false, free: false, inTransaction: false}
				payload = nil
			}
			return
		default:
			if ns.Cmd != common.RcStillExecuting {
				worker.setState(wsWait)
			}
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "workerclient (<<< pid =", worker.pid, "): data:", DebugString(ns.Serialized), len(ns.Serialized))
			}
			if len(payload) > 0 {
				worker.outCh <- &workerMsg{data: payload, eor: false, free: false, inTransaction: false}
				payload = nil
			}
			worker.outCh <- &workerMsg{data: ns.Serialized, eor: false, free: false, inTransaction: false, ns: ns}
		}
	}
}

// Write sends a message to the worker
func (worker *WorkerClient) Write(ns *netstring.Netstring, nsCount uint16) error {
	worker.setState(wsBusy)

	worker.rqId += uint32(nsCount)

	//
	// racmaint query could come after a worker is already terminated during mux shutdown.
	//
	if worker.workerConn == nil {
		return errors.New("writing to a closed workerconn")
	}
	err := WriteAll(worker.workerConn, ns.Serialized)
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "workerclient (>>>worker pid =", worker.pid, ", rqID =", worker.rqId, " ): ", DebugString(ns.Serialized))
	}
	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "workerclient write error:", err.Error())
		}
		worker.Terminate()
	}
	return err
}

// setState updates the worker state
func (worker *WorkerClient) setState(status HeraWorkerStatus) {
	if worker.Status == status {
		return
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "worker pid=", worker.pid, " changing status from", worker.Status, "to", status)
	}

	// TODO: sync atomic set
	worker.Status = status

	GetStateLog().PublishStateEvent(StateEvent{eType: WorkerStateEvt, shardID: worker.shardID, wType: worker.Type, instID: worker.instID, workerID: worker.ID, newWState: status})
}

// Channel returns the worker out channel
func (worker *WorkerClient) channel() <-chan *workerMsg {
	// TODO: should remove this method, and instead have Write(...) return (<- chan *netstring.Netstring, err) ?
	return worker.outCh
}

// isProcessRunning checks if the corresponding worker process is running
func (worker *WorkerClient) isProcessRunning() bool {
	process, err := os.FindProcess(worker.pid)
	if err != nil {
		// right now on Unix erp is always nil
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "workerclient pid=", worker.pid, ", find process error", err.Error())
		}
		syscall.Kill(worker.pid, syscall.SIGKILL)
		return false
	}
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		return false
	}
	return true
}
