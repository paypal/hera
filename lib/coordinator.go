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
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

// Coordinator is the entity managing a client session. It receives commands from the client connection and allocates
// one or more workers to execute the request. After the request is completed it will free the worker(s) if they are not needed.
// It is possible that a request will start a transaction, in which case the worker will staty allocated until the transaction
// is completed (with COMMIT) or canceled (with ROLLBACK)
type Coordinator struct {
	conn          net.Conn                    // used to send back the response(s)
	clientchannel <-chan *netstring.Netstring // channel where client netstring arrives
	ctx           context.Context
	done          chan int
	sqlParser     common.SQLParser

	corrID         *netstring.Netstring
	preppendCorrID bool
	// tells if the current request is SELECT
	isRead bool
	// for debugging
	id        string
	sqlhash   int32
	shard     *shardInfo
	prevShard *shardInfo

	workerpool    *WorkerPool   // if it is in transaction/in cursor, the pool of the worker attached
	worker        *WorkerClient // if it is in transaction/in cursor, the worker attached
	inTransaction bool          // if the worker is in transaction
	ticket        string        // the ticket for the worker

	// if the current netstring is compositie, cache the subnetstrings so that it's not parsed again
	nss []*netstring.Netstring

	// if this handles an internal client like rac maintenance config or shard config
	isInternal bool
}

// NewCoordinator creates a coordinator, clientchannel is used to read the requests, conn is used to write responses
func NewCoordinator(ctx context.Context, clientchannel <-chan *netstring.Netstring, conn net.Conn) *Coordinator {
	coordinator := &Coordinator{clientchannel: clientchannel, conn: conn, ctx: ctx, done: make(chan int, 1), id: conn.RemoteAddr().String(), shard: &shardInfo{sessionShardID: -1}, prevShard: &shardInfo{sessionShardID: -1}}
	var err error
	coordinator.sqlParser, err = common.NewRegexSQLParser()
	if err != nil {
		logger.GetLogger().Log(logger.Alert, coordinator.id, "Can't create regex SQL parser, R/W disabled, error:", err.Error())
		coordinator.sqlParser = common.NewDummyParser()
	}
	if conn.RemoteAddr().Network() == "pipe" {
		coordinator.isInternal = true
	}
	return coordinator
}

// Run is designed to be hosted by a constantly running goroutine
// for the duration of a client connection. client requests are picked off
// through clientchannel one at a time and handed over to DispatchSession.
// A session contains a collection of client netstrings within the same transactional
// context. it starts from a "fresh" client request and ends at a client request receiving
// an worker EOR with intrans=false, at which point the next client request is the start
// of another "fresh" client request. a simple session may consist of a single client
// netstring command (e.g. a read query). a more complicated session may span over
// several client netstring commands (e.g. a update netstring followed by a
// commit/rollback netstring).
// when processing session with multiple client netstring commands, Run() reads off the
// first netstring command to start a session, while the subsequent netstring commands
// within the same session are read off by corresponding session dispatching functions.
// as of now, dispatchDefaultSession is the only such dispatching function allowed to
// handle session with multiple netstring commands. Caching/Routing sessions contains
// read queries with no open cursor, where multiple client netsting commands in those
// sessions are treated as client protocol errors
// during the lifecyle of a coordinator, multiple sessions could be processed, each
// ends on an eor free from the worker. at the end of each session, flow control is
// returned back to Run(), and the next client request is parsed again before dispatching
func (crd *Coordinator) Run() {
	defer crd.conn.Close()
	idleTimeoutMs := time.Duration(GetIdleTimeoutMs()) * time.Millisecond
	idleTimer := time.NewTimer(idleTimeoutMs)
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, crd.id, "idle timeout", GetIdleTimeoutMs(), GetTrIdleTimeoutMs())
	}
	idleTimerCh := idleTimer.C
	var workerChan <-chan *workerMsg
	var workerCtrlChan <-chan *workerMsg
	running := true
	for running {
		select {
		case ns, ok := <-crd.clientchannel:
			if !ok {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, crd.id, "Coordinator exiting (closed channel) ...")
				}
				if idleTimer != nil {
					idleTimer.Stop()
				}
				if crd.worker != nil {
					GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: crd.worker.shardID, wType: crd.worker.Type, instID: crd.worker.instID, oldCState: Assign, newCState: Idle})
					go crd.worker.Recover(crd.workerpool, crd.ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String()})
					crd.resetWorkerInfo()
				}
				return
			}
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, crd.id, "coordinator run got client request.")
			}
			crd.nss = nil
			// new session
			handle, _ := crd.handleMux(ns)
			if !handle {
				// not handled by mux, it means it is a worker command

				// if the current worker is not in transaction we recover the current worker and dispatch to a new worker
				// the reason is that for R/W split it is possible that the new query needs to go to a write worker
				wk := crd.worker
				//
				// if current worker is in transaction, stay with it.
				//
				if (wk != nil) && !(crd.inTransaction) && (ns.IsComposite()) {
					GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: crd.worker.shardID, wType: crd.worker.Type, instID: crd.worker.instID, oldCState: Assign, newCState: Idle})
					go crd.worker.Recover(crd.workerpool, crd.ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String(), nameSuffix: "_SWITCH_RECOVER"}, common.StrandedSwitch)
					crd.resetWorkerInfo()
					//
					// ignore messages from recovering worker
					//
					workerChan = nil
					workerCtrlChan = nil
				}

				running = crd.dispatch(ns)
				if crd.worker != nil {
					workerChan = crd.worker.channel()
					workerCtrlChan = crd.worker.ctrlCh
				} else {
					workerChan = nil
					workerCtrlChan = nil
				}
			}
			if idleTimer != nil {
				if !idleTimer.Stop() {
					<-idleTimer.C
				}
				if crd.worker != nil {
					idleTimeoutMs = time.Duration(GetTrIdleTimeoutMs()) * time.Millisecond
				} else {
					idleTimeoutMs = time.Duration(GetIdleTimeoutMs()) * time.Millisecond
				}
				idleTimer.Reset(idleTimeoutMs)
			}

		case <-idleTimerCh:
			crd.done <- int(idleTimeoutMs / time.Millisecond)
			idleTimerCh = nil
			idleTimer = nil

		case msg, ok := <-workerChan:
			if !ok {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, crd.id, "Run: worker closed, exiting")
				}

				calmsg := fmt.Sprintf("worker (type%d,inst%d,id%d) %d closed connection on coordinator", crd.worker.Type, crd.worker.instID, crd.worker.ID, crd.worker.pid)
				evtname := "unexpected_eof"
				et := cal.NewCalEvent(cal.EventTypeWarning, evtname, cal.TransOK, calmsg)
				et.Completed()
				if crd.worker != nil {
					GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: crd.worker.shardID, wType: crd.worker.Type, instID: crd.worker.instID, oldCState: Assign, newCState: Idle})
				}
				return
			}
			// Sometimes Oracle return IN_TRANSACTION for read requests
			if !crd.isRead {
				crd.inTransaction = msg.inTransaction
			}
			msglen := len(msg.data)
			if msglen > 0 {
				_, err := crd.conn.Write(msg.data)
				if err != nil {
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, crd.id, "Fail to reply to client")
					}
					if crd.worker != nil {
						GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: crd.worker.shardID, wType: crd.worker.Type, instID: crd.worker.instID, oldCState: Assign, newCState: Idle})
						//
						// not a worker failure. recover worker if failed to write to client
						//
						go crd.worker.Recover(crd.workerpool, crd.ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String()})
					}
					return
				}

				if msglen >= 64*1024 {
					evt := cal.NewCalEvent(EvtTypeMux, "large_payload_out", cal.TransOK, "")
					evt.AddDataInt("len", int64(msglen))
					evt.Completed()
				}
			}

			if msg.free {
				if crd.worker != nil {
					atomic.StoreUint32(&(crd.worker.sqlStartTimeMs), 0)
					GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: crd.worker.shardID, wType: crd.worker.Type, instID: crd.worker.instID, oldCState: Assign, newCState: Idle})
				}
				crd.workerpool.ReturnWorker(crd.worker, crd.ticket)
				crd.resetWorkerInfo()
				workerChan = nil
				workerCtrlChan = nil
				if idleTimer != nil {
					if !idleTimer.Stop() {
						<-idleTimer.C
					}
					idleTimeoutMs = time.Duration(GetIdleTimeoutMs()) * time.Millisecond
					idleTimer.Reset(idleTimeoutMs)
					idleTimerCh = idleTimer.C
				}
			}

		case msg, ok := <-workerCtrlChan:
			if !ok {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, crd.id, "Run: worker ctrlchan closed, exiting")
				}
				et := cal.NewCalEvent(cal.EventTypeWarning, "workerCtrlChanClosed(run)", cal.TransOK, "")
				et.Completed()
				if crd.worker != nil {
					GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: crd.worker.shardID, wType: crd.worker.Type, instID: crd.worker.instID, oldCState: Assign, newCState: Idle})
					//
					// a worker failure. do not need to recover worker.
					//
					//go crd.worker.Recover(crd.workerpool, crd.ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String()})
				}
				return
			}
			if msg.abort {
				if crd.worker != nil {
					atomic.StoreUint32(&(crd.worker.sqlStartTimeMs), 0)

					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, crd.id, "Run: worker ctrlchan abort", crd.worker.pid)
					}
					GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: crd.worker.shardID, wType: crd.worker.Type, instID: crd.worker.instID, oldCState: Assign, newCState: Idle})
					go crd.worker.Recover(crd.workerpool, crd.ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String(), nameSuffix: "_SATURATION_RECOVERED"}, common.StrandedSaturationRecover)
					crd.resetWorkerInfo()
				} else {
					// this should not happen, log in case it happens
					logger.GetLogger().Log(logger.Alert, crd.id, "Abort received from unknown worker")
				}
				if msg.bindEvict {
					crd.processError(ErrBindEviction)
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, crd.id, "Coordinator sending bind evict err")
					}
				} else {
					crd.processError(ErrSaturationKill)
				}
				return
			}
		}
	}
	if idleTimer != nil {
		idleTimer.Stop()
	}
	//
	// if coordinator has to bail out, we need to inform requesthandler the same
	// by close the client connection. this way requesthandler gets EOF from client
	//
	if crd.worker != nil { // for a client disconn
		et := cal.NewCalEvent("HERAMUX", "CATCH_CLIENT_DROP_FREE_WORKER", cal.TransOK, "")
		et.AddDataStr("raddr", crd.conn.RemoteAddr().String())
		et.AddDataStr("worker_pid", fmt.Sprintf("%d",crd.worker.pid))
		et.Completed()

		GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: crd.worker.shardID, wType: crd.worker.Type, instID: crd.worker.instID, oldCState: Assign, newCState: Idle})
		go crd.worker.Recover(crd.workerpool, crd.ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String()})
		crd.resetWorkerInfo()
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, crd.id, "Coordinator exiting ...")
	}
}

func (crd *Coordinator) dispatch(request *netstring.Netstring) bool {
	if GetConfig().EnableTAF && (crd.worker == nil) {
		taferr := crd.DispatchTAFSession(request)
		crd.processError(taferr)
		return (taferr == nil)
	}

	deferr := crd.dispatchRequest(request)
	crd.processError(deferr)
	return (deferr == nil)
}

func (crd *Coordinator) computeSQLHash(request *netstring.Netstring) {
	hash, found := ExtractSQLHash(request)
	if found {
		crd.sqlhash = int32(hash)
	}
}

// Check for multiple command in a given NetString
func (crd *Coordinator) parseCmd(request *netstring.Netstring) (hasPrepare bool, hasCommit bool, hasRollback bool, parseErr error) {
	foundPrepare := false
	foundCommit := false
	foundRollback := false
	if request == nil {
		return foundPrepare, foundCommit, foundRollback, ErrReqParseFail
	}

	if request.IsComposite() {
		if crd.nss == nil {
			crd.nss, parseErr = netstring.SubNetstrings(request)
			if parseErr != nil {
				return foundPrepare, foundCommit, foundRollback, parseErr
			}
		}
		for _, ns := range crd.nss {
			if (ns.Cmd == common.CmdPrepare) || (ns.Cmd == common.CmdPrepareV2) || (ns.Cmd == common.CmdPrepareSpecial) {
				foundPrepare = true
			} else if ns.Cmd == common.CmdCommit {
				foundCommit = true
			} else if ns.Cmd == common.CmdRollback {
				foundRollback = true
			}
		}
		return foundPrepare, foundCommit, foundRollback, nil
	} else {
		ns := request
		if (ns.Cmd == common.CmdPrepare) || (ns.Cmd == common.CmdPrepareV2) || (ns.Cmd == common.CmdPrepareSpecial) {
			return true, false, false, nil
		} else if ns.Cmd == common.CmdCommit {
			return false, true, false, nil
		} else if ns.Cmd == common.CmdRollback {
			return false, false, true, nil
		}
	}
	return false, false, false, nil
}

/*
 * it handles the command if it is the case. if the command is indended for a worker, it will return false.
 * worker commands start with one of the prepare/prepare_v2
 */
func (crd *Coordinator) handleMux(request *netstring.Netstring) (bool, error) {
	crd.isRead = false
	crd.preppendCorrID = (crd.worker == nil)
	if request.IsComposite() {
		// TODO: avoid full parsing if necessary
		// if this is a worker command, only a shallow parse might be needed (if sharding is enabled, full parsing still needed anyway)
		nss, err := netstring.SubNetstrings(request)
		if err != nil {
			return false, err
		}
		crd.nss = nss
		for _, ns := range nss {
			if (ns.Cmd == common.CmdPrepare) || (ns.Cmd == common.CmdPrepareV2) || (ns.Cmd == common.CmdPrepareSpecial) {
				crd.sqlhash = int32(utility.GetSQLHash(string(ns.Payload)))
				crd.isRead = crd.sqlParser.IsRead(string(ns.Payload))
				handled := false
				if GetConfig().EnableSharding {
					hangup, err := crd.PreprocessSharding(nss)
					if err != nil {
						handled = true
						if logger.GetLogger().V(logger.Debug) {
							logger.GetLogger().Log(logger.Debug, crd.id, "Error preprocessing sharding, hangup:", err.Error(), hangup)
						}
						if hangup {
							crd.conn.Close()
						}
					}
				}
				return handled, err
			}

			handled, err := crd.processMuxCommand(ns)
			if !handled {
				if nss[0].Cmd == common.CmdClientCalCorrelationID {
					crd.preppendCorrID = false
				}
				return false, err
			}
		}
		return true, nil
	}
	crd.nss = nil
	// an individual request
	if (request.Cmd == common.CmdPrepare) || (request.Cmd == common.CmdPrepareV2) || (request.Cmd == common.CmdPrepareSpecial) {
		crd.isRead = crd.sqlParser.IsRead(string(request.Payload))
		return false, nil
	}
	return crd.processMuxCommand(request)
}

/*
 * process one mux command
 */
func (crd *Coordinator) processMuxCommand(request *netstring.Netstring) (bool, error) {
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, crd.id, "Mux handle command:", request.Cmd)
	}

	switch request.Cmd {
	case common.CmdClientCalCorrelationID:
		crd.corrID = request
	case common.CmdServerPingCommand:
		crd.respond([]byte("4:1009,"))
	case common.CmdBacktrace: // TODO passing command to worker
	case common.CmdClientInfo:
		crd.processClientInfoMuxCommand(string(request.Payload))
	case common.CmdCommit, common.CmdRollback:
		if crd.worker != nil {
			// in transaction, it will be handled by the worker
			return false, nil
		}
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, crd.id, "Mux handle command sent alone:", request.Cmd)
		}
		// send OK. client did not need to send it, but it is a NOOP anyway
		crd.respond([]byte("1:5,"))
	// TODO: add all other case ...
	case common.CmdFetch:
		if crd.worker != nil {
			return false, nil
		}
		crd.respond([]byte("41:2 fetch requested but no statement exists,"))
	case common.CmdPrepare, common.CmdPrepareV2, common.CmdPrepareSpecial:
		return false, nil
	// sharding commands
	case common.CmdSetShardID:
		err := crd.processSetShardID(request.Payload)
		if err == nil {
			// send OK
			crd.respond([]byte("1:5,"))
		} else {
			ns := netstring.NewNetstringFrom(common.RcError, []byte(err.Error()))
			crd.respond(ns.Serialized)
			// critical error, close
			crd.conn.Close()
			return true, err
		}
	case common.CmdGetNumShards:
		numShards := fmt.Sprintf("%d", GetConfig().NumOfShards)
		ns := netstring.NewNetstringFrom(common.RcOK, []byte(numShards))
		crd.respond(ns.Serialized)
	default:
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, crd.id, "Mux ignores command:", request.Cmd)
		}
	}
	return true, nil
}

/*
 * answers to the client info command with this server information. also it logs to cal the client info
 */
func (crd *Coordinator) processClientInfoMuxCommand(clientInfo string) {
	hostname, _ := os.Hostname()
	// C++ server has cap of 39 characters
	if len(hostname) >= 40 {
		hostname = hostname[:39]
	}
	serverInfo := fmt.Sprintf("%s:load_saved_sessions*CalThreadId=0*TopLevelTxnStartTime=TopLevelTxn not set*Host=%s",
		cal.GetCalClientInstance().GetPoolName(), hostname)
	ns := netstring.NewNetstringFrom(common.RcOK, []byte(serverInfo))
	crd.respond(ns.Serialized)
	var poolName string
	prefix := "Poolname: "
	pos := strings.LastIndex(clientInfo, prefix)
	if pos != -1 {
		pos += len(prefix)
		poolName = clientInfo[pos:]
		end := strings.Index(poolName, ",")
		if end != -1 {
			poolName = poolName[:end]
		}
	} else {
		poolName = "UNKNOWN"
	}

	et := cal.NewCalEvent(cal.EventTypeClientInfo, poolName, cal.TransOK, "mux")
	et.AddDataStr("raddr", crd.conn.RemoteAddr().String())
	// TODO: cal pool stack stuff
	calInstance := cal.GetCalClientInstance()
	if calInstance.IsPoolstackEnabled() {
		prefix = "PoolStack: "
		pos := strings.LastIndex(clientInfo, prefix)
		if pos != -1 {
			pos += len(prefix)
			parentPoolStack := clientInfo[pos:]
			end := strings.Index(poolName, ",")
			if end != -1 {
				parentPoolStack = parentPoolStack[:end]
			}
			et.SetParentStack(parentPoolStack, "CLIENT_INFO")
		}
	}
	et.AddPoolStack()

	// extract corrID
	corrID := "NotSet"
	if crd.corrID != nil {
		cid := string(crd.corrID.Payload)
		pos = strings.Index(cid, "=")
		if pos != -1 {
			cid = cid[pos+1:]
			pos = strings.Index(cid, "&")
			if pos == -1 {
				corrID = cid
			} else {
				corrID = cid[:pos]
			}
		}
	}
	et.AddDataStr("corr_id", corrID)
	et.Completed()
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, crd.id, "client info:", clientInfo, "| server info:", serverInfo, "| corr_id:", corrID)
	}
}

func (crd *Coordinator) resetWorkerInfo() {
	crd.worker = nil
	crd.workerpool = nil
	crd.ticket = ""
	crd.inTransaction = false
}

/*
 * Starts running a session, which is a series of netstring.Netstrings executed by the same resource.
 * Session is completed when the worker sends EOR free, for example after a commit, a rollback
 * or as part of end-of-data if the request was a select+fetch
 *
 */
func (crd *Coordinator) dispatchRequest(request *netstring.Netstring) error {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator dispatchrequest: starting")
	}
	defer func() {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator dispatchrequest: exiting")
		}
	}()

	var err error
	workerpool := crd.workerpool
	worker := crd.worker
	ticket := crd.ticket
	xShardRead := false

	// check bind throttle
	_, ok := GetBindEvict().BindThrottle[uint32(crd.sqlhash)]
	if ok {
		wType := wtypeRW
		cfg := GetNumWorkers(crd.shard.shardID)
		if GetConfig().ReadonlyPct > 0 {
			if crd.isRead {
				wType = wtypeRO
				cfg = int( float64(cfg)*float64(GetConfig().ReadonlyPct)/100.0 );
			} else {
				cfg = int( float64(cfg)*float64(100-GetConfig().ReadonlyPct)/100.0 );
			}
		}
		numFree := GetStateLog().numFreeWorker(crd.shard.shardID, wType)
		heavyUsage := false
		thres := float64(GetConfig().BindEvictionTargetConnPct) / 100.0 * float64(cfg)
		if numFree < int(thres) {
			heavyUsage = true
		}
		if logger.GetLogger().V(logger.Verbose) {
			msg := fmt.Sprintf("bind throttle heavyUsage?%t free:%d cfg:%d pct:%d thres:%f", heavyUsage,
				numFree, cfg, GetConfig().BindEvictionTargetConnPct , thres)
			logger.GetLogger().Log(logger.Verbose, msg)
		}
		needBlock,throttleEntry := GetBindEvict().ShouldBlock(uint32(crd.sqlhash), parseBinds(request), heavyUsage)
		if needBlock {
			msg := fmt.Sprintf("k=%s&v=%s&allowEveryX=%d&allowFrac=%.5f&raddr=%s",
				throttleEntry.Name,
				throttleEntry.Value,
				throttleEntry.AllowEveryX,
				1.0/float64(throttleEntry.AllowEveryX),
				crd.conn.RemoteAddr().String())
			sqlhashStr := fmt.Sprintf("%d",uint32(crd.sqlhash))
			evt := cal.NewCalEvent("BIND_THROTTLE", sqlhashStr, "1", msg)
			evt.Completed()
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "bind throttle", sqlhashStr, msg)
			}
			ns := netstring.NewNetstringFrom(common.RcError, []byte(ErrBindThrottle.Error()))
			crd.respond(ns.Serialized)
			crd.conn.Close()
			return fmt.Errorf("bind throttle block")
		} else {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, "bind throttle allow",uint32(crd.sqlhash))
			}
		}
	}

	if worker == nil {
		if crd.isRead && (GetConfig().ReadonlyPct != 0) {
			workerpool, err = GetWorkerBrokerInstance().GetWorkerPool(wtypeRO, 0, crd.shard.shardID)
			if err != nil {
				return err
			}
			if crd.isInternal {
				worker, ticket, err = workerpool.GetWorker(crd.sqlhash, 0 /*no backlog timeout*/)
			} else {
				worker, ticket, err = workerpool.GetWorker(crd.sqlhash)
			}
			if err != nil {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, crd.id, "coordinator dispatchrequest: no worker in RO pool", err)
				}
				return err
			}
		} else {
			workerpool, err = GetWorkerBrokerInstance().GetWorkerPool(wtypeRW, 0, crd.shard.shardID)
			if err != nil {
				return err
			}
			if crd.isInternal {
				worker, ticket, err = workerpool.GetWorker(crd.sqlhash, 0 /*no backlog timeout*/)
			} else {
				worker, ticket, err = workerpool.GetWorker(crd.sqlhash)
			}
			if err != nil {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, crd.id, "coordinator dispatchrequest: no worker", err)
				}
				return err
			}
		}
	} else {
		if crd.isRead {
			if crd.shard.shardID != worker.shardID {
				// we allow this but we need to have a different worker since it is a different shard
				wType := wtypeRO
				if GetConfig().ReadonlyPct == 0 {
					wType = wtypeRW
				}

				evt := cal.NewCalEvent(EvtTypeMux, "cross_shard_request", cal.TransOK, "")
				evt.Completed()

				workerpool, err = GetWorkerBrokerInstance().GetWorkerPool(wType, 0, crd.shard.shardID)
				if err != nil {
					return err
				}
				worker, ticket, err = workerpool.GetWorker(crd.sqlhash)
				if err != nil {
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, crd.id, "coordinator dispatchrequest: no worker in RO pool during shardswitch", err)
					}
					return err
				}
				xShardRead = true
				// for now change change to fetch all
				// TODO: later when doing scatter-gather review this
				request = crd.removeFetchSize(request)
				if !crd.inTransaction {
					if logger.GetLogger().V(logger.Alert) {
						logger.GetLogger().Log(logger.Alert, crd.id, "Expected to be in transaction")
					}
				}
			}
		}
	}

	wait, err := crd.doRequest(crd.ctx, worker, request, crd.conn, nil)

	if !xShardRead {
		if wait {
			crd.worker = worker
			crd.workerpool = workerpool
			crd.ticket = ticket
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator dispatchrequest: waiting for client.")
			}
			return nil
		}
		crd.resetWorkerInfo()
	} else {
		// restore the shard info
		crd.copyShardInfo(crd.shard, crd.prevShard)
		crd.inTransaction = true
		// this can happen when Oracle returns inTransaction for read SQLs
		if wait {
			GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: worker.shardID, wType: worker.Type, instID: worker.instID, oldCState: Assign, newCState: Idle})
			go worker.Recover(workerpool, ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String()})
			return nil
		}
	}

	GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: worker.shardID, wType: worker.Type, instID: worker.instID, oldCState: Assign, newCState: Idle})

	if err == nil {
		workerpool.ReturnWorker(worker, ticket)
		return nil
	}

	crd.inTransaction = false
	if err != ErrWorkerFail {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, crd.id, "coordinator dispatchrequest: stranded conn", err.Error())
		}
		//
		// donot return a stranded worker. recover inserts a good worker back to pool.
		//
		if err == ErrSaturationKill {
			go worker.Recover(workerpool, ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String(), nameSuffix: "_SATURATION_RECOVERED"}, common.StrandedSaturationRecover)
		} else {
			go worker.Recover(workerpool, ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String()})
		}
	} else {
		//
		// worker failure or saturationkill will recover worker.
		//
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, crd.id, "coordinator dispatchrequest: worker failure", err.Error())
		}
	}
	return err
}

// Errors returned to the main loop for the connection
var (
	ErrClientFail = errors.New("Client error")
	ErrWorkerFail = errors.New("Worker error")
	ErrTimeout    = errors.New("Timeout")
	ErrCanceled   = errors.New("Canceled")
)


/* does not return all values from array binds */
func parseBinds(request *netstring.Netstring) (map[string]string) {
    out := make(map[string]string)

    requests, err := netstring.SubNetstrings(request)
    if err != nil {
	return out
    }

    sz := len(requests)
    for i := 0; i < sz; i++ {
        if requests[i].Cmd == common.CmdBindName {
            bindName := string(requests[i].Payload)
            for j:=1; i+j<sz; j++ {
                if requests[i+j].Cmd == common.CmdBindNum {
                    continue
                } else if requests[i+j].Cmd == common.CmdBindType {
                    continue
                } else if requests[i+j].Cmd == common.CmdBindValueMaxSize {
                    continue
                } else if requests[i+j].Cmd == common.CmdBindValue {
                    out[bindName] = string(requests[i+j].Payload)
                } else {
                    // i=3 i+j=5 ---- 3:name 4:value 5:name
                    i += j-2
                    break
                }
            }
        } // end if bind name
    }

    return out
}

/**
 * performs a SQL, which is a communication of request & responses until EOR_... is received, or some
 * exception happens (client disconnects, worker exits, timeout)
 * 2nd return parameter tells if the worker is still busy (in transaction or in cursor)
 */
func (crd *Coordinator) doRequest(ctx context.Context, worker *WorkerClient, request *netstring.Netstring, clientWriter io.Writer, rqTimer *time.Timer) (bool, error) {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator dorequeset: starting")
	}
	defer func() {
		//
		// only one coordinator can own the worker at one time, no lock required.
		//
		if worker != nil {
			worker.reqCount++
		}
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator dorequest: exiting")
		}
	}()

	now := time.Now().UnixNano()
	timesincestart := uint32((now - GetStateLog().GetStartTime()) / int64(time.Millisecond))
	atomic.StoreUint32(&(worker.sqlStartTimeMs), timesincestart)

	if request != nil {
		_/*isPrepare*/, isCommit, isRollback, parseErr := crd.parseCmd(request)
		if parseErr != nil {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "doRequest: can't parse the client request", parseErr)
			}
			return false, ErrReqParseFail
		}
		cnt := 1
		if request.IsComposite() {
			cnt = len(crd.nss)
			if cnt == 0 {
				logger.GetLogger().Log(logger.Alert, crd.id, "Unexpected embedded ns length")
			}
		}
		plusAnyCorrId := request
		if crd.preppendCorrID {
			corrID := crd.corrID
			if corrID == nil {
				corrID = netstring.NewNetstringFrom(common.CmdClientCalCorrelationID, []byte("CorrId=NotSet"))
			}
			var ns []*netstring.Netstring;
			if !request.IsComposite() {
				ns = make([]*netstring.Netstring, 2)
				ns[0] = corrID
				ns[1] = request
			} else { // composite
				rnss, _ := netstring.SubNetstrings(request)
				ns = make([]*netstring.Netstring, len(rnss)+1)
				ns[0] = corrID
				for i:=0; i<len(rnss); i++ {
					ns[i+1] = rnss[i]
				}
			}
			plusAnyCorrId = netstring.NewNetstringEmbedded(ns)
			cnt++
		}
		err := worker.Write(plusAnyCorrId, uint16(cnt))
		if err != nil {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, crd.id, "doRequest: can't send the session starter request to worker")
			}
			return false, ErrWorkerFail
		}
		timesincestart := uint32(0)
		if isCommit || isRollback { // set the sqlStartTimeMs to 0 to avoid recover routine to pick during saturation for OCC_COMMIT and OCC_ROLLBACK
			atomic.StoreUint32(&(worker.sqlStartTimeMs), 0)
		}
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "worker pid:", worker.pid, "crd sqlhash =", uint32(worker.sqlHash), "sqltime=", timesincestart)
		}
	}

	//
	// the assumption is each dosession deals with a single sql. if not, uncomment the sqlhash
	// extraction code in workerclient to reset worker.sqlHash on each prepare inside one
	// dosession.
	//
	atomic.StoreInt32(&(worker.sqlHash), crd.sqlhash)
	worker.sqlBindNs.Store(request)
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, crd.id, "crd sqlhash =", uint32(worker.sqlHash), "sqltime=", timesincestart)
	}

	logmsg := fmt.Sprintf("worker (type%d,inst%d,id%d) %d", worker.Type, worker.instID, worker.ID, worker.pid)

	idleTimer := time.NewTimer(time.Duration(GetTrIdleTimeoutMs()) * time.Millisecond)
	defer idleTimer.Stop()

	var timeout <-chan time.Time
	if rqTimer != nil {
		timeout = rqTimer.C
	}

	//
	// request string used to log eor status when there is a multiple_client_req
	//
	var reqStr string
	clientChannel := crd.clientchannel
	done := ctx.Done()
	for {
		select {
		case <-timeout:
			return false, ErrTimeout
		case <-idleTimer.C:
			crd.done <- GetTrIdleTimeoutMs()
			return false, ErrTimeout
		case ns, ok := <-clientChannel:
			if !ok {
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, crd.id, "doRequest: client channel closed", logmsg)
				}
				evt := cal.NewCalEvent(EvtTypeMux, "client_closed", cal.TransOK, "")
				evt.Completed()
				return false, ErrClientFail
			}
			if (ns != nil) && (ns.Cmd != common.CmdFetch) && (ns.Cmd != common.CmdCols) && (ns.Cmd != common.CmdColsInfo) {
				//
				// if one dorequest gets multiple multiple_client_req, do this once.
				//
				if len(reqStr) == 0 {
					var buf bytes.Buffer
					buf.WriteString("reqns=")
					if request != nil {
						buf.WriteString(DebugString(request.Serialized))
					}
					buf.WriteString(" reqcorrid=")
					if crd.corrID != nil {
						buf.WriteString(DebugString(crd.corrID.Serialized))
					}
					reqStr = buf.String()
				}
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, crd.id, "doSession: multiple client req", logmsg, DebugString(ns.Serialized), reqStr)
				}
				evt := cal.NewCalEvent(EvtTypeMux, "multiple_client_req", cal.TransOK, logmsg+fmt.Sprintf(", cmd=%s %s", DebugString(ns.Serialized), reqStr))
				evt.Completed()
			}
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, crd.id, "coordinator dorequest got client request")
			}

			if !idleTimer.Stop() {
				<-idleTimer.C
			}
			idleTimer.Reset(time.Duration(GetTrIdleTimeoutMs()) * time.Millisecond)

			// this is typically for the JDBCs "execute" use case
			//? TODO: we should actually modify the worker to send a IN_CURSOR_IN_TRANSACTION / IN_CURSOR_NOT_IN_TRANSACTION EOR after the execute
			cnt := 1
			if ns.IsComposite() {
				nss, err := netstring.SubNetstrings(ns)
				if err != nil {
					logger.GetLogger().Log(logger.Alert, crd.id, "Can't parse embedded ns, size", len(ns.Serialized))
					return false, ErrClientFail
				}
				cnt = len(nss)
			}

			err := worker.Write(ns, uint16(cnt))
			if err != nil {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, crd.id, "doRequest: can't send request to worker, err=", err)
				}
				return false, ErrWorkerFail
			}
			// disable timeout
			// TODO: support failover for these clients
			timeout = nil
		case <-done:
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "doRequest: request canceled")
			}
			// TODO: correct the event type to C++ name
			evt := cal.NewCalEvent(EvtTypeMux, "eor_late_or_recover", cal.TransOK, "")
			evt.Completed()
			return false, ErrCanceled
		case msg, ok := <-worker.channel():
			if !ok {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, crd.id, "doRequest: worker closed, exiting")
				}

				calmsg := logmsg + "closed connection on coordinator"
				evtname := "unexpected_eof"
				et := cal.NewCalEvent(cal.EventTypeWarning, evtname, cal.TransOK, calmsg)
				et.Completed()
				return false, ErrWorkerFail
			}
			msglen := len(msg.data)
			if msglen > 0 {
				// disable timeout once response was sent to the client
				timeout = nil

				_, err := clientWriter.Write(msg.data)
				if err != nil {
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, crd.id, "Fail to reply to client")
					}
					return false, ErrClientFail
				}

				if msglen >= 64*1024 {
					evt := cal.NewCalEvent(EvtTypeMux, "large_payload_out", cal.TransOK, "")
					evt.AddDataInt("len", int64(msglen))
					evt.Completed()
				}
			}

			if msg.free {
				if msg.rqId != worker.rqId {
					evname := "crqId"
					if (msg.rqId > worker.rqId) && ((worker.rqId > 10000) || (msg.rqId < 10000) /*rqId can wrap around to 0, this test checks that it did not just wrap*/) {
						// this is not expected, so log with different name
						evname = "crqId_Error"
					}
					e := cal.NewCalEvent("WARNING", evname, cal.TransOK, "")
					e.AddDataInt("mux", int64(worker.rqId))
					e.AddDataInt("wk", int64(msg.rqId))
					e.Completed()
				}

				atomic.StoreUint32(&(worker.sqlStartTimeMs), 0)
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, crd.id, "workersqltime=", worker.sqlStartTimeMs)
				}
				if len(reqStr) > 0 {
					evt := cal.NewCalEvent(EvtTypeMux, "multiple_client_req_get_eor_free", cal.TransOK, logmsg+fmt.Sprintf(", %s", reqStr))
					evt.Completed()
				}
				return false, nil
			}

			if msg.eor {
				// Sometimes Oracle return IN_TRANSACTION for read requests
				if !crd.isRead {
					crd.inTransaction = msg.inTransaction
				}
				if len(reqStr) > 0 {
					evt := cal.NewCalEvent(EvtTypeMux, "multiple_client_req_get_eor_intxn", cal.TransOK, logmsg+fmt.Sprintf(", %s", reqStr))
					evt.Completed()
				}
				return true, nil
			}
		case msg, ok := <-worker.ctrlCh:
			//
			// workerctrlchan is closed on worker restart. return worker error to skip recover.
			//
			if !ok {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, crd.id, "doRequest: worker ctrlchan closed, exiting")
				}
				et := cal.NewCalEvent(cal.EventTypeWarning, "workerCtrlChanClosed", cal.TransOK, "")
				et.Completed()
				return false, ErrWorkerFail
			}
			if msg.abort {
				//
				// reset sqlstarttime to prevent the same worker from saturationrecover again.
				//
				atomic.StoreUint32(&(worker.sqlStartTimeMs), 0)

				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, crd.id, "doRequest: worker ctrlchan abort")
				}
				if msg.bindEvict {
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, crd.id, "doRequest: worker ctrlchan bind evict")
					}
					return false, ErrBindEviction
				} else {
					return false, ErrSaturationKill
				}
			}
		}
	}
}

func (crd *Coordinator) respond(data []byte) error {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "Responded to client =", crd.id, ": ", DebugString(data))
	}
	return WriteAll(crd.conn, data)
}

/**
 * TODO other shard related error responses
 */
func (crd *Coordinator) processError(err error) {
	if err == nil {
		return
	}
	//
	// TODO fix the hardcoded error string.
	//
	if (err == ErrBklgTimeout) ||
		(err == ErrBklgEviction) ||
		(err == ErrBindEviction) ||
		(err == ErrRejectDbDown) ||
		(err == ErrSaturationKill) ||
		(err == ErrSaturationSoftSQLEviction) {
		ns := netstring.NewNetstringFrom(common.RcError, []byte(err.Error()))
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "error to client", string(ns.Serialized))
		}
		WriteAll(crd.conn, ns.Serialized)
	}
}

// Done returns the channel used when the coordinator is done
func (crd *Coordinator) Done() <-chan int {
	return crd.done
}
