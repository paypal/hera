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
	"net"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

// Errors
var (
	ErrDML = errors.New("DML not allowed")
)

// tafResponsePreproc it pre-processes the responses coming from the primary worker. If the response contain an ORA error,
// with a code in a given list (3113, 3114, 3135, 12514, 3128, 3127, 3123, 3111, 3106, 1012, 28, 31, 51, 25400, 25401, 25402, 25403, 25404, 25405, 25407, 25408, 25409, 25425, 24343, 1041, 600, 700, 7445)
// it discards the response, and it reports to the coordinator. The coordinator then will retry the request on a worker from the fallback pool
type tafResponsePreproc struct {
	// tells if the request was forwarded to the client, so the coordinator will not retry on the fallback.
	// the request was either successfull, or had some error which is "not-retriable"
	ok bool
	// the client connection to forward the response got from the worker
	conn net.Conn
	// for CAL, in case !ok, the ORA error
	ora string
	// tells if a partial response was sent to the client. It is used to basically disable the failover if that was the case
	dataSent bool
	// time when the first reply came
	replyTime int64
}

// Write is the prepocessor function. It is a filter between the worker and the client, it processes the response and it
// decides whether to forward the request or to discard it
func (p *tafResponsePreproc) Write(bf []byte) (int, error) {
	if p.replyTime == 0 {
		p.replyTime = time.Now().UnixNano()
	}
	ns, err := netstring.NewNetstring(bytes.NewReader(bf))
	if err == nil {
		if !p.dataSent /*if prior to this some response was alredy sent - then disable this check*/ {
			// look inside for SQLError
			if ns.Cmd == common.RcSQLError {
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, p.conn.RemoteAddr().String(), "TAF response: found SQL error", string(ns.Payload[:20]))
				}
				ora, sz := atoi(ns.Payload)
				switch ora {
				case 3113, 3114, 3135, 12514, 3128, 3127, 3123, 3111, 3106, 1012, 28, 31, 51, 25400, 25401, 25402, 25403, 25404, 25405, 25407, 25408, 25409, 25425, 24343, 1041, 600, 700, 7445:
					//for testing 962=<table doesn't exist>:	case 942:
					p.ok = false
					p.ora = string(ns.Payload[:sz])
				default:
					p.ok = true
				}
			}
		}
	} else {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, p.conn.RemoteAddr().String(), "TAF response error from worker:", err)
		}
	}
	if p.ok {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, p.conn.RemoteAddr().String(), "TAF response forwarded to client")
		}
		p.dataSent = true
		return p.conn.Write(bf)
	}
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, p.conn.RemoteAddr().String(), "TAF response dropped")
	}
	// not an error, but zero bytes sent to the client
	return 0, nil
}

// removeFetchSize replaces fetch chunk size value with zero. For simplicity we remove the fetch size hint. Fetch hint should not be used anyways for TAF case
func (crd *Coordinator) removeFetchSize(request *netstring.Netstring) *netstring.Netstring {
	nss := crd.nss
	for i := range nss {
		if nss[i].Cmd == common.CmdFetch {
			payload := nss[i].Payload
			if (len(payload) != 1) || (payload[0] != '0') {
				nss[i].Payload = []byte("0")
				nss[i].Serialized = []byte("3:7 0,")
				return netstring.NewNetstringEmbedded(crd.nss)
			}
		}
	}

	return request
}

// DispatchTAFSession starts running a session, which is a series of netstring.Netstrings executed by the same resource.
// Session is completed when the worker sends EOR free, for example after a commit, a rollback
// or as part of end-of-data if the request was a select+fetch
// When the primary database is OK all the requests go to it. When a request to the primary fails,
// (which can be because of a configured timeout or because of some ORA error) we remember into a structure TAF.
// A measure of the "health" for the primary database is how many queries are failing versus the total - see taf.go for details.
// When the primary database health decreases, we start sending the some requests directly to the fallback database.
// If the primary is completely down, we still send 1% of requests to the primary, as a "health check", so that
// we can c ompletely switch to the primary when the primary eventualy comes back up
func (crd *Coordinator) DispatchTAFSession(request *netstring.Netstring) error {
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, crd.id, "TAFSession: starting")
	}
	defer func() {
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, crd.id, "TAFSession: exiting")
		}
	}()

	request = crd.removeFetchSize(request)

	var worker *WorkerClient
	var ticket string
	var err error

	tf := GetTAF(crd.shard.shardID)
	primaryPool, err := GetWorkerBrokerInstance().GetWorkerPool(wtypeRW, 0, crd.shard.shardID)
	if err != nil {
		// wow, is this possible?
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, crd.id, "No primary pool")
		}
		return err
	}

	// if sql is registered and slow, we should run without fallback
	var tq *TafQueries
	queryNormallySlow := false
	if GetConfig().TAFBinDuration > 0 {
		tq = GetTafQueries(crd.shard.shardID)
		queryNormallySlow, err = tq.IsNormallySlow(crd.sqlhash)
	}

	// we run without fallback if the fallback pool isn't healthy
	fallbackPool, err := GetWorkerBrokerInstance().GetWorkerPool(wtypeStdBy, 0, crd.shard.shardID)
	if (err != nil) || !fallbackPool.Healthy() {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "Fallback not healthy, not using failover")
		}
		err = crd.dispatchRequest(request)
		return err
	}

	usePrimary := tf.UsePrimary()
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "usePrimary=", usePrimary)
	}
	if usePrimary {
		if primaryPool.Healthy() {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "Will try first the primary pool")
			}
			worker, ticket, err = primaryPool.GetWorker(crd.sqlhash, 0 /*no wait in backlog*/)
			if err == nil {
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, crd.id, "Trying first pool")
				}
				respProcessor := &tafResponsePreproc{conn: crd.conn, ok: true, dataSent: false}

				timeout := time.Duration(GetConfig().TAFTimeoutMs) * time.Millisecond
				if queryNormallySlow {
					timeout = 3600 * time.Second
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, crd.id, "Running without failover, slow query", uint32(crd.sqlhash))
					}
					evt := cal.NewCalEvent("TafNormSlow", fmt.Sprintf("%d", uint32(crd.sqlhash)), cal.TransOK, "")
					if GetConfig().EnableSharding {
						evt.AddDataInt("sh", int64(crd.shard.shardID))
					}
					evt.Completed()
				}
				if crd.isInternal {
					timeout = 3600 * time.Second
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, crd.id, "Running without failover, internal query", uint32(crd.sqlhash))
					}
				}

				rqTimer := time.NewTimer(timeout)
				startTime := time.Now()
				var timeUsed time.Duration
				var wait bool
				wait, err = crd.doRequest(crd.ctx, worker, request, respProcessor, rqTimer)
				if wait {
					// this should not happen for real, because TAF queries are read only
					if GetConfig().TestingEnableDMLTaf {
						crd.worker = worker
						crd.workerpool = primaryPool
						crd.ticket = ticket
						if logger.GetLogger().V(logger.Verbose) {
							logger.GetLogger().Log(logger.Verbose, crd.id, "DML in TAF allowed for testing")
						}
						if respProcessor.ok {
							tf.NotifyOK()
						} else {
							tf.NotifyError()
						}
						return nil
					}
					GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: worker.shardID, wType: worker.Type, instID: worker.instID, oldCState: Assign, newCState: Idle})
					go worker.Recover(primaryPool, ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String()})
					return ErrDML
				}
				GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: worker.shardID, wType: worker.Type, instID: worker.instID, oldCState: Assign, newCState: Idle})
				if respProcessor.replyTime == 0 { // when a timeout happens
					timeUsed = time.Since(startTime)
				} else {
					timeUsed = time.Duration(respProcessor.replyTime-startTime.UnixNano()) * time.Nanosecond
				}
				if err == ErrTimeout && tq != nil && tf.GetPct() >= 105 {
					tq.RecordTimeout(crd.sqlhash)
				}
				rqTimer.Stop()

				if err == nil {
					primaryPool.ReturnWorker(worker, ticket)
					if respProcessor.ok {
						tf.NotifyOK()
						return nil
					}
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, crd.id, "ORA error trying first pool")
					}
					tf.NotifyError()
					evt := cal.NewCalEvent(EvtTypeTAF, EvtNameTAFOra+respProcessor.ora, cal.TransOK, "")
					if GetConfig().EnableSharding {
						evt.AddDataInt("sh", int64(crd.shard.shardID))
					}
					evt.Completed()
				} else {
					if logger.GetLogger().V(logger.Debug) {
						logger.GetLogger().Log(logger.Debug, crd.id, "Error trying first pool:", err)
					}
					if err != ErrWorkerFail {
						if err == ErrReqParseFail {
							if logger.GetLogger().V(logger.Warning) {
								logger.GetLogger().Log(logger.Warning, "TAFSession: can't parse the client request", err.Error())
							}
							et := cal.NewCalEvent(EvtTypeMux, "TAFSession_primary_request_parse_fail", cal.TransWarning, err.Error())
							et.Completed()
							if logger.GetLogger().V(logger.Warning) {
								logger.GetLogger().Log(logger.Warning, "Returning worker back to primary pool after ErrReqParseFail")
							}
							primaryPool.ReturnWorker(worker, ticket)
							// return err
						} else if err == ErrSaturationKill {
							go worker.Recover(primaryPool, ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String(), nameSuffix: "_SATURATION_RECOVERED"}, common.StrandedSaturationRecover)
						} else {
							go worker.Recover(primaryPool, ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String()})
						}
					} // if worker fail, it is terminated so no need to recover
					if respProcessor.dataSent {
						return err
					}

					if err == ErrTimeout {
						// it was a timeout, with no data sent already
						tf.NotifyError()
						evt := cal.NewCalEvent(EvtTypeTAF, EvtNameTAFTmo, cal.TransOK, "")
						evt.AddDataInt("pct", int64(tf.GetPct()))
						evt.AddDataInt("sqlhash", int64(uint32(crd.sqlhash)))
						evt.AddDataInt("timeout_ms", int64(timeout.Nanoseconds()/1000/1000))
						evt.AddDataInt("used_ms", int64(timeUsed.Nanoseconds()/1000/1000))
						if GetConfig().EnableSharding {
							evt.AddDataInt("sh", int64(crd.shard.shardID))
						}
						evt.Completed()
					} else {
						if (err == ErrWorkerFail) && !(respProcessor.ok) {
							if logger.GetLogger().V(logger.Debug) {
								logger.GetLogger().Log(logger.Debug, crd.id, "ORA error trying first pool, worker exiting")
							}
							tf.NotifyError()
							evt := cal.NewCalEvent(EvtTypeTAF, EvtNameTAFOra+respProcessor.ora, cal.TransOK, "")
							if GetConfig().EnableSharding {
								evt.AddDataInt("sh", int64(crd.shard.shardID))
							}
							evt.Completed()
						} else {
							return err
						}
					}
				}
			} else {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, crd.id, "Error getting worker for first pool:", err)
				}
				evt := cal.NewCalEvent(EvtTypeTAF, EvtNAmeTafBklg, cal.TransOK, "")
				evt.AddDataInt("pct", int64(tf.GetPct()))
				if GetConfig().EnableSharding {
					evt.AddDataInt("sh", int64(crd.shard.shardID))
				}
				evt.Completed()
			}
		}
	}

	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "Trying the falback pool")
	}

	var fbticket string
	worker, fbticket, err = fallbackPool.GetWorker(crd.sqlhash)
	if err == nil {
		var wait bool
		wait, err = crd.doRequest(crd.ctx, worker, request, crd.conn, nil)
		if wait {
			// this should not happen for real, because TAF queries are read only
			if GetConfig().TestingEnableDMLTaf {
				crd.worker = worker
				crd.workerpool = fallbackPool
				crd.ticket = fbticket
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, crd.id, "DML in TAF allowed for testing")
				}
				return nil
			}
			GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: worker.shardID, wType: worker.Type, instID: worker.instID, oldCState: Assign, newCState: Idle})
			go worker.Recover(primaryPool, ticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String()})
			return ErrDML
		}
		GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: worker.shardID, wType: worker.Type, instID: worker.instID, oldCState: Assign, newCState: Idle})

		if err == nil {
			fallbackPool.ReturnWorker(worker, fbticket)
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, crd.id, "Query finished using the fallback pool")
			}
		} else {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, crd.id, "Error trying the last pool:", err)
			}
			if err != ErrWorkerFail {
				if err == ErrReqParseFail {
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "TAFSession: can't parse the client request", err.Error())
					}
					et := cal.NewCalEvent(EvtTypeMux, "TAFSession_fallback_request_parse_fail", cal.TransWarning, err.Error())
					et.Completed()
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "Returning worker back to fallback pool after ErrReqParseFail")
					}
					fallbackPool.ReturnWorker(worker, fbticket)
					// return err
				} else if err == ErrSaturationKill {
					go worker.Recover(fallbackPool, fbticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String(), nameSuffix: "_SATURATION_RECOVERED"}, common.StrandedSaturationRecover)
				} else {
					go worker.Recover(fallbackPool, fbticket, &strandedCalInfo{raddr: crd.conn.RemoteAddr().String(), laddr: crd.conn.LocalAddr().String()})
				}
			}
		}
	} else {
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, crd.id, "Error getting worker for the last pool:", err)
		}
	}

	return err
}
