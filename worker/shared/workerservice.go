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

package shared

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/config"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

const (
	signalRecover = 1
	signalExit    = 2
)

const envMuxStartTimeSec string = "MUX_START_TIME_SEC"
const envMuxStartTimeUsec string = "MUX_START_TIME_USEC"
const envCalClientSession string = "CAL_CLIENT_SESSION"
const envDBHostName string = "DB_HOSTNAME"
const envModule string = "HERA_NAME"
const envLogPrefix string = "logger.LOG_PREFIX"

type workerConfig struct {
	pin              []byte
	serverName       string
	muxStartTimeSec  int
	muxStartTimeUsec int
	clientSession    string
	dbHostName       string
	module           string
	hbInterval       time.Duration // 0 will set to default
}

// Start is the initial method, performing the initializations and starting runworker() to wait for requests
func Start(adapter CmdProcessorAdapter) {
	cfg, err := config.NewTxtConfig("hera.txt")
	if err != nil {
		fmt.Printf("Can't open config hera.txt")
		return
	}
	logLevel := cfg.GetOrDefaultInt("log_level", logger.Info)
	//
	// @TODO
	//
	logPrefix := os.Getenv(envLogPrefix)
	if logPrefix == "" {
		logPrefix = "WORKER"
	}
	err = logger.CreateLogger(cfg.GetOrDefaultString("log_file", "hera.log"), logPrefix, int32(logLevel))
	if err != nil {
		return
	}
	//
	// extracting environment parameter.
	//
	wconfig := &workerConfig{}
	wconfig.serverName = "hera"
	wconfig.muxStartTimeSec, err = strconv.Atoi(os.Getenv(envMuxStartTimeSec))
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "mux_start_time_sec defaults to 0")
		}
	}
	wconfig.muxStartTimeUsec, err = strconv.Atoi(os.Getenv(envMuxStartTimeUsec))
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "mux_start_time_usec defaults to 0")
		}
	}
	wconfig.clientSession = os.Getenv(envCalClientSession)
	wconfig.dbHostName = os.Getenv(envDBHostName)
	wconfig.module = os.Getenv(envModule)

	wconfig.hbInterval = (time.Duration(cfg.GetOrDefaultInt("db_heartbeat_interval", 120)) * time.Second)
	if wconfig.hbInterval == 0 {
		wconfig.hbInterval = 120 * time.Second
	}

	logger.GetLogger().Log(logger.Info, "DB heartbeat interval:", wconfig.hbInterval)

	evt := cal.NewCalEvent(cal.EventTypeServerInfo, "worker-go-start", cal.TransOK, "")
	evt.Completed()
	//
	// set up uds.
	//
	sockMux := os.NewFile(uintptr(3), fmt.Sprintf("worker_sp%d", 0))

	cmdprocessor := NewCmdProcessor(adapter, sockMux)

	err = cmdprocessor.InitDB()
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Can't connect to DB:", err)
		}
		return
	}
	//
	// send back controlmessage.
	//
	// TODO: get real info
	payload := []byte("0 MyDB")
	WriteAll(sockMux, netstring.NewNetstringFrom(common.CmdControlMsg, payload))
	//
	// start worker mainloop.
	//
	runworker(sockMux, cmdprocessor, wconfig)
}

// runworker is the infinite loop, serving requests
func runworker(sockMux *os.File, cmdprocessor *CmdProcessor, cfg *workerConfig) {
	var ns *netstring.Netstring
	var ok = true
	var sig int
	var err error

	nschannel := readNextNetstring(sockMux)
	cmdprocessor.moreIncomingRequests = func() bool {
		return (len(nschannel) > 0)
	}
	sigchannel := waitForSignal()

outerloop:
	for {
		select {
		case <-time.After(cfg.hbInterval):
			// heartbeat to DB only when the worker is free.
			if cmdprocessor.heartbeat && cmdprocessor.isIdle() {
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, "sending heartbeat to DB")
				}

				ok := cmdprocessor.SendDbHeartbeat()
				if !ok {
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "master db is unavailable, worker exiting")
					}
					break outerloop
				}
			}
			continue

		case sig, ok = <-sigchannel:
			if sig == signalRecover {
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, sockMux.Name(), "worker recover")
				}
				evt := cal.NewCalEvent("WORKER", "recoverworker", cal.TransOK, "")
				evt.Completed()
				//
				// if recover fails, stop worker.
				//
				err = recoverworker(cmdprocessor, nschannel)
				if err != nil {
					break outerloop
				} else {
					continue
				}
			} else if sig == signalExit {
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, sockMux.Name(), "worker exiting")
				}
				break outerloop
			}
		case ns, ok = <-nschannel:
			cmdprocessor.rqId++
		}

		//
		// @TODO let !ok go.
		//
		if (ns == nil) || (!ok) {
			break
		}
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, sockMux.Name(), ": worker read <<<", DebugString(ns.Serialized))
		}
		//
		// process one netstring command at a time.
		//
		err = cmdprocessor.ProcessCmd(ns)
		if err != nil {
			if logger.GetLogger().V(logger.Warning) {
				msg := string(ns.Serialized)
				if len(msg) > 20 {
					msg = msg[:20]
				}
				logger.GetLogger().Log(logger.Warning, "Error:", err.Error(), " - processing", ns.Cmd, msg)
			}

			break outerloop
		}
	}

	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "======== worker exits")
	}
	sockMux.Close()
	sockMux = nil
}

/**
 * reading the next command from socketpair and sending it to commandchannel.
 * block on read. exit only when readnext returns an error.
 */
func readNextNetstring(sockMux *os.File) <-chan *netstring.Netstring {
	//
	// up to 10 ns substrings will be queued up in the buffer.
	//
	commandch := make(chan *netstring.Netstring, 10)
	nsreader := netstring.NewNetstringReader(sockMux)
	go func() {
		for {
			ns, err := nsreader.ReadNext()
			if err != nil {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, sockMux.Name(), ":worker readerr", err.Error())
				}
				commandch <- nil
			} else {
				commandch <- ns
			}
		}
		//close(commandch)
	}()
	return commandch
}

// waitForSignal runs in its goroutine waiting for signals. When a signal is received, a message is sent to the
// channel where the main processor listen. There are two signals used: SIGHUP - used when the mux asks the worker to interrups to current work
// and SIGTERM - used when the workewr is asked to exit
func waitForSignal() <-chan int {
	recoverch := make(chan int)

	schannel := make(chan os.Signal, 1)
	signal.Notify(schannel, syscall.SIGHUP, syscall.SIGTERM)
	go func(sigchannel chan os.Signal) {
	outerloop:
		for {
			select {
			case signal := <-sigchannel:
				switch signal {
				case syscall.SIGHUP:
					recoverch <- signalRecover
				case syscall.SIGTERM:
					recoverch <- signalExit
					break outerloop
				}
			}
		}
		close(schannel)
	}(schannel)
	return recoverch
}

// recoverworker drains the mux channel and rollbacks the current transaction
func recoverworker(cmdprocessor *CmdProcessor, nschannel <-chan *netstring.Netstring) error {
	drainIncomingChannel(cmdprocessor, nschannel)
	err := cmdprocessor.ProcessCmd(netstring.NewNetstringFrom(common.CmdRollback, []byte("")))
	return err
}

// drainIncomingChannel clears the mux channel
func drainIncomingChannel(cmdprocessor *CmdProcessor, nschannel <-chan *netstring.Netstring) {
	for {
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "draining nschannel")
		}
		select {
		case ns, ok := <-nschannel:
			if !ok {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, "draining: nschannel closed")
				}
				return
			}
			cmdprocessor.rqId++
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "nschannel draining", DebugString(ns.Serialized))
			}
			//
			// let readNextnetstring.Netstring reload nschannel if chann buffer was full.
			//
			if len(nschannel) != 0 {
				time.Sleep(time.Microsecond * 10)
			}
		default:
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "draining: nschannel empty")
			}
			return
		}
	}
}
