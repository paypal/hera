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
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"

	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// BouncerReasonCode defines the possible reason for bouncing
type BouncerReasonCode int

// BouncerReasonCode constants
const (
	BRCUnknown BouncerReasonCode = iota
	BRCNoActiveWorker
	BRCNoWorkerCapacity
	BRCConnectionLimit
)

// HandlerFunc defines the signature of the callback to handle the connection
type HandlerFunc func(net.Conn)

// Listener interface is used by the server to accept connections
type Listener interface {
	// Accept waits for and returns the next connection to the listener.
	Accept() (net.Conn, error)

	// Initialize the connection
	Init(net.Conn) (net.Conn, error)

	// Close closes the listener.
	// Any blocked Accept operations will be unblocked and return errors.
	Close() error
}

// Server contains the Run method which is the infinite loop
type Server interface {
	Run()
}

// server accepts connections from the Listener and after the validation checks it spawns a goroutine to handle it
type server struct {
	listener Listener
	handler  HandlerFunc

	//
	// last time bouncerrequired() is calls
	//
	capacityCheckTime int64
	capacityCheckCnt  int
	bouncerActivated  bool

	bouncerStartupDelayDone bool
	startShutdown           int64
}

// NewServer creates a server from the Lister and the function handling the connections accepted
func NewServer(lsn Listener, f HandlerFunc) Server {
	srv := &server{listener: lsn, handler: f, bouncerActivated: false, capacityCheckTime: 0, capacityCheckCnt: 0, bouncerStartupDelayDone: false}
	return srv
}

// FullShutdown kills the parent process if it is named occwatchdog and exits the process
func FullShutdown() {
	fileh, err := os.Open(fmt.Sprintf("/proc/%d/status", os.Getppid()))
	if err == nil {
		linescan := bufio.NewScanner(fileh)
		if linescan.Scan() {
			ln := linescan.Text()
			if strings.Contains(ln, "occwatchdog") || strings.Contains(ln, "occmux") {
				pproc, err := os.FindProcess(os.Getppid())
				if err == nil {
					if logger.GetLogger().V(logger.Alert) {
						logger.GetLogger().Log(logger.Alert, "killing parent occwatchdog", os.Getppid())
					}
					pproc.Kill()
				}

			}
		}
		fileh.Close()
	}

	os.Exit(9)
}

// Run has an infinite loop accepting connections and if it passes all the checks it spawns a go routine to handle
// each conection until the connection is closed. There are capacity checks and authentication checks, if any fails the connection is bounced.
func (srv *server) Run() {
	startTime := time.Now().Unix()
	startupDelay := GetConfig().BouncerStartupDelay
	pollInterval := GetConfig().BouncerPollInterval

	for {
		//
		// running out of fd (connection) can crash golang when attempting Accept().
		// skipping accept right away rather than checking it 4 times in bounceRequired.
		// client conn request is left dangling until we have capacity.
		//
		curConnCount := GetStateLog().GetTotalConnections()
		if curConnCount > 65536 {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "max fd reached, skipping/delaying accept.")
			}
			//
			// between panic over fd cap and delay 100 us, choose delay.
			//
			time.Sleep(time.Microsecond * 100)
			continue
		}

		if srv.startShutdown > 0 {
			if GetStateLog().GetTotalConnections() > 0 && time.Now().Unix()-srv.startShutdown < 62 {
				// still some connections, allow some time to finish
				time.Sleep(time.Microsecond * 100)
				continue
			}
			if logger.GetLogger().V(logger.Alert) {
				logger.GetLogger().Log(logger.Alert, "occwatchdog died, terminating")
			}
			return
		}

		conn, err := srv.listener.Accept()

		if srv.bounceRequired(startTime, startupDelay, pollInterval) {
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "bouncing", conn.RemoteAddr().String())
			}
			srv.bounce(conn)
			continue
		}

		if err != nil {
			if logger.GetLogger().V(logger.Alert) {
				logger.GetLogger().Log(logger.Alert, "server: accept: ", err.Error())
			}
			//
			// InetServerSocket.cpp::bounce()
			//
			e := cal.NewCalEvent(cal.EventTypeError, "ACCEPT", cal.TxnStatus(cal.TransError, "ASFIO", "ACCEPT_FAILED", "-1"), "")
			e.AddDataStr("fwk", "MUX") // occproxy/main.cpp
			if conn != nil {
				e.AddDataStr("raddr", conn.RemoteAddr().String())
				e.AddDataStr("laddr", conn.LocalAddr().String())
				conn.Close()
			}
			e.Completed()
			continue

			//break
		}
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "server: accepted from ", conn.RemoteAddr())
		}

		go srv.authAndHandle(conn, srv.handler)
	}
}

// authAndHandle calls the Listener Init. If successful it calls the handler, otherwise closes the connection
func (srv *server) authAndHandle(c net.Conn, f HandlerFunc) {
	conn, err := srv.listener.Init(c)
	if err == nil {
		f(conn)
	}

	e := cal.NewCalEvent("CLOSE", IPAddrStr(c.RemoteAddr()), cal.TransOK, "")
	e.AddDataStr("fwk", "occmuxgo")
	e.AddDataStr("raddr", c.RemoteAddr().String())
	e.AddDataStr("laddr", c.LocalAddr().String())
	e.Completed()

	c.Close()
}

/**
 * bouncer in c++ has a start delay of 10 seconds. upon onset of bouncing condition, it
 * also sleeps for 100 ms and comes back to reconfirm the bouncing condition. bouncing
 * conditional has to be reconfirmed 4 times before bouncer really kicks in.
 */
func (srv *server) bounceRequired(_startTime int64, _startupDelay int, _pollInterval int) bool {
	if !(GetConfig().BouncerEnabled) {
		return false
	}

	if !(srv.bouncerStartupDelayDone) {
		if time.Now().Unix() < (_startTime + int64(_startupDelay)) {
			return false
		}
		srv.bouncerStartupDelayDone = true
	}

	if (time.Now().UnixNano() - int64(time.Millisecond)*int64(_pollInterval)) < srv.capacityCheckTime {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "bouncer skipping", srv.capacityCheckTime, srv.capacityCheckCnt, srv.bouncerActivated)
		}
		return false
	}

	canAccept, rc, msg := srv.canAccept()
	if srv.bouncerActivated && canAccept {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "bouncer deactivated")
		}
	}
	//
	// even when server cannot accept, we do not start bouncing right away.
	//
	if !(canAccept) && !(srv.bouncerActivated) {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "bouncer checking checktime=", srv.capacityCheckTime, " checkcount=", srv.capacityCheckCnt, " activated=", srv.bouncerActivated, " msg=", msg)
		}
		//
		// beginning of a new wave of bouncing conditions.
		//
		if srv.capacityCheckTime == 0 {
			srv.capacityCheckTime = time.Now().UnixNano()
			srv.capacityCheckCnt = 1
			return false
		}
		srv.capacityCheckCnt++
		if srv.capacityCheckCnt > 3 {
			//
			// activate bouncer
			//
			e := cal.NewCalEvent(cal.EventTypeWarning, fmt.Sprintf("bouncer_activate_%d", rc), cal.TransOK, "")
			e.AddDataStr("reason", msg)
			e.Completed()
			srv.bouncerActivated = true
			srv.capacityCheckTime = 0
			srv.capacityCheckCnt = 0
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "bouncer activated")
			}
			return true
		}
		//
		// reset pooltime
		//
		srv.capacityCheckTime = time.Now().UnixNano()
		return false
	}
	//
	// if server can accept again, set srv.bouncerActivated to false
	//
	srv.bouncerActivated = !canAccept

	return srv.bouncerActivated
}

/*
	Check if there is capacity to handle the new connection
*/
func (srv *server) canAccept() (bool, BouncerReasonCode, string) {
	//
	// check init & quiesce. this rarely happens.
	//
	if !(GetStateLog().HasActiveWorker()) {
		return false, BRCNoActiveWorker, "no active worker"
	}

	maxout, msg := srv.maxConnReached()
	if maxout {
		return false, BRCConnectionLimit, msg
	}

	//
	// check proxy capacity
	//
	backlogCheck, bsize := GetStateLog().ProxyHasCapacity(GetConfig().GetBacklogLimit(wtypeRW, 0 /*shard 0*/), GetConfig().GetBacklogLimit(wtypeRO, 0 /*shard 0*/))
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "bouncer backlogcheck", backlogCheck, bsize, GetConfig().GetBacklogLimit(wtypeRW, 0 /*shard 0*/), GetConfig().GetBacklogLimit(wtypeRO, 0 /*shard 0*/))
	}
	if backlogCheck {
		if bsize > 0 {
			evt := cal.NewCalEvent("OCCMUX", "bklg_on_acpt", cal.TransOK, "")
			evt.Completed()
		}
		return backlogCheck, BRCUnknown, ""
	}
	return backlogCheck, BRCNoWorkerCapacity, "out of worker capacity"
}

func (srv *server) maxConnReached() (bool, string) {
	currentConn := GetStateLog().GetTotalConnections()
	var total int
	if GetConfig().EnableWhitelistTest {
		total = GetNumWorkers(0) + GetConfig().NumWhitelistChildren*(GetConfig().NumOfShards-1)
	} else {
		total = GetNumWorkers(0) * GetConfig().NumOfShards
	}

	// safety limit of 1500 incoming connections for each db connection
	// java connection pools make lots of idle connections
	connAllowed := 1500 * total
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "bouncer conn ", currentConn, connAllowed)
	}
	if currentConn >= connAllowed {
		return true, fmt.Sprintf("connection limit reached - conn_allowed: %d, cur_conn: %d", connAllowed, currentConn)
	}
	return false, ""
}

// logs the event and close the connection
func (srv *server) bounce(_conn net.Conn) {
	e := cal.NewCalEvent(cal.EventTypeWarning, "Bounce", cal.TxnStatus(cal.TransWarning, "SERVER", "BOUNCE", "-1"), "")
	if _conn != nil {
		e.AddDataStr("raddr", _conn.RemoteAddr().String())
		_conn.Close()
	}
	e.Completed()
}
