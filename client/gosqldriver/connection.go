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

// Package gosqldriver implements the database/sql/driver interfaces for the the Hera golang driver
package gosqldriver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
	"net"
	"os"
)

var corrIDUnsetCmd = netstring.NewNetstringFrom(common.CmdClientCalCorrelationID, []byte("CorrId=NotSet"))

type heraConnection struct {
	id     string // used for logging
	conn   net.Conn
	reader *netstring.Reader
	// for the sharding extension
	shardKeyPayload []byte
	// correlation id
	corrID     *netstring.Netstring
	clientinfo *netstring.Netstring

	// for context support (Go 1.8+)
	watching bool
	watcher  chan<- context.Context
	closech  chan struct{}
	finished chan<- struct{}
	canceled atomicError // set non-nil if conn is canceled
	closed   atomicBool  // set when conn is closed, before closech is closed
}

// NewHeraConnection creates a structure implementing a driver.Con interface
func NewHeraConnection(conn net.Conn) driver.Conn {
	hera := &heraConnection{conn: conn,
		id:      conn.RemoteAddr().String(),
		reader:  netstring.NewNetstringReader(conn),
		corrID:  corrIDUnsetCmd,
		closech: make(chan struct{}),
	}

	hera.startWatcher()
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, hera.id, "create driver connection")
	}
	return hera
}

func (heraConn *heraConnection) watchCancel(ctx context.Context) error {
	if heraConn.watching {
		// Reach here if canceled,
		// so the connection is already invalid
		heraConn.cleanup()
		return nil
	}
	// When ctx is already cancelled, don't watch it.
	if err := ctx.Err(); err != nil {
		return err
	}
	// When ctx is not cancellable, don't watch it.
	if ctx.Done() == nil {
		return nil
	}
	// When watcher is not alive, can't watch it.
	if heraConn.watcher == nil {
		return nil
	}

	heraConn.watching = true
	heraConn.watcher <- ctx
	return nil
}

// Closes the network connection and unsets internal variables. Do not call this
// function after successfully authentication, call Close instead. This function
// is called before auth or on auth failure because HERA will have already
// closed the network connection.
func (heraConn *heraConnection) cleanup() {
	if heraConn.closed.Swap(true) {
		return
	}

	// Makes cleanup idempotent
	close(heraConn.closech)
	if heraConn.conn == nil {
		return
	}
	heraConn.finish()
	if err := heraConn.conn.Close(); err != nil {
		logger.GetLogger().Log(logger.Alert, err)
	}
}

//error
func (heraConn *heraConnection) error() error {
	if heraConn.closed.Load() {
		if err := heraConn.canceled.Value(); err != nil {
			return err
		}
		return ErrInvalidConn
	}
	return nil
}

// finish is called when the query has succeeded.
func (heraConn *heraConnection) finish() {
	if !heraConn.watching || heraConn.finished == nil {
		return
	}
	select {
	case heraConn.finished <- struct{}{}:
		heraConn.watching = false
	case <-heraConn.closech:
	}
}

// Prepare returns a prepared statement, bound to this connection.
func (heraConn *heraConnection) Prepare(query string) (driver.Stmt, error) {
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, heraConn.id, "prepare SQL:", query)
	}
	return newStmt(heraConn, query), nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (heraConn *heraConnection) Close() error {
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, heraConn.id, "close driver connection")
	}
	heraConn.cleanup()
	return nil
}

//Start watcher for connection
func (heraConn *heraConnection) startWatcher() {
	watcher := make(chan context.Context, 1)
	heraConn.watcher = watcher
	finished := make(chan struct{})
	heraConn.finished = finished
	go func() {
		for {
			var ctx context.Context
			select {
			case ctx = <-watcher:
			case <-heraConn.closech:
				return
			}

			select {
			case <-ctx.Done():
				heraConn.cancel(ctx.Err())
			case <-finished:
			case <-heraConn.closech:
				return
			}
		}
	}()
}

// finish is called when the query has canceled.
func (heraConn *heraConnection) cancel(err error) {
	heraConn.canceled.Set(err)
	heraConn.cleanup()
}

// Begin starts and returns a new transaction.
func (heraConn *heraConnection) Begin() (driver.Tx, error) {
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, heraConn.id, "begin txn")
	}
	if heraConn.closed.Load() {
		logger.GetLogger().Log(logger.Alert, ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	return &tx{hera: heraConn}, nil
}

// internal function to execute commands
func (heraConn *heraConnection) exec(cmd int, payload []byte) error {
	if heraConn.closed.Load() {
		logger.GetLogger().Log(logger.Alert, ErrInvalidConn)
		return driver.ErrBadConn
	}
	return heraConn.execNs(netstring.NewNetstringFrom(cmd, payload))
}

// internal function to execute commands
func (heraConn *heraConnection) execNs(ns *netstring.Netstring) error {
	if heraConn.closed.Load() {
		logger.GetLogger().Log(logger.Alert, ErrInvalidConn)
		return driver.ErrBadConn
	}
	if logger.GetLogger().V(logger.Verbose) {
		payload := string(ns.Payload)
		if len(payload) > 1000 {
			payload = payload[:1000]
		}
		logger.GetLogger().Log(logger.Verbose, heraConn.id, "send command:", ns.Cmd, ", payload:", payload)
	}
	_, err := heraConn.conn.Write(ns.Serialized)
	return err
}

// returns the next message from the connection
func (heraConn *heraConnection) getResponse() (*netstring.Netstring, error) {
	if heraConn.closed.Load() {
		logger.GetLogger().Log(logger.Alert, ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	ns, err := heraConn.reader.ReadNext()
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, heraConn.id, "Failed to read response")
		}
		return nil, errors.New("Failed to read response")
	}
	if logger.GetLogger().V(logger.Verbose) {
		payload := string(ns.Payload)
		if len(payload) > 1000 {
			payload = payload[:1000]
		}
		//		logger.GetLogger().Log(logger.Verbose, c.id, "got response command:", ns.Cmd, ", payload:", payload)
	}
	return ns, nil
}

// implementing the extension HeraConn interface
func (heraConn *heraConnection) SetShardID(shard int) error {
	heraConn.exec(common.CmdSetShardID, []byte(fmt.Sprintf("%d", shard)))
	ns, err := heraConn.getResponse()
	if err != nil {
		return err
	}
	if ns.Cmd == common.RcError {
		return errors.New(string(ns.Payload))
	}
	if ns.Cmd != common.RcOK {
		return fmt.Errorf("Unknown error, cmd=%d, payload size=%d", ns.Cmd, len(ns.Payload))
	}
	return nil
}

// implementing the extension HeraConn interface
func (heraConn *heraConnection) ResetShardID() error {
	return heraConn.SetShardID(-1)
}

// implementing the extension HeraConn interface
func (heraConn *heraConnection) GetNumShards() (int, error) {
	heraConn.exec(common.CmdGetNumShards, nil)
	ns, err := heraConn.getResponse()
	if err != nil {
		return -1, err
	}
	if ns.Cmd == common.RcError {
		return -1, errors.New(string(ns.Payload))
	}
	if ns.Cmd != common.RcOK {
		return -1, fmt.Errorf("Unknown error, cmd=%d, payload size=%d", ns.Cmd, len(ns.Payload))
	}
	var num int
	fmt.Sscanf(string(ns.Payload), "%d", &num)
	return num, nil
}

// implementing the extension HeraConn interface
func (heraConn *heraConnection) SetShardKeyPayload(payload string) {
	heraConn.shardKeyPayload = []byte(payload)
}

// implementing the extension HeraConn interface
func (heraConn *heraConnection) ResetShardKeyPayload() {
	heraConn.SetShardKeyPayload("")
}

// implementing the extension HeraConn interface
func (heraConn *heraConnection) SetCalCorrID(corrID string) {
	heraConn.corrID = netstring.NewNetstringFrom(common.CmdClientCalCorrelationID, []byte(fmt.Sprintf("CorrId=%s", corrID)))
}

// SetClientInfo actually sends it over to Hera server
func (heraConn *heraConnection) SetClientInfo(poolName string, host string) error {
	if len(poolName) <= 0 && len(host) <= 0 {
		return nil
	}
	if heraConn.closed.Load() {
		logger.GetLogger().Log(logger.Alert, ErrInvalidConn)
		return driver.ErrBadConn
	}
	pid := os.Getpid()
	data := fmt.Sprintf("PID: %d, HOST: %s, Poolname: %s, Command: SetClientInfo,", pid, host, poolName)
	heraConn.clientinfo = netstring.NewNetstringFrom(common.CmdClientInfo, []byte(string(data)))
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "SetClientInfo", heraConn.clientinfo.Serialized)
	}

	_, err := heraConn.conn.Write(heraConn.clientinfo.Serialized)
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Failed to send client info")
		}
		return errors.New("Failed custom auth, failed to send client info")
	}
	ns, err := heraConn.reader.ReadNext()
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Failed to read server info")
		}
		return errors.New("Failed to read server info")
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Server info:", string(ns.Payload))
	}
	return nil
}

func (heraConn *heraConnection) SetClientInfoWithPoolStack(poolName string, host string, poolStack string) error {
	if len(poolName) <= 0 && len(host) <= 0 && len(poolStack) <= 0 {
		return nil
	}
	if heraConn.closed.Load() {
		logger.GetLogger().Log(logger.Alert, ErrInvalidConn)
		return driver.ErrBadConn
	}
	pid := os.Getpid()
	data := fmt.Sprintf("PID: %d, HOST: %s, Poolname: %s, PoolStack: %s, Command: SetClientInfo,", pid, host, poolName, poolStack)
	heraConn.clientinfo = netstring.NewNetstringFrom(common.CmdClientInfo, []byte(string(data)))
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "SetClientInfo", heraConn.clientinfo.Serialized)
	}

	_, err := heraConn.conn.Write(heraConn.clientinfo.Serialized)
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Failed to send client info")
		}
		return errors.New("Failed custom auth, failed to send client info")
	}
	ns, err := heraConn.reader.ReadNext()
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Failed to read server info")
		}
		return errors.New("Failed to read server info")
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Server info:", string(ns.Payload))
	}
	return nil
}
