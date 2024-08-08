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
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

var corrIDUnsetCmd = netstring.NewNetstringFrom(common.CmdClientCalCorrelationID, []byte("CorrId=NotSet"))

type heraConnectionInterface interface {
	Prepare(query string) (driver.Stmt, error)
	Close() error
	Begin() (driver.Tx, error)
	exec(cmd int, payload []byte) error
	execNs(ns *netstring.Netstring) error
	getResponse() (*netstring.Netstring, error)
	SetShardID(shard int) error
	ResetShardID() error
	GetNumShards() (int, error)
	SetShardKeyPayload(payload string)
	ResetShardKeyPayload()
	SetCalCorrID(corrID string)
	SetClientInfo(poolName string, host string) error
	SetClientInfoWithPoolStack(poolName string, host string, poolStack string) error
	getID() string
	getCorrID() *netstring.Netstring
	getShardKeyPayload() []byte
	setCorrID(*netstring.Netstring)
}

type heraConnection struct {
	id     string // used for logging
	conn   net.Conn
	reader *netstring.Reader
	// for the sharding extension
	shardKeyPayload []byte
	// correlation id
	corrID     *netstring.Netstring
	clientinfo *netstring.Netstring
}

// NewHeraConnection creates a structure implementing a driver.Con interface
func NewHeraConnection(conn net.Conn) driver.Conn {
	hera := &heraConnection{conn: conn, id: conn.RemoteAddr().String(), reader: netstring.NewNetstringReader(conn), corrID: corrIDUnsetCmd}
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, hera.id, "create driver connection")
	}
	return hera
}

// Prepare returns a prepared statement, bound to this connection.
func (c *heraConnection) Prepare(query string) (driver.Stmt, error) {
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, c.id, "prepare SQL:", query)
	}
	return newStmt(c, query), nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *heraConnection) Close() error {
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, c.id, "close driver connection")
	}
	c.conn.Close()
	return nil
}

// Begin starts and returns a new transaction.
func (c *heraConnection) Begin() (driver.Tx, error) {
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, c.id, "begin txn")
	}
	return &tx{hera: c}, nil
}

// internal function to execute commands
func (c *heraConnection) exec(cmd int, payload []byte) error {
	return c.execNs(netstring.NewNetstringFrom(cmd, payload))
}

// internal function to execute commands
func (c *heraConnection) execNs(ns *netstring.Netstring) error {
	if logger.GetLogger().V(logger.Verbose) {
		payload := string(ns.Payload)
		if len(payload) > 1000 {
			payload = payload[:1000]
		}
		logger.GetLogger().Log(logger.Verbose, c.id, "send command:", ns.Cmd, ", payload:", payload)
	}
	_, err := c.conn.Write(ns.Serialized)
	return err
}

// returns the next message from the connection
func (c *heraConnection) getResponse() (*netstring.Netstring, error) {
	ns, err := c.reader.ReadNext()
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, c.id, "Failed to read response")
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
func (c *heraConnection) SetShardID(shard int) error {
	c.exec(common.CmdSetShardID, []byte(fmt.Sprintf("%d", shard)))
	ns, err := c.getResponse()
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
func (c *heraConnection) ResetShardID() error {
	return c.SetShardID(-1)
}

// implementing the extension HeraConn interface
func (c *heraConnection) GetNumShards() (int, error) {
	c.exec(common.CmdGetNumShards, nil)
	ns, err := c.getResponse()
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
func (c *heraConnection) SetShardKeyPayload(payload string) {
	c.shardKeyPayload = []byte(payload)
}

// implementing the extension HeraConn interface
func (c *heraConnection) ResetShardKeyPayload() {
	c.SetShardKeyPayload("")
}

// implementing the extension HeraConn interface
func (c *heraConnection) SetCalCorrID(corrID string) {
	c.corrID = netstring.NewNetstringFrom(common.CmdClientCalCorrelationID, []byte(fmt.Sprintf("CorrId=%s", corrID)))
}

// SetClientInfo actually sends it over to Hera server
func (c *heraConnection) SetClientInfo(poolName string, host string) error {
	if len(poolName) <= 0 && len(host) <= 0 {
		return nil
	}

	pid := os.Getpid()
	data := fmt.Sprintf("PID: %d, HOST: %s, Poolname: %s, Command: SetClientInfo,", pid, host, poolName)
	c.clientinfo = netstring.NewNetstringFrom(common.CmdClientInfo, []byte(string(data)))
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "SetClientInfo", c.clientinfo.Serialized)
	}

	_, err := c.conn.Write(c.clientinfo.Serialized)
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Failed to send client info")
		}
		return errors.New("Failed custom auth, failed to send client info")
	}
	ns, err := c.reader.ReadNext()
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

func (c *heraConnection) SetClientInfoWithPoolStack(poolName string, host string, poolStack string) error {
	if len(poolName) <= 0 && len(host) <= 0 && len(poolStack) <= 0 {
		return nil
	}

	pid := os.Getpid()
	data := fmt.Sprintf("PID: %d, HOST: %s, Poolname: %s, PoolStack: %s, Command: SetClientInfo,", pid, host, poolName, poolStack)
	c.clientinfo = netstring.NewNetstringFrom(common.CmdClientInfo, []byte(string(data)))
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "SetClientInfo", c.clientinfo.Serialized)
	}

	_, err := c.conn.Write(c.clientinfo.Serialized)
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Failed to send client info")
		}
		return errors.New("Failed custom auth, failed to send client info")
	}
	ns, err := c.reader.ReadNext()
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

func (c *heraConnection) getID() string {
	return c.id
}

func (c *heraConnection) getCorrID() *netstring.Netstring {
	return c.corrID
}

func (c *heraConnection) getShardKeyPayload() []byte {
	return c.shardKeyPayload
}

func (c *heraConnection) setCorrID(newCorrID *netstring.Netstring) {
	c.corrID = newCorrID
}
