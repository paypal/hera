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
	"context"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
	"io"
	"net"
	"strconv"
)

// Spawns a goroutine which blocks waiting for a message on conn. When a message is received it writes
// to the channel and exit. It basically wrapps the net.Conn in a channel
func wrapNewNetstring(conn net.Conn) <-chan *netstring.Netstring {
	ch := make(chan *netstring.Netstring, 1)
	go func() {
		ns, err := netstring.NewNetstring(conn)
		if err != nil {
			if err == io.EOF {
				if logger.GetLogger().V(logger.Debug) {
					logger.GetLogger().Log(logger.Debug, conn.RemoteAddr(), ": Connection closed (eof) ")
				}
			} else {
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, conn.RemoteAddr(), ": Connection handler read error", err.Error())
				}
			}
			ch <- nil
		} else {
			if ns.Serialized != nil && len(ns.Serialized) > 64*1024 {
				evt := cal.NewCalEvent("OCCMUX", "large_payload_in", cal.TransOK, "")
				evt.AddDataInt("len", int64(len(ns.Serialized)))
				evt.Completed()
			}
			ch <- ns
		}
		close(ch)
	}()
	return ch
}

// HandleConnection runs as a go routine handling a client connection.
// It creates the coordinator go-routine and the one way channel to communicate
// with the coordinator. Then it sits in a loop for the life of the connection
// reading data from the connection. Once a complete netstring is read, the
// netstring object (which can contain nested sub-netstrings) is passed on
// to the coordinator for processing
func HandleConnection(conn net.Conn) {
	//
	// proxy just took a new connection. increment the idel connection count.
	//
	GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: 0, wType: wtypeRW, instID: 0, oldCState: Close, newCState: Idle})

	clientchannel := make(chan *netstring.Netstring, 1)
	// closing of clientchannel will notify the coordinator to exit
	defer func() {
		close(clientchannel)
		GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: 0, wType: wtypeRW, instID: 0, oldCState: Idle, newCState: Close})
	}()

	//TODO: create a context with timeout
	ctx, cancel := context.WithCancel(context.Background())
	crd := NewCoordinator(ctx, clientchannel, conn)
	go crd.Run()

	//
	// clientchannel is a mechanism for request handler to pass over the client netstring
	// this loop blocks on the client connection.
	// - when receiving a netstring, it writes the netstring to the channel
	// - when receiving a connection error, it closes the clientchannel which is a
	//   detectable event in coordinator such that coordinator can clean up and exit too
	//
	addr := conn.RemoteAddr()
	for {
		var ns *netstring.Netstring
		select {
		case ns = <-wrapNewNetstring(conn):
		case timeout := <-crd.Done():
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "Connection handler idle timeout", addr)
			}
			evt := cal.NewCalEvent("OCCMUX", "idle_timeout_"+strconv.Itoa(int(timeout)), cal.TransOK, "")
			evt.Completed()

			conn.Close() // this forces netstring.NewNetstring() conn.Read to exit with err=read tcp 127.0.0.1:8081->127.0.0.1:57968: use of closed network connection
			ns = nil
		}
		if ns == nil {
			break
		}
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, addr, ": Connection handler read <<<", DebugString(ns.Serialized))
		}
		//
		// coordinator is ready to go, send over the new netstring.
		// this could block when client close the connection abruptly. e.g. when coordinator write
		// is the first one to encounter the closed connection, coordinator exits. meanwhile there
		// could still be a last pending message from client that is blocked since there is not one
		// listening to clientchannel anymore. to avoid blocking, give clientchannel a buffer.
		//
		clientchannel <- ns
	}
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "======== Connection handler exits", addr)
	}
	conn.Close()
	conn = nil
	cancel()
}
