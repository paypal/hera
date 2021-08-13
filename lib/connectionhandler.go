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
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/encoding"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/encoding/postgrespackets"
	"github.com/paypal/hera/utility/logger"
)

// Spawns a goroutine which blocks waiting for a message on conn. When a message is received it writes
// to the channel and exit. It basically wrapps the net.Conn in a channel

// func wrapNewNetstring(conn net.Conn) <-chan *netstring.Netstring {
func wrapNewNetstring(conn net.Conn, isPostgreSQL bool) <-chan *encoding.Packet {
	// ch := make(chan *netstring.Netstring, 1)
	ch := make(chan *encoding.Packet, 1)
	go func() {
		var ns *encoding.Packet
		var err error

		if isPostgreSQL {
			logger.GetLogger().Log(logger.Info, "getting to psql")
			ns, err = postgrespackets.NewInitPSQLPacket(conn)

		} else {
			ns, err = netstring.NewInitNetstring(conn)
			// ns, err = netstring.NewNetstring(conn)
		}
		// ns, err := netstring.NewNetstring(conn)
		// ns, err = netstring.NewInitNetstring(conn)

		logger.GetLogger().Log(logger.Info, "what is ns", ns)

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
				evt := cal.NewCalEvent(EvtTypeMux, "large_payload_in", cal.TransOK, "")
				evt.AddDataInt("len", int64(len(ns.Serialized)))
				evt.Completed()
			}
			ch <- ns
		}
		close(ch)
	}()
	return ch
}

// func sendHandshake(conn net.Conn) {
// 	// logger.GetLogger().Log(logger.Info, "log 1 in send handshake")

// 	// writeBuf := make([]byte, postgrespackets.MAX_PACKET_SIZE)
// 	writeBuf := make([]byte, 150)
// 	pos := 0

// 	user := os.Getenv("username")
// 	db := os.Getenv("dbname")
// 	// Startup message - `user\0${user}\0database\0${db}\0\0`;
// 	// https://medium.com/@asayechemeda/communicating-with-postgresql-database-using-tcp-sockets-dcb4c2cd49c5
// 	// https://www.postgresql.org/docs/9.5/protocol-message-formats.html
// 	str := `user ` + user + ` database ` + db

// 	totalLength := 8 + len(str) + 1

// 	// logger.GetLogger().Log(logger.Info, "log 2 in send handshake")

// 	logger.GetLogger().Log(logger.Info, byte(totalLength))

// 	// Int32 - length of message contents in bytes, including self
// 	postgrespackets.WriteFixedLenInt(writeBuf, postgrespackets.INT4, totalLength, &pos)
// 	// logger.GetLogger().Log(logger.Info, "what is the writeBuf part 1", writeBuf)

// 	// Int32(196608) - protocol version number
// 	postgrespackets.WriteFixedLenInt(writeBuf, postgrespackets.INT4, 196608, &pos)
// 	// logger.GetLogger().Log(logger.Info, "what is the writeBuf part 2", writeBuf)

// 	// String - parameter name
// 	postgrespackets.WriteString(writeBuf, str, postgrespackets.FIXEDSTR, &pos, len(str))
// 	// logger.GetLogger().Log(logger.Info, "what is the writeBuf part 3", writeBuf)

// 	// logger.GetLogger().Log(logger.Info, "log 3 in send handshake")

// 	postgrespackets.WriteFixedLenInt(writeBuf, postgrespackets.INT1, 0, &pos)

// 	handshake := postgrespackets.NewPSQLPacketFrom(0, writeBuf[0:pos])
// 	logger.GetLogger().Log(logger.Info, "log 4 in send handshake", handshake)
// 	logger.GetLogger().Log(logger.Info, "log 4.2 in send handshake", handshake.Serialized[1:])

// 	_, err := conn.Write(handshake.Serialized[1:])
// 	if err != nil {
// 		logger.GetLogger().Log(logger.Verbose, ": Failed to write handshake to PostgreSQL client >>>") //, DebugString(handshake.Serialized))
// 	}
// 	logger.GetLogger().Log(logger.Info, ": Writing handshake to PostgreSQL client >>>", handshake.Serialized[1:])

// 	logger.GetLogger().Log(logger.Info, "handshake sent")

// }

func readHandshakeResponse(conn net.Conn) {

	reader := bufio.NewReader(conn)

	length := make([]byte, 4)
	_, err := io.ReadFull(reader, length)
	if err != nil {
		logger.GetLogger().Log(logger.Info, "read byte err", err)
	}

	_, err = io.ReadFull(reader, make([]byte, 4))
	if err != nil {
		logger.GetLogger().Log(logger.Info, "commandID err", err)
	}
	// else {
	// 	logger.GetLogger().Log(logger.Info, "what is commandID", commandID)
	// }

	// Read in the payload.
	data := binary.BigEndian.Uint32(length)
	packet := make([]byte, data-8)
	n, err := io.ReadFull(reader, packet)
	if err != nil {
		logger.GetLogger().Log(logger.Info, "packet reading err", err)
	}

	logger.GetLogger().Log(logger.Info, "what is packet", packet)
	logger.GetLogger().Log(logger.Info, "what is n", n)

	// Check that the length of the payload is correct.
	// if n != int(length) {
	// 	logger.GetLogger().Log(logger.Verbose, fmt.Sprintf("Expected %d bytes, read %d", length, n))
	// } else if err != nil {
	// 	logger.GetLogger().Log(logger.Verbose, err.Error())
	// }

	pos := 0
	postgrespackets.ReadString(packet, postgrespackets.EOFSTR, &pos, n)

	logger.GetLogger().Log(logger.Info, "read startup message")

}

func writeAuthOK(conn net.Conn) {
	writeBuf := make([]byte, 9)
	pos := 0
	str := "R"

	postgrespackets.WriteString(writeBuf, str, postgrespackets.FIXEDSTR, &pos, len(str))

	postgrespackets.WriteFixedLenInt(writeBuf, postgrespackets.INT4, 8, &pos)

	postgrespackets.WriteFixedLenInt(writeBuf, postgrespackets.INT4, 0, &pos)

	handshake := postgrespackets.NewPSQLPacketFrom(0, writeBuf[0:pos])
	_, err := conn.Write(handshake.Serialized[1:])
	if err != nil {
		logger.GetLogger().Log(logger.Verbose, ": Failed to write Auth OK to PostgreSQL client >>>")
	}
	logger.GetLogger().Log(logger.Info, "authentication OK sent")

}

func zResponse(conn net.Conn) {
	writeBuf := make([]byte, 150)
	pos := 0
	messageType := "Z"
	status := "I"

	postgrespackets.WriteString(writeBuf, messageType, postgrespackets.FIXEDSTR, &pos, len(messageType))

	postgrespackets.WriteFixedLenInt(writeBuf, postgrespackets.INT4, 5, &pos)

	postgrespackets.WriteString(writeBuf, status, postgrespackets.FIXEDSTR, &pos, len(status))

	handshake := postgrespackets.NewPSQLPacketFrom(0, writeBuf[0:pos])
	_, err := conn.Write(handshake.Serialized[1:])
	if err != nil {
		logger.GetLogger().Log(logger.Verbose, ": Failed to write Z query cycle to PostgreSQL client >>>")
	}
	logger.GetLogger().Log(logger.Info, "Z query cycle sent")

}

// HandleConnection runs as a go routine handling a client connection.
// It creates the coordinator go-routine and the one way channel to communicate
// with the coordinator. Then it sits in a loop for the life of the connection
// reading data from the connection. Once a complete netstring is read, the
// netstring object (which can contain nested sub-netstrings) is passed on
// to the coordinator for processing
func HandleConnection(conn net.Conn) {
	logger.GetLogger().Log(logger.Info, "start handle connection")
	//
	// proxy just took a new connection. increment the idel connection count.
	//
	GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: 0, wType: wtypeRW, instID: 0, oldCState: Close, newCState: Idle})

	// clientchannel := make(chan *netstring.Netstring, 1)
	clientchannel := make(chan *encoding.Packet, 1)

	// closing of clientchannel will notify the coordinator to exit
	defer func() {
		close(clientchannel)
		GetStateLog().PublishStateEvent(StateEvent{eType: ConnStateEvt, shardID: 0, wType: wtypeRW, instID: 0, oldCState: Idle, newCState: Close})
	}()

	//TODO: create a context with timeout
	ctx, cancel := context.WithCancel(context.Background())

	// Right now this is set to true. Set to false if you expect non-PostgreSQL client.
	IsPostgreSQL := true

	if IsPostgreSQL {
		// logger.GetLogger().Log(logger.Info, "Sending handshake")
		// sendHandshake(conn)

		logger.GetLogger().Log(logger.Info, "Reading handshake response")
		readHandshakeResponse(conn)

		logger.GetLogger().Log(logger.Info, "write auth response")
		writeAuthOK(conn)

		logger.GetLogger().Log(logger.Info, "Z response")
		zResponse(conn)
	}

	logger.GetLogger().Log(logger.Info, "Created coordinator in connection handler")

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
		// var ns *netstring.Netstring
		var ns *encoding.Packet
		logger.GetLogger().Log(logger.Info, "connection loop")

		select {
		// case ns = <-wrapNewNetstring(conn):
		case ns = <-wrapNewNetstring(conn, true):
		case timeout := <-crd.Done():
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "Connection handler idle timeout", addr)
			}
			evt := cal.NewCalEvent(EvtTypeMux, "idle_timeout_"+strconv.Itoa(int(timeout)), cal.TransOK, "")
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
