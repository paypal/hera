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

package cal

import (
	"encoding/binary"
	"errors"
	"net"
	"syscall"
	"time"
	//"log"
)

const (
	//
	// max size of msg body.
	//
	calMessageBodyMaxSize = 4096
	handlerCtrlMsgNewRoot = "calHandlerCtrlmsgNewRoot"
)

type clientSocketStatus int

const (
	statusConnInProgress = iota
	statusConnected
	statusNotConnected
)

/**
 * cal socket handler is default. parent interface stays with default.
 */
type handler interface {
	init(_config *calConfig, _msgchann <-chan string) error
	run()
}

/**
 * gateway to caldaemon
 */
type socketHandler struct {

	//
	// readonly channel for arriving calmessages
	//
	mMsgReadChann <-chan string
	//
	// a message placeholder repopulated during each request
	//
	mMsg *clientMessage
	//
	// ring buffer to hold outgoing bytes (not thread safe)
	//
	mRingBuf *ringBuffer
	//
	//
	//
	mSocket *clientSocket
	//
	// a serialized calclientmsg for label. first msg to caldaemon after each reconnect
	//
	mLabel     []byte
	mLabelSize int
	//
	// indication of message loss to let next roottxn to reconnect
	//
	mRootTxnLossyFlag bool
	mConfig           *calConfig
}

/**
 * message between calclient and caldaemon
 */
type clientMessage struct {
	//
	// three header fields (stored as is, but going out in network byte order big endian)
	//
	mThreadID    uint32
	mConnectTime uint32
	mMsgLen      uint32

	mBody []byte
	mZero []byte // go has no memset. use this slice to clear 0 in one shot
}

/**
 * connection to caldaemon
 */
type clientSocket struct {
	mLastConnAttemptTime int
	mConnTime            int
	mStatus              clientSocketStatus
	mCaldAddress         string
	mConn                net.Conn
}

/********************************************************
 ********************************************************
 *               clientMessage
 ********************************************************
 */
func (sh *clientMessage) init(_label []byte, _enableTG bool) {
	sh.mBody = make([]byte, calMessageBodyMaxSize, calMessageBodyMaxSize)
	sh.mZero = make([]byte, calMessageBodyMaxSize, calMessageBodyMaxSize)
	sh.resetMessageHeader()

	//
	// add thread(group)id as the last four bytes of the msg. endianess really does not
	// matter since reading forward or backward produces the same uniqeness.
	//
	// setting threadid to 0 in callabelmsg force all calmsgs from the same process in
	// the same swimminglane in callogview, even when those calmsgs carry different
	// threadids in their calmsgheaders.
	//
	// occworker has a c++ calclient sending a callabelmsg with threadid==0. if their
	// msg arrives before proxy's callabelmsg having a threadid=0x1111, we cannot see
	// any proxy calmsg getting displayed (apparently, caldaemon rejects calmsgs having
	// a different threadid if the pool is started with threadid=0. in this case, we
	// need to set callabelmsg in proxy to match threadid==0. but, that would put
	// all calmsgs from proxy under the same swimminglane in callogview
	//
	// alternatively, we could make sure proxy fires off callabelmsg first. that way
	// calmsgs with different threadid in proxy will be spread into different
	// swimminglanes. at the same time, calmsg from different occworker processes would
	// also get their own separated lanes.
	//
	if _enableTG {
		_label = append(_label, 0, 0, 0, 1)
	} else {
		_label = append(_label, 0, 0, 0, 0)
	}
	sh.constructCalMessage(_label)
}

func (sh *clientMessage) resetMessageHeader() {
	//
	// @TODO messages having different thread id go to different vertical lanes in calview
	//
	sh.mThreadID = 0
	sh.mConnectTime = uint32(time.Now().Unix())
}

func (sh *clientMessage) toBytes() []byte {
	serialized := make([]byte, sh.getLength())
	binary.BigEndian.PutUint32(serialized[0:4], sh.mThreadID)
	binary.BigEndian.PutUint32(serialized[4:8], sh.mConnectTime)
	binary.BigEndian.PutUint32(serialized[8:12], sh.mMsgLen)
	copy(serialized[12:], sh.mBody[:sh.mMsgLen])
	return serialized
}

func (sh *clientMessage) getLength() uint32 {
	return sh.mMsgLen + 12 // 12 bytes header
}

func (sh *clientMessage) constructCalMessage(_buffer []byte) {
	/* *  This gets called for every single CAL message, so it
	 *  needs a little optimization.  There may not be a
	 *  trailing NUL on the end of the string, but it
	 *  wouldn't get copied anyway.  We get the string
	 *  length, ensure that it ends with at least one \r\n
	 *  (although it could end up ending with two), and
	 *  then use memcpy (fast!) to ship it out.
	 */
	copy(sh.mBody, sh.mZero)

	//
	// last four bytes is thread id
	//
	size := len(_buffer)
	sh.mThreadID = binary.LittleEndian.Uint32(_buffer[size-4:])
	_buffer = _buffer[0 : size-4]

	if len(_buffer) > calMessageBodyMaxSize {
		_buffer = _buffer[:calMessageBodyMaxSize]
	}
	copy(sh.mBody, _buffer)

	sh.mBody[calMessageBodyMaxSize-2] = '\r'
	sh.mBody[calMessageBodyMaxSize-1] = '\n'

	sh.mMsgLen = uint32(len(_buffer))
	//log.Println("constructcalmsg ", _buffer, sh.mMsgLen)
}

/********************************************************
 ********************************************************
 *               socketHandler
 ********************************************************
 *
 * Init is called by CalClient in a synchronized block
 */
func (sh *socketHandler) init(_config *calConfig, _msgchann <-chan string) error {
	if _config == nil {
		return errors.New("passing nil calconfig to calsockethandler")
	}
	sh.mConfig = _config

	sh.mRingBuf = &ringBuffer{}
	sh.mRingBuf.Init(_config.getRingBufferSize())

	sh.mMsg = &clientMessage{}
	sh.mMsg.init([]byte(_config.getLabel()), _config.enableTG)
	//
	// keep a serialized label message for each reconnection
	//
	sh.mLabelSize = int(sh.mMsg.getLength())
	sh.mLabel = sh.mMsg.toBytes()

	sh.mSocket = &clientSocket{}
	sh.mSocket.Init(_config.host, _config.port, int(_config.connTimeSec))
	sh.mSocket.establishCaldConnection()

	sh.mMsgReadChann = _msgchann

	//
	// put label on the ringbuf and let the next client write to flush it to caldaemon
	//
	sh.mRingBuf.WriteData(sh.mLabel, sh.mLabelSize)
	sh.mRootTxnLossyFlag = false

	return nil
}

/**
 * ringbuffer is not made thread safe. instead, we use a channel to ensure single
 * access to ringbuffer and clientsocket. calling other functions like addDataToRingBuffer
 * directly would cause race condition having undefined behavior(s)
 */
func (sh *socketHandler) run() {
	for {
		calmsg, ok := <-sh.mMsgReadChann
		if !ok {
			//
			// @TODO calclient closed channel, clean up memory and exit
			//
			return
		}

		switch calmsg {
		case handlerCtrlMsgNewRoot:
			sh.handleNewRootTransaction()
		default:
			sh.completeConnectionToCaldaemon()
			sh.addDataToRingBuffer(calmsg)
			sh.flush()
		}
	}
}

/**
 * none of these functions should be called from outside socketHandler
 */
func (sh *socketHandler) completeConnectionToCaldaemon() {
	if sh.mSocket.mStatus == statusNotConnected {
		sh.handleDisconnect()
	} else if sh.mSocket.mStatus == statusConnInProgress {
		sh.handleConnectionInProgress()
	}
}

func (sh *socketHandler) addDataToRingBuffer(_msg string) {
	sh.mMsg.constructCalMessage([]byte(_msg))

	if !sh.mRingBuf.WriteData(sh.mMsg.toBytes(), int(sh.mMsg.getLength())) {
		sh.mRootTxnLossyFlag = true
	}
}

func (sh *socketHandler) flush() {
	if sh.mSocket.mStatus != statusConnected {
		return
	}
	var size = sh.mRingBuf.mUsed
	buffer := make([]byte, size)
	sh.mRingBuf.CopyData(buffer, size)

	var dataSent = sh.mSocket.SendData(buffer, size)
	if dataSent < 0 {
		//m_logger->write_trace_message (CAL_LOG_WARNING, errno, "Closing connection: failed to send %d bytes. %d", size, errno);
		sh.closeConnection()
		sh.mRootTxnLossyFlag = true
	} else {
		sh.mRingBuf.RemoveData(dataSent)
	}
}

func (sh *socketHandler) handleDisconnect() {
	if sh.mSocket.establishCaldConnection() {
		sh.mMsg.resetMessageHeader()
	}
}

func (sh *socketHandler) handleConnectionInProgress() {
	/* This would fail in the following cases
	1) caldaemon down
	2) time out during connection progress
	In both of the cases it make sense to close connection */

	if !sh.mSocket.connectToCaldaemon() {
		sh.closeConnection()
	}
}

func (sh *socketHandler) closeConnection() {
	sh.mSocket.closeSocket()
	sh.mRingBuf.Clear()

	//
	// Always fill the RB with the label, after discarding its data
	//
	sh.mRingBuf.WriteData(sh.mLabel, sh.mLabelSize)
}

func (sh *socketHandler) handleNewRootTransaction() {
	if sh.mRootTxnLossyFlag {
		sh.flush()
		sh.closeConnection()
		sh.mRootTxnLossyFlag = false
		sh.completeConnectionToCaldaemon()
	}
}

/********************************************************
 ********************************************************
 *               clientSocket
 ********************************************************
 */
func (sh *clientSocket) Init(_host string, _port string, _connTime int) {
	sh.mLastConnAttemptTime = 0
	sh.mStatus = statusNotConnected
	sh.mConnTime = _connTime
	sh.mCaldAddress = _host + ":" + _port
}

func (sh *clientSocket) hasConnTimeoutExpired() bool {
	if int(time.Now().Unix()) >= (sh.mLastConnAttemptTime + sh.mConnTime) {
		return true
	}
	return false
}

func (sh *clientSocket) establishCaldConnection() bool {
	if !sh.hasConnTimeoutExpired() {
		return false
	}

	sh.mLastConnAttemptTime = int(time.Now().Unix())
	if !sh.connectToCaldaemon() {
		sh.closeSocket()
		return false
	}
	return true
}

func (sh *clientSocket) connectToCaldaemon() bool {
	//
	// connection timeout at 500 ms going after caldaemon on the same box.
	//
	var err error
	sh.mConn, err = net.DialTimeout("tcp", sh.mCaldAddress,
		time.Duration(500*time.Millisecond))
	if err != nil {
		return sh.checkSocketConnectErrors(err)
	}
	sh.mStatus = statusConnected
	return true
}

func (sh *clientSocket) checkSocketConnectErrors(_err error) bool {
	var rt = false
	//
	// @TODO test these error conditions on stage
	//
	switch t := _err.(type) {
	case syscall.Errno:
		switch t {
		case syscall.EINPROGRESS:
			sh.mStatus = statusConnInProgress
			rt = true
		case syscall.EISCONN:
			sh.mStatus = statusConnected
			rt = true
		case syscall.EALREADY:
			if !sh.hasConnTimeoutExpired() {
				sh.mStatus = statusConnInProgress
				rt = true
			}
		default:
		}
		//
		// all other type of errors return false. none of these are tested.
		//
		//case net.Error:
		//	if t.Timeout()
		//case *net.OpError:
		//	if opError.Op == "dial"/unknowhost
		//	if opError.Op == "read"/connrefused
	}
	return rt
}

func (sh *clientSocket) SendData(_msg []byte, _size int) int {
	if sh.mStatus != statusConnected {
		return 0
	}

	sh.mConn.SetWriteDeadline(time.Now().Add(time.Duration(500 * time.Millisecond)))
	sent, err := sh.mConn.Write(_msg)
	//log.Println("senddata ", _msg, sent, string(_msg[12:]))

	if (sent <= 0) && (err != nil) {
		switch t := err.(type) {
		case syscall.Errno:
			switch t {
			case syscall.EAGAIN:
				return 0
			default:
				return -1
			}
		default:
			return -1
		}
	}
	return sent
}

func (sh *clientSocket) closeSocket() {
	if sh.mStatus != statusNotConnected {
		if sh.mConn != nil {
			sh.mConn.Close()
		}
		sh.mStatus = statusNotConnected
	}
}
