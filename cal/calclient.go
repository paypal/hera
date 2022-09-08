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
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/paypal/hera/config"
)

const (
	bfMsgChannelSize = 300
)

// Client keeps the state of the client
type Client struct {
	mCalConfig *calConfig
	//
	// handler runs a its own goroutine and receives calmessages from mMsgChann
	//
	mCalHandler handler // interface
	mMsgChann   chan string
	//
	// key of the map is the ThreadId passed to each calmessage by the caller.
	// each group behind a key has its own caltxn chain with pending msgbuf, flag etc.
	// since golang does not recommend extracting goid, we need to generate a unique id
	//  - either with a global counter
	//  - or the address of the incomming connection pointer
	//  - or some other way we'd like the messages to be grouped
	//
	mPendingMsgBuffer map[string]*[]string
	mPending          map[string]bool
	mCurrentCalTxn    map[string]*calTransaction
	//
	// a root txn can set a pending flag to hold all messages until the end of root txn
	//
	mRootCalTxn map[string]*calTransaction
	//
	// correlation id. one per threadid
	//
	mCorrelationID map[string]string
	//
	// legacy pool stack. one poolstack per running process shared by all threads.
	//
	mParentStack   map[string]string
	mCurrentOpName map[string]string
	//
	// c++ attribute, not used now
	//
	//mSessionId			map[string]string
	//mLogId				map[string]string
	//
	// flag (in c++) to avoid multiple init of calclient. not sure if still needs it
	//
	mAlreadyInit bool
}

/**
 * singleton
 */
var sCalClientInstance *Client
var once sync.Once

//var sPid int
/**
 * the map holding information for each thread groups is not threadsafe.
 * modification of pendingmessage map happens in calactivity.
 * go mutex is not reentrant
 */
var gMapMtx *sync.Mutex

// GetCalClientInstance returns the singleton instance. The very first time is called it creates and initialize the structure
func GetCalClientInstance() *Client {
	once.Do(func() {
		gMapMtx = &sync.Mutex{}
		sCalClientInstance = &Client{mAlreadyInit: false}
		cfg, err := config.NewTxtConfig("cal_client.txt")
		if err != nil {
			sCalClientInstance = nil
			return
		}
		vcfg, err := config.NewTxtConfig("version.txt")
		if err != nil {
			vcfg = nil
		}
		err = sCalClientInstance.init(cfg, vcfg)
		if err != nil {
			sCalClientInstance = nil
		}
	})
	return sCalClientInstance
}

/**
 * called from a synchronized block in GetCalClientInstance.
 *  - should not be called multiple times
 *  - should not be exposed outside Client
 */
func (c *Client) init(cfg config.Config, vcfg config.Config) error {
	if c.mAlreadyInit {
		//
		// @TODO revisit this c++ pid stuff under multithreading environment
		//
		// if spawned into another process having a different pid, clear msgbuffer
		// we just reset size of slice to 0 so the old string should get gc after
		// its slot is given to a different string. try to avoid allocating a new slice
		//
		/*
			if sPid != os.Getpid()	{
				c.mPendingMsgBuffer = c.mPendingMsgBuffer[0:0]
			} else	{
				return nil
			}*/
		return nil
	}
	//sPid = os.Getpid()

	c.mCalConfig = &calConfig{}
	err := c.mCalConfig.initialize(cfg, vcfg, "")
	if err != nil {
		return err
	}

	c.mPendingMsgBuffer = make(map[string]*[]string)
	c.mPending = make(map[string]bool)
	c.mCurrentCalTxn = make(map[string]*calTransaction)
	c.mRootCalTxn = make(map[string]*calTransaction)
	c.mCorrelationID = make(map[string]string)
	c.mParentStack = make(map[string]string)
	c.mCurrentOpName = make(map[string]string)
	//
	// buffered channel to allow 300 backlogged messages. after that, bounce new request.
	//
	c.mMsgChann = make(chan string, bfMsgChannelSize)

	if strings.EqualFold("file", c.mCalConfig.getHandlerType()) {
		//
		// @TODO not working
		//
		c.mCalHandler = &fileHandler{}
	} else {
		c.mCalHandler = &socketHandler{}
	}
	c.mCalHandler.init(c.mCalConfig, c.mMsgChann)
	go c.mCalHandler.run()

	c.mAlreadyInit = true

	return nil
}

// WriteData is the single place for calmessage classes to send a request to caldaemon
func (c *Client) WriteData(_msg string) error {
	select {
	case c.mMsgChann <- _msg:
	default:
		return errors.New("write buffer full. no message written")
	}
	return nil
}

// getConfigInstance gets the config
func (c *Client) getConfigInstance() *calConfig {
	return c.mCalConfig
}

// IsEnabled says if CAL is enabled
func (c *Client) IsEnabled() bool {
	return ((c.mCalConfig != nil) && c.mCalConfig.enabled)
}

// IsInitialized says if CAL is initialized
func (c *Client) IsInitialized() bool {
	return c.mAlreadyInit
}

// IsPoolstackEnabled says if pool stack is enabled
func (c *Client) IsPoolstackEnabled() bool {
	if c.mCalConfig == nil {
		return false
	}
	return c.mCalConfig.getPoolstackEnabled()
}

/**
 * caller request different cxt buckets, and caller has a better knowledge about the
 * lifecycle of a cxt (e.g. start/end of a client connection). when a given cxt is
 * out of scope, caller should invoke this function to release the associated resource(s)
 * failing to do the cleanup may cause a memory leak when more objects keep arriving
 * while old objects still reside in the map but never get used again.
 */
func (c *Client) releaseCxtResource(_tgname ...string) {
	var ctxKey string
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		ctxKey = _tgname[0]
	} else {
		ctxKey = DefaultTGName
	}

	c.mPendingMsgBuffer[ctxKey] = nil
	delete(c.mPendingMsgBuffer, ctxKey)

	delete(c.mPending, ctxKey)
	delete(c.mCurrentCalTxn, ctxKey)
	delete(c.mRootCalTxn, ctxKey)
	delete(c.mCorrelationID, ctxKey)
	delete(c.mParentStack, ctxKey)
	delete(c.mCurrentOpName, ctxKey)
}

/**
 * If the requested key doesn't exist, we get the value type's zero value.
 */
func (c *Client) getCurrentCalTxn(_tgname ...string) *calTransaction {
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		return c.mCurrentCalTxn[_tgname[0]]
	}
	return c.mCurrentCalTxn[DefaultTGName]
}

func (c *Client) setCurrentCalTxn(_currentCalTxn *calTransaction, _tgname ...string) {
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		c.mCurrentCalTxn[_tgname[0]] = _currentCalTxn
	}
	c.mCurrentCalTxn[DefaultTGName] = _currentCalTxn
}

func (c *Client) getRootCalTxn(_tgname ...string) *calTransaction {
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		return c.mRootCalTxn[_tgname[0]]
	}
	return c.mRootCalTxn[DefaultTGName]
}

func (c *Client) setRootCalTxn(_rootCalTxn *calTransaction, _tgname ...string) {
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		c.mRootCalTxn[_tgname[0]] = _rootCalTxn
	} else {
		c.mRootCalTxn[DefaultTGName] = _rootCalTxn
	}
	if _rootCalTxn != nil {
		c.WriteData(handlerCtrlMsgNewRoot)
	}
}

func (c *Client) getCorrelationID(_tgname ...string) string {
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		return c.mCorrelationID[_tgname[0]]
	}
	return c.mCorrelationID[DefaultTGName]
}

func (c *Client) setCorrelationID(_id string, _tgname ...string) {
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		c.mCorrelationID[_tgname[0]] = _id
	} else {
		c.mCorrelationID[DefaultTGName] = _id
	}
}

func (c *Client) getPendingFlag(_tgname ...string) bool {
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		return c.mPending[_tgname[0]]
	}
	return c.mPending[DefaultTGName]
}

func (c *Client) setPendingFlag(_pending bool, _tgname ...string) {
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		c.mPending[_tgname[0]] = _pending
	} else {
		c.mPending[DefaultTGName] = _pending
	}
}

/**
 * need to return pointer of slice to allow caller append new elementes
 */
func (c *Client) getPendingMessageBuffer(_tgname ...string) *[]string {
	var ctxKey string
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		ctxKey = _tgname[0]
	} else {
		ctxKey = DefaultTGName
	}

	bucket, exist := c.mPendingMsgBuffer[ctxKey]

	//fmt.Printf("getmessagebuffer %s %d %d\n", ctxKey, exist, bucket)

	if !exist {
		var msgBufferSize = c.mCalConfig.getMsgBufferSize()
		//
		// who clean up the slice after conn is closed. see comment in ReleaseCtxResource
		//
		newbucket := make([]string, 0, msgBufferSize+5)
		bucket = &newbucket
		c.mPendingMsgBuffer[ctxKey] = bucket
	}

	//fmt.Printf("getmessagebuffer after %s %d %p\n", ctxKey, len(*bucket), bucket)
	return bucket
}

// GetPoolStack returns the pool stack information
func (c *Client) GetPoolStack(_tgname ...string) string {
	var pstack string
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		pstack = c.mParentStack[_tgname[0]]
	} else {
		pstack = c.mParentStack[DefaultTGName]
	}

	if len(pstack) > 0 {
		return (pstack + calPoolSeperator + c.getCurrentPoolInfo())
	}
	return c.getCurrentPoolInfo()
}

// GetPoolName gets the CAL pool name
func (c *Client) GetPoolName() string {
	cfg := c.getConfigInstance()
	if cfg == nil {
		return "UNSET"
	}
	return cfg.getPoolName()
}

// GetReleaseBuildNum gets the build number as a string
func (c *Client) GetReleaseBuildNum() string {
	cfg := c.getConfigInstance()
	if cfg == nil {
		return ""
	}
	return cfg.getReleaseBuildNum()
}

// SetParentStack set the CAL parent inthe stack
func (c *Client) SetParentStack(_clientpoolInfo string, _operationName string, _tgname string) (err error) {
	if len(_clientpoolInfo) >= c.mCalConfig.poolStackSize {
		first := strings.Index(_clientpoolInfo, calPoolSeperator)
		if first != -1 {
			_clientpoolInfo = _clientpoolInfo[first:]
			second := strings.Index(_clientpoolInfo, calPoolSeperator)
			c.mParentStack[_tgname] = _clientpoolInfo[second:]
		} else {
			c.mParentStack[_tgname] = _clientpoolInfo
			err = fmt.Errorf("bad poolstack info. clientpoolinfo data: %s", _clientpoolInfo)
		}
	} else {
		c.mParentStack[_tgname] = _clientpoolInfo
	}

	if len(_operationName) > 0 {
		c.mCurrentOpName[_tgname] = _operationName
	}
	return err
}

func (c *Client) getCurrentPoolInfo(_tgname ...string) string {
	if !(c.IsEnabled() && c.IsPoolstackEnabled()) {
		return ""
	}
	var ctxkey string
	var ctxkeyprintable string
	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		ctxkey = _tgname[0]
		ctxkeyprintable = ctxkey
	} else {
		ctxkey = DefaultTGName
		ctxkeyprintable = "0"
	}
	localhost, err := os.Hostname()
	if err != nil {
		localhost = loopbackIP
	}
	root := c.getRootCalTxn(ctxkey)
	txnStartTime := "TopLevelTxn not set"
	if root != nil {
		txnStartTime = c.getLogID(root.mTimeStamp)
	}
	//
	//the current pool info format <poolname>:<Op name>*CalThreadId=<thread id>*TopLevelTxnStartTime=<toplevel txn start time>*Host=<host>
	//
	info := fmt.Sprintf("%s%c%s*CalThreadId=%s*TopLevelTxnStartTime=%s*Host=%s",
		c.mCalConfig.getPoolName(), calFeatureSeperator, c.mCurrentOpName[ctxkey], ctxkeyprintable, txnStartTime, localhost)
	return info
}

func (c *Client) getLogID(_ts string) string {
	pos := strings.Index(_ts, ".")
	centisec, err := strconv.Atoi(_ts[pos:])
	if err != nil {
		centisec = 0
	}
	now := time.Now()
	year, month, day := now.Date()
	t := time.Date(year, month, day, 0, 0, 0, 0, now.Location()).Unix()*1000 + int64(centisec)*10
	return fmt.Sprintf("%X", t)
}
