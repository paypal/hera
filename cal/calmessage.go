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
	"bytes"
	//"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/paypal/hera/utility"
	//"log"
)

/**
 * different from c++ calclient, go calclient support multi-threading.
 * user of calclient does not need to worry about resource protection.
 * the extra consideration here is to decide whether to enable thread grouping
 * and, if so, how to group calmessages.
 * thread grouping is controled by "cal_enable_threadgroup" in cal_client.txt.
 * once enabled, user can set thread group name in each calmessage through Init().
 * messages in the same thread group are put in the same swimlane in cal logview.
 * if cal_enable_threadgroup!=true, all calmsgs will end up in one swimminglane.
 * there is no limit on how many groups to create, but there is a limit on the number
 * of swimlanes in cal logview. go client supported a maximum of 100 swimlanes.
 * if number of thread groups is more than 100, some of them will share swimlanes.
 *
 * sample usage
 *   et := cal.NewCalEvent(cal.EVENT_TYPE_MESSAGE, "mux_started", cal.TRANS_OK, "", "tgname")
 *   et.AddDataStr("key", "loadprotected")
 *   et.Completed()
 *   cal.ReleaseCxtResource("tgname")	// optional. see comment
 * in this case, thread group name is "tgname".
 *
 * for a long running process, a cal user may choose to create a large number of
 * thread groups. inside cal, a considerable amount of resources is allocated for
 * each thread group. cal user has a better knowledge about the lifecycle
 * of a particular thread group. so, it is cal user's responsibility to release
 * resources allocated for a thread group at the end of its lifecycle to avoid memory
 * leak. by the way, if cal user put all his calmessages under a single thread group,
 * it is not required to do this cleanup since one bucket has a fixed size. only the
 * growth of buckets is a concern. however, allowing calmessages from all goroutines
 * to go under one thread group may lead to a messy caltxn chain and illegible output
 * in cal logview
 *
 * caltxns on different goroutine need to be put into different thread group.
 * caltxn is not thread safe for managing nested caltxns from different goroutine. there is a
 * known race condition in get/setcurrentcaltxn and completeAnyNestedTransactions. there
 * could be a way to navigate out a thread safe solution for nested caltxn from different goroutine.
 * but caltxn on one goroutine as parent of caltxn on another goroutine does not make much sense.
 */
const (
	//
	// name of the thread group passed in through a context.WithValue
	//
	CALThreadIDName = "ThreadId"
	DefaultTGName   = "CDTGName##%%&&**"
	//
	// @TODO max number of thread groups allowed. if client created too many groups, we
	// need a way to protect ourselves from overflowing. cal document suggests it
	// will cut off at 200 (double check).
	// each thread group still has its calmsg chain/nesting maintained separately by
	// the maps in calclient. but when writing into cal logview, different thread group
	// may end up having to share a same swimminglane.
	// the way this works is thread group name is first hashed into a 4-byte integer
	// after that the integer is modulized by 100 to produce a number between 0 and 99
	//
	CALMaxThreadNum = 100

	calUnset        = "unset"
	calUnknown      = "U"
	calZero         = "0"
	calEndOfLine    = "\r\n"
	calSetName      = "SetName"
	calAddDataPairs = "AddDataPairs"
	calBase64Data   = "__Base64Data__"
	calAmpersand    = "&"
	calEquals       = "="

	calTab    = "\t"
	calDollar = "$"

	calPeriod                 = "."
	calMessageLogPrefix       = "CalMessageLog:"
	calTypeBadInstrumentation = "BadInstrumentation"
	calNameAlreadyCompleted   = "AlreadyCompleted"
	calNameCompletingParent   = "CompletingParentWithUncompletedChild"

	calClassStartTransaction  = "t"
	calClassEndTransaction    = "T"
	calClassAtomicTransaction = "A"
	calClassEvent             = "E"
	calClassHeartbeat         = "H"

	calMaxNameLength        = 127 //PPSCR01148980
	calMaxTypeLength        = 127 //PPSCR01148980
	calClientThreadID       = 0
	calMaxMessageBufferSize = 300

	calFeatureSeperator = ':'
	calPoolSeperator    = "^"
)

/**
 * private implementation classes.
 *
 * CalTxn, Event, and CalHeartbeat may be created in different goroutines.
 * but the same object lives inside one goroutine and is not touched by other goroutines.
 * in other words, attributes inside these message objects do not have to be threadsafe.
 *
 * however, there is only one CalSocketHandler object underneath serving all the
 * message objects. attributes in CalSocketHandler like ringbuffer and clientsocket
 * should be protected. same is ture for each of the non-concurrent map in calclient.
 */
type calActivity struct {
	Activity // not really necessary, but just to be explicit.
	//
	// name of the thread group this calactivity should be put under.
	// for mux, it could be the address of incoming connection pointer.
	// aggregrate all calmsg for one incoming connection seems to be a good way to group
	//
	mThreadGroupName string
	//
	// 11 bytes [00:00:00.00]
	//
	mTimeStamp string
	//
	// 1 byte (A t T E or H)
	//
	mClass  string
	mType   string
	mName   string
	mStatus string
	mData   string
	//
	// tracking to prevent duplicate write
	//
	mCompleted bool
}
type calEvent struct {
	calActivity
	mParent *calTransaction
}
type calHeartBeat struct {
	calActivity
	mParent *calTransaction
}
type calTransaction struct {
	calActivity
	mParent   *calTransaction
	mDuration float32
	mTimer    CalTimer
}

// NewCalEvent creates a CAL event
func NewCalEvent(_type string, _name string, _status string, _data string, _tgname ...string) Event {
	et := new(calEvent)
	if len(_tgname) > 0 {
		et.init(_type, _name, _status, _data, _tgname[0])
	} else {
		et.init(_type, _name, _status, _data)
	}
	return et
}

// NewCalHeartBeat creates a CAL heartbeat
func NewCalHeartBeat(_type string, _name string, _status string, _data string, _tgname ...string) HeartBeat {
	hb := new(calHeartBeat)
	if len(_tgname) > 0 {
		hb.init(_type, _name, _status, _data, _tgname[0])
	} else {
		hb.init(_type, _name, _status, _data)
	}
	return hb
}

// NewCalTransaction creates a CAL transaction
func NewCalTransaction(_type string, _name string, _status string, _data string, _tgname string) Transaction {
	ct := new(calTransaction)
	ct.init(_type, _name, _status, _data, _tgname)
	return ct
}

// ReleaseCxtResource releases the internal context
func ReleaseCxtResource(_tgname ...string) {
	var client = GetCalClientInstance()
	if client != nil {
		var ctxkey string
		if len(_tgname) > 0 && len(_tgname[0]) > 0 {
			ctxkey = _tgname[0]
		} else {
			ctxkey = DefaultTGName
		}

		gMapMtx.Lock()
		client.releaseCxtResource(ctxkey)
		gMapMtx.Unlock()
	}
}

/********************************************************
 ********************************************************
 *               calActivity
 ********************************************************
 */
func (act *calActivity) initialize(_type string, _name string, _status string, _data string, _tgname ...string) {
	//act.mThreadGroupName, _ = _ctx.Value(CALThreadIDName).(string)
	//act.mCtx = _ctx

	if len(_tgname) > 0 && len(_tgname[0]) > 0 {
		act.mThreadGroupName = _tgname[0]
	} else {
		act.mThreadGroupName = DefaultTGName
	}

	act.mTimeStamp = time.Now().Format("15:04:05.00")
	act.validateAndSetType(_type)
	act.validateAndSetName(_name)
	act.validateAndSetStatus(_status)
	act.AddData(_data)
}

func (act *calActivity) SetName(_name string) {
	if !act.isCalClientEnabled() {
		return
	}
	if act.mCompleted {
		act.reportAlreadyCompletedEvent(calSetName, _name)
		return
	}
	act.validateAndSetName(_name)
}

func (act *calActivity) SetStatus(_status string) {
	if !act.isCalClientEnabled() {
		return
	}
	//
	// ignore the new status if the member status is already set to non-zero
	//
	if (act.mStatus != calZero) && (act.mStatus != calUnknown) {
		return
	}
	act.validateAndSetStatus(_status)
}

func (act *calActivity) SetStatusRc(_status string, _rc uint32) {
	if !act.isCalClientEnabled() {
		return
	}
	act.SetStatus(act.formatStatusWithRc(_status, _rc))
}

func (act *calActivity) AddDataInt(_name string, _value int64) {
	if !act.isCalClientEnabled() {
		return
	}
	act.AddDataStr(_name, strconv.FormatInt(_value, 10))
}

func (act *calActivity) AddDataStr(_name string, _value string) {
	if !act.isCalClientEnabled() {
		return
	}
	var buf bytes.Buffer
	if len(_name) > 0 {
		buf.WriteString(_name)
		buf.WriteString(calEquals)
	}
	buf.WriteString(_value)
	act.AddData(buf.String())
}

func (act *calActivity) AddData(_nameValuePairs string) {
	if !act.isCalClientEnabled() {
		return
	}
	if len(_nameValuePairs) <= 0 {
		return
	}
	if act.mCompleted {
		act.reportAlreadyCompletedEvent(calAddDataPairs, _nameValuePairs)
		return
	}
	act.validateAndAppendData(_nameValuePairs)
}

func (act *calActivity) GetStatus() string {
	if !act.isCalClientEnabled() {
		return ""
	}
	return act.mStatus
}

func (act *calActivity) Completed() {
	if act.mCompleted {
		return
	}
	if !act.isCalClientEnabled() {
		return
	}
	act.sendSelf()
	act.mCompleted = true
}

/**
 * maps in calclients are not threadsafe and require access protection.
 */
func (act *calActivity) GetRootCalTxn() Transaction {
	var root Transaction
	var client = GetCalClientInstance()
	if client != nil {
		gMapMtx.Lock()
		root = client.getRootCalTxn(act.mThreadGroupName)
		gMapMtx.Unlock()
	}
	return root
}

func (act *calActivity) SetRootCalTxn(_root Transaction) {
	var client = GetCalClientInstance()
	if client != nil {
		gMapMtx.Lock()
		//
		// ignore and let it go if client passes a different type.
		//
		ct, ok := _root.(*calTransaction)
		if ok {
			client.setRootCalTxn(ct, act.mThreadGroupName)
		}
		gMapMtx.Unlock()
	}
}

func (act *calActivity) GetCurrentCalTxn() Transaction {
	var cur Transaction
	var client = GetCalClientInstance()
	if client != nil {
		gMapMtx.Lock()
		cur = client.getCurrentCalTxn(act.mThreadGroupName)
		gMapMtx.Unlock()
	}
	return cur
}

func (act *calActivity) SetCurrentCalTxn(_cur Transaction) {
	var client = GetCalClientInstance()
	if client != nil {
		gMapMtx.Lock()
		//
		// ignore and let it go if client passes a different type.
		//
		ct, ok := _cur.(*calTransaction)
		if ok {
			client.setCurrentCalTxn(ct, act.mThreadGroupName)
		}
		gMapMtx.Unlock()
	}
}

func (act *calActivity) AddPoolStack() {
	if !act.isCalClientEnabled() || !GetCalClientInstance().IsPoolstackEnabled() {
		return
	}

	if act.mType == EventTypeClientInfo {
		gMapMtx.Lock()
		stackInfo := GetCalClientInstance().GetPoolStack()
		gMapMtx.Unlock()
		if len(stackInfo) > 0 {
			act.AddDataStr("PoolStack", stackInfo)
		}
	}
}

func (act *calActivity) SetParentStack(_clientpoolInfo string, _operationName string, _tgname ...string) (err error) {
	var client = GetCalClientInstance()
	if client != nil {
		var ctxkey string
		if len(_tgname) > 0 && len(_tgname[0]) > 0 {
			ctxkey = _tgname[0]
		} else {
			ctxkey = DefaultTGName
		}

		gMapMtx.Lock()
		err = client.SetParentStack(_clientpoolInfo, _operationName, ctxkey)
		gMapMtx.Unlock()
	}
	return err
}

/**
 * calactivity private functions
 */
func (act *calActivity) isPending() bool {
	var client = GetCalClientInstance()
	if client != nil {
		gMapMtx.Lock()
		defer gMapMtx.Unlock()
		return client.getPendingFlag(act.mThreadGroupName)
	}
	return false
}

func (act *calActivity) setPending(_pendingFlag bool) {
	var client = GetCalClientInstance()
	if client != nil {
		gMapMtx.Lock()
		client.setPendingFlag(_pendingFlag, act.mThreadGroupName)
		gMapMtx.Unlock()
	}
}

func (act *calActivity) getPendingMessageBuffer() *[]string {
	var client = GetCalClientInstance()
	if client != nil {
		gMapMtx.Lock()
		defer gMapMtx.Unlock()
		return client.getPendingMessageBuffer(act.mThreadGroupName)
	}
	return nil
}

func (act *calActivity) sendSelf() {
	var buf bytes.Buffer
	buf.WriteString(act.mClass)
	//
	// c++ String.append(char*) drops the terminator, so 11 bytes only.
	//
	buf.WriteString(act.mTimeStamp)
	buf.WriteString(calTab)
	buf.WriteString(act.mType)
	buf.WriteString(calTab)
	buf.WriteString(act.mName)
	buf.WriteString(calTab)
	buf.WriteString(act.mStatus)
	buf.WriteString(calTab)
	buf.WriteString(act.mData)
	buf.WriteString(calEndOfLine)
	var str = buf.String()
	act.writeData(str)
}

func (act *calActivity) writeData(_msg string) {
	var client = GetCalClientInstance()
	if client == nil || !client.IsInitialized() || !client.IsEnabled() {
		return
	}
	var messageBuffer = act.getPendingMessageBuffer()
	if messageBuffer == nil {
		return
	}
	if len(*messageBuffer) >= act.getMaxMsgBufferSize() {
		act.writeTraceMessage(calLogDebug, 0, "Message buffer limit of 300 for CAL_PENDING flag exceeded. Forcefully disabling the pending flag.")
		act.setPending(false)
	}
	//
	// go mutex is not reentrant. as a result, we can not nest it.
	//
	gMapMtx.Lock()
	//fmt.Printf("before append %d %p %d\n", len(*messageBuffer), *messageBuffer, messageBuffer)
	*messageBuffer = append(*messageBuffer, _msg)
	//fmt.Printf("after append %d %p %d\n", len(*messageBuffer), *messageBuffer, messageBuffer)
	gMapMtx.Unlock()

	act.flushMessageBuffer(false)
}

func (act *calActivity) flushMessageBuffer(_forceFlush bool) {
	//
	// flushMessageBuffer could be called directly without WriteData
	// eg: a)Finalizing the root transaction's Name
	//     b)Completing root transaction without finalizing its Name
	//
	var client = GetCalClientInstance()
	if client == nil || !client.IsInitialized() || !client.IsEnabled() {
		return
	}
	if act.isPending() && !_forceFlush {
		return
	}
	var messageBuffer = act.getPendingMessageBuffer()
	if messageBuffer == nil {
		return
	}
	//log.Println("before writemessage ", len(*messageBuffer))
	for _, msg := range *messageBuffer {
		act.writeMessageToHandler(msg)
	}
	//
	// reset size to 0 to reuse the same slice instead of reallocating a new one.
	// an old string left in the slice gets gc-ed when a newer string is assigned to
	// its slot
	//
	gMapMtx.Lock()
	*messageBuffer = (*messageBuffer)[0:0]
	//*messageBuffer = ([]string{})
	gMapMtx.Unlock()
}

func (act *calActivity) writeMessageToHandler(_msg string) {
	var client = GetCalClientInstance()
	if client == nil {
		//
		// @TODO error message
		//
		return
	}
	var enableTG = false
	var cfg = client.getConfigInstance()
	if cfg != nil {
		enableTG = cfg.enableTG
	}

	//
	// @TODO: performance. hash ctxkey into 4 bytes, mod by 100, add to the end of _msg.
	//
	tid := make([]byte, 4)
	if enableTG {
		h := fnv.New32a()
		h.Write([]byte(act.mThreadGroupName))
		//binary.LittleEndian.PutUint32(tid, h.Sum32())
		binary.LittleEndian.PutUint32(tid, (h.Sum32() % CALMaxThreadNum))
	}
	client.WriteData(_msg + string(tid))
}

func (act *calActivity) isCalClientEnabled() bool {
	var client = GetCalClientInstance()
	if client != nil {
		return client.IsEnabled()
	}
	return false
}

func (act *calActivity) getMaxMsgBufferSize() int {
	var client = GetCalClientInstance()
	if client == nil {
		return calMaxMessageBufferSize
	}
	var cfg = client.getConfigInstance()
	if cfg == nil {
		return calMaxMessageBufferSize
	}
	return cfg.getMsgBufferSize()
}

func (act *calActivity) validateAndSetType(_type string) {
	var strlen = len(_type)
	if strlen <= 0 {
		act.mType = calUnset
	}
	if strlen > calMaxTypeLength {
		// up to calMaxTypeLength - 2
		act.mType = _type[0 : calMaxTypeLength-1]
		// mark mType as truncated at index calMaxTypeLength - 1
		act.mType += "+"
	} else {
		act.mType = _type
	}
}

func (act *calActivity) validateAndSetName(_name string) {
	var strlen = len(_name)
	if strlen <= 0 {
		act.mName = calUnset
	}
	if strlen > calMaxNameLength {
		// up to calMaxName- 2
		act.mName = _name[0 : calMaxNameLength-1]
		// mark mName as truncated at index calMaxNameLength - 1
		act.mName += "+"
	} else {
		act.mName = _name
	}
}

func (act *calActivity) validateAndSetStatus(_status string) {
	// if status.length() is 0, then the string is probably a temporary
	// created by somebody calling SetStatus(0) instead of SetStatus("0").
	if len(_status) <= 0 {
		act.mStatus = calZero
	} else {
		act.mStatus = _status
	}
}

func (act *calActivity) validateAndAppendData(_data string) {
	var buf bytes.Buffer
	if len(act.mData) > 0 {
		buf.WriteString(act.mData)
		buf.WriteString(calAmpersand)
	}
	//
	// @TODO if data.skip_newline(0) < (int)data.length()	{
	//
	if false {
		buf.WriteString(calBase64Data)
		buf.WriteString("=")
		var dataBytes = []byte(_data)
		buf.WriteString(base64.StdEncoding.EncodeToString(dataBytes))
	} else {
		buf.WriteString(_data)
	}
	act.mData = buf.String()
}

func (act *calActivity) formatStatusWithRc(_status string, _rc uint32) string {
	//
	// if string doesn't already end with a '.', add one.
	//
	if strings.HasSuffix(_status, ".") {
		return fmt.Sprintf("%s%d", _status, _rc)
	}
	return fmt.Sprintf("%s.%d", _status, _rc)
}

func (act *calActivity) reportAlreadyCompletedEvent(_func string, _arg string) {
	var event = new(calEvent)
	event.initialize(calTypeBadInstrumentation, calNameAlreadyCompleted, "1", "", act.mThreadGroupName)
	event.AddDataStr("Class", act.mClass)
	event.AddDataStr("When", act.mTimeStamp)
	event.AddDataStr("Type", act.mType)
	event.AddDataStr("Name", act.mName)
	event.AddDataStr("Func", _func)
	event.AddDataStr("Arg", _arg)
	event.Completed()
}

func (act *calActivity) SendSQLData(sqlQuery string) uint32 {
	hash32 := utility.GetSQLHash(sqlQuery)

	var buf bytes.Buffer
	buf.WriteString(calDollar)
	buf.WriteString(fmt.Sprintf("%d", hash32))
	buf.WriteString(calTab)
	buf.WriteString(sqlQuery)
	buf.WriteString(calEndOfLine)

	act.writeData(buf.String())

	return hash32
}
func (act *calActivity) isBacktraceEnabled() bool {
	return false
}
func (act *calActivity) generateBacktraceData() string {
	return ""
}
func (act *calActivity) writeTraceMessage(_loglevel LogLevel, _errno int32, _msg string) {
	// not used
}

/********************************************************
 ********************************************************
 *               calEvent
 ********************************************************
 */
func (act *calEvent) init(_type string, _name string, _status string, _data string, _tgname ...string) {
	if !act.isCalClientEnabled() {
		return
	}
	act.mClass = calClassEvent
	if len(_tgname) > 0 {
		act.initialize(_type, _name, _status, _data, _tgname[0])
	} else {
		act.initialize(_type, _name, _status, _data)
	}
	act.mParent = act.GetCurrentCalTxn().(*calTransaction)
	if act.mParent != nil {
		act.mParent.onChildCreation()
	}
	//
	// @TODO backtrace
	//
}

func (act *calEvent) SetType(_type string) {
	if !act.isCalClientEnabled() {
		return
	}
	act.validateAndSetType(_type)
}

/********************************************************
 ********************************************************
 *               calHeartbeat
 ********************************************************
 */
func (act *calHeartBeat) init(_type string, _name string, _status string, _data string, _tgname ...string) {
	if !act.isCalClientEnabled() {
		return
	}
	act.mClass = calClassHeartbeat
	if len(_tgname) > 0 {
		act.initialize(_type, _name, _status, _data, _tgname[0])
	} else {
		act.initialize(_type, _name, _status, _data)
	}
	act.mParent = act.GetCurrentCalTxn().(*calTransaction)
	if act.mParent != nil {
		act.mParent.onChildCreation()
	}
}

/********************************************************
 ********************************************************
 *               calTransaction
 ********************************************************
 */
func (act *calTransaction) init(_type string, _name string, _status string, _data string, _tgname string) {
	if !act.isCalClientEnabled() {
		return
	}
	act.mClass = calClassAtomicTransaction
	act.initialize(_type, _name, _status, _data, _tgname)

	act.mParent = act.GetCurrentCalTxn().(*calTransaction)
	if act.mParent != nil {
		act.mParent.onChildCreation()
	}

	rootTxn := act.GetRootCalTxn()
	if rootTxn == nil {
		act.SetRootCalTxn(act)
	}
	act.SetCurrentCalTxn(act)
	act.mTimer.Reset()
	act.mDuration = -1
	//
	// @TODO backtrace
	//
}

func (act *calTransaction) SetNameWithFlag(_name string, _flag Flags) {
	if !act.isCalClientEnabled() {
		return
	}
	act.SetName(_name)

	if act.mParent == nil {
		act.SetOperationName(_name, true)
	}
	if _flag == FlagPending {
		act.handlePendingFlag()
	} else if _flag == FlagFinalizeRootName {
		act.handleFinalizeRootNameFlag(_name)
	}
}

func (act *calTransaction) SetStatusWithFlag(_status string, _flag Flags) {
	if !act.isCalClientEnabled() {
		return
	}
	act.SetStatus(_status)
	if _flag == FlagSetRootStatus {
		root := act.GetRootCalTxn()
		if root != nil {
			root.SetStatus(_status)
		}
	}
}

func (act *calTransaction) SetRootTransactionStatus(_status string) {
	if !act.isCalClientEnabled() {
		return
	}
	root := act.GetRootCalTxn()
	if root != nil {
		root.SetStatus(_status)
	}
}

func (act *calTransaction) AddDataToRoot(_name string, _value string) {
	if !act.isCalClientEnabled() {
		return
	}
	root := act.GetRootCalTxn()
	if root != nil {
		root.AddDataStr(_name, _value)
	}
}

func (act *calTransaction) SetDuration(_duration int) {
	if !act.isCalClientEnabled() {
		return
	}
	if _duration < minDuration {
		act.mDuration = minDuration
	} else if _duration > maxDuration {
		act.mDuration = maxDuration
	} else {
		act.mDuration = float32(_duration)
	}
}

func (act *calTransaction) Completed() {
	if act.mCompleted {
		return
	}
	if !act.isCalClientEnabled() {
		return
	}
	//log.Println("caltxn completed")
	if (act.mParent == nil) && (act.mClass == calClassEndTransaction) {
		act.addAdditionalFieldsForRoot()
	}
	act.completeAnyNestedTransactions()
	act.sendSelf()
	act.mCompleted = true
	act.onCompletion()
}

func (act *calTransaction) CompletedWithStatus(_status string) {
	if !act.isCalClientEnabled() {
		return
	}
	act.SetStatus(_status)
	act.Completed()
}

// TxnStatus creates the txn status string out of severity, module and error
func TxnStatus(_severity string, _module string, _sysErr string, rc ...interface{}) string {
	if _severity != TransOK {
		var buf bytes.Buffer
		buf.WriteString(_severity)
		buf.WriteString(calPeriod)
		buf.WriteString(_module)
		buf.WriteString(calPeriod)
		buf.WriteString(_sysErr)
		buf.WriteString(calPeriod)
		if len(rc) > 0 {
			switch v := rc[0].(type) {
			case int:
				buf.WriteString(strconv.Itoa(v))
			case string:
				buf.WriteString(v)
			default:
				buf.WriteString("0")
			}
		} else {
			buf.WriteString("0")
		}
		return buf.String()
	}
	return TransOK
}

func (act *calTransaction) GetCorrelationID() string {
	var client = GetCalClientInstance()
	if client != nil {
		gMapMtx.Lock()
		defer gMapMtx.Unlock()
		return client.getCorrelationID(act.mThreadGroupName)
	}
	return ""
}

func (act *calTransaction) SetCorrelationID(_id string) {
	var client = GetCalClientInstance()
	if client != nil {
		gMapMtx.Lock()
		client.setCorrelationID(act.mThreadGroupName, _id)
		gMapMtx.Unlock()
	}
}

/**
 * @TODO correlation and others like operationname sessionid poolinfo
 */
func (act *calTransaction) SetOperationName(_opname string, _forceFlag bool) {
	// not used
}

/**
 * caltransaction private functions
 */
func (act *calTransaction) onChildCreation() {
	if act.mClass == calClassAtomicTransaction {
		act.mClass = calClassStartTransaction
		act.sendSelf()
	}
}

func (act *calTransaction) sendSelf() {
	switch act.mClass {
	case calClassAtomicTransaction: //Send end transaction message with 'A'
		act.flushAtomicTransaction()
	case calClassStartTransaction: //Send start transaction mesage with 't'
		act.flushStartTransaction()
		act.mClass = calClassEndTransaction
	case calClassEndTransaction: //Send end transaction message with 'T'
		act.flushEndTransaction()
	default:
		// Adding log
	}
	//log.Println("caltxn write")
}

func (act *calTransaction) handlePendingFlag() {
	if (act.mParent == nil) && (act.mClass != calClassEndTransaction) {
		// Enable message bufferring if the current transaction is root
		// and there is no CAL message flushed out yet.
		act.writeTraceMessage(calLogDebug, 0, "Starting message buffering with Pending Flag")
		act.flushMessageBuffer(false)
		act.setPending(true)
	}
}

func (act *calTransaction) handleFinalizeRootNameFlag(_name string) {
	if !act.isPending() {
		return
	}
	var root = act.GetRootCalTxn().(*calTransaction)
	var msgbuffer = act.getPendingMessageBuffer()
	//
	// Transaction is atomic till now
	//
	if (len(_name) > 0) && (root != nil) && (msgbuffer != nil) && (len(*msgbuffer) > 0) {
		root.SetName(_name)
		act.SetOperationName(_name, true)
		var buf = root.prepareStartOfTransactionMessage(calClassStartTransaction)
		gMapMtx.Lock()
		(*msgbuffer)[0] = buf
		gMapMtx.Unlock()
		act.writeTraceMessage(calLogDebug, 0, buf+" -- Finalized ")
	}
	act.setPending(false)
	act.flushMessageBuffer(false)
}

func (act *calTransaction) flushStartTransaction() {
	act.writeData(act.prepareStartOfTransactionMessage(calClassStartTransaction))
}

func (act *calTransaction) flushAtomicTransaction() {
	act.writeData(act.prepareEndOfTransactionMessage(calClassAtomicTransaction))
}

func (act *calTransaction) flushEndTransaction() {
	act.mTimeStamp = time.Now().Format("15:04:05.00")
	act.writeData(act.prepareEndOfTransactionMessage(calClassEndTransaction))
}

func (act *calTransaction) prepareStartOfTransactionMessage(_msgClass string) string {
	var buf bytes.Buffer
	if act.mParent == nil {
		// to make raw logs more readable (for humans)
		// we put out a blank line before each level 0 non-atomic transaction
		buf.WriteString(calEndOfLine)
	}
	buf.WriteString(_msgClass)
	buf.WriteString(act.mTimeStamp)
	buf.WriteString(calTab)
	buf.WriteString(act.mType)
	buf.WriteString(calTab)
	buf.WriteString(act.mName)
	buf.WriteString(calEndOfLine)
	return buf.String()
}

func (act *calTransaction) prepareEndOfTransactionMessage(_msgClass string) string {
	var duration_str string
	// to safeguard 64 to 32 bit int conversion
	value := act.mTimer.Duration()
	var duration float32
	if int32(value) > maxDuration { // Check on comparision between float and int
		duration = maxDuration
	} else {
		duration = value
	}
	if act.mDuration >= minDuration {
		duration = act.mDuration
	}
	duration_str = fmt.Sprintf("%.1f", duration)
	var buf bytes.Buffer
	buf.WriteString(_msgClass)
	buf.WriteString(act.mTimeStamp)
	buf.WriteString(calTab)
	buf.WriteString(act.mType)
	buf.WriteString(calTab)
	buf.WriteString(act.mName)
	buf.WriteString(calTab)
	buf.WriteString(act.mStatus)
	buf.WriteString(calTab)
	buf.WriteString(duration_str)
	buf.WriteString(calTab)
	buf.WriteString(act.mData)
	buf.WriteString(calEndOfLine)
	return buf.String()
}

func (act *calTransaction) onCompletion() {
	act.SetCurrentCalTxn(act.mParent)
	if act.mParent == nil {
		//
		// Flush buffer definitely before the end of a root transaction
		//
		act.setPending(false)
		act.flushMessageBuffer(true)
		//
		// If root transaction is being completed, reset Root transaction variable to NULL
		//
		act.SetRootCalTxn(nil)
		act.writeTraceMessage(calLogDebug, 0, "Cleared root transaction ")
	}
}

func (act *calTransaction) completeAnyNestedTransactions() {
	//
	// the innermost transaction
	//
	var nested = act.GetCurrentCalTxn().(*calTransaction)
	//
	// the transaction explicity being closed
	//
	var self = act
	for (nested != nil) && (nested != self) {
		var event = new(calEvent)
		event.init(act.mThreadGroupName, calTypeBadInstrumentation, calNameCompletingParent, "1", "")
		event.AddDataStr("ParentType", self.mType)
		event.AddDataStr("ParentName", self.mName)
		event.AddDataStr("ChildType", nested.mType)
		event.AddDataStr("ChildName", nested.mName)
		event.Completed()
		nested.Completed()
		nested = nested.mParent
	}
}

/**
 * prefix correlationid, logid and sessionid to paylaod in transaction
 */
func (act *calTransaction) addAdditionalFieldsForRoot() {
	var buf bytes.Buffer
	buf.WriteString("corr_id_")
	buf.WriteString(calEquals)
	buf.WriteString(act.GetCorrelationID())
	buf.WriteString(calAmpersand)
	buf.WriteString("log_id_")
	buf.WriteString(calEquals)
	//buf.WriteString(client.GetLogId())
	buf.WriteString(calAmpersand)
	buf.WriteString("session_id_")
	buf.WriteString(calEquals)
	//buf.WriteString(client.GetSessionId())
	if len(act.mData) > 0 {
		buf.WriteString(calAmpersand)
	}
	buf.WriteString(act.mData)

	act.mData = buf.String()
}
