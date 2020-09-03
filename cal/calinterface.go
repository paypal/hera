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

// Activity defines the interface into CAL
// sample usage
//   var et Event = cal.NewCalEvent(cal.EventTypeMESSAGE, "mux_started", cal.TRANS_OK, "", "tgname")
//   et.AddDataStr("key", "loadprotected")
//   et.Completed()
//   cal.GetCalClientInstance().ReleaseCxtResource("tgname")	// optional. see comment in calmessage.go
// in this case, thread group is "tgname". see comments in calmessage.go on threadgroup
type Activity interface {
	SetName(string)
	SetStatus(string)
	SetStatusRc(string, uint32)
	AddDataInt(string, int64)
	AddDataStr(string, string)
	AddData(string)
	GetStatus() string
	Completed()
	GetRootCalTxn() Transaction
	SetRootCalTxn(Transaction)
	GetCurrentCalTxn() Transaction
	SetCurrentCalTxn(Transaction)
	AddPoolStack()
	SetParentStack(string, string, ...string)
	SendSQLData(string) uint32
	SetLossyRootTxnFlag()
}

// Event declares the functions for generating CAL events
type Event interface {
	Activity
	SetType(string)
}

// Transaction declares the functions for generating CAL transactions
type Transaction interface {
	Activity
	SetNameWithFlag(string, Flags)
	SetStatusWithFlag(string, Flags)
	SetRootTransactionStatus(string)
	AddDataToRoot(string, string)
	SetDuration(int)
	CompletedWithStatus(string)
	GetCorrelationID() string
	SetCorrelationID(string)
	SetOperationName(string, bool)
}

// HeartBeat declares the functions for generating CAL heartbeat
type HeartBeat interface {
	Activity
}

/**
 * Transaction status code standard.
 * For normal termination of a Transaction, the status code can simply be set to
 * CAL::TRANS_OK
 * For error termination, the format of the status code is as follow:
 * <severity>.<module name>.<system error code>.<module return code>
 * Transaction Types
 * Predefined CAL Transactions TYPES, DON't REUSE these unless you are instrumenting
 * new server or web application
 */
const (
	TransTypeClient        = "CLIENT"
	TransTypeExec          = "EXEC"
	TransTypeFetch         = "FETCH"
	TransTypeAPI           = "API"
	TransTypeURL           = "URL"
	TransTypeReplay        = "REPLAY"
	TransTypeClientSession = "CLIENT_SESSION"

	// severity code, Don't add extra level of SEVERITY
	TransOK      = "0"
	TransFatal   = "1"
	TransError   = "2"
	TransWarning = "3"

	// Addition data field name
	ErrDEscription = "ERR_DESCRIPTION"
	ErrAction      = "ERR_ACTION"

	// System error codes
	SysErrNone             = ""
	SysErrAccessDenied     = "ACCESS DENIED"
	SysErrConfig           = "CONFIG"
	SysErrConnectionFailed = "CONNECTION_FAILED"
	SysErrData             = "DATA"
	SysErrHandshake        = "HANDSHAKE"
	SysErrInternal         = "INTERNAL"
	SysErrMarkedDown       = "MARKED DOWN"
	SysErrHera             = "HERA"
	SysErrOracle           = "ORACLE"
	SysErrSQL              = "SQL"
	SysErrUnknown          = "UNKNOWN"

	// Event types
	EventTypeFatal     = "FATAL"
	EventTypeError     = "ERROR"
	EventTypeWarning   = "WARNING"
	EventTypeException = "EXCEPTION"
	EventTypeBacktrace = "Backtrace"
	EventTypePayload   = "Payload"
	EventTypeMarkup    = "MarkUp"
	EventTypeMarkdown  = "MarkDown"
	EventTypeTL        = "TL"
	EventTypeEOA       = "EOA"
	EventTypeMessage   = "MSG"

	//The name used by infrastructure to log client related information not to be used in product code.
	EventTypeClientInfo = "CLIENT_INFO"
	EventTypeServerInfo = "SERVER_INFO"

	//All cal messages for business monitoring should be done with this event type. It will be mainly used by product code.
	EventTypeBIZ = "BIZ"
)

// Flags defines the possible transaction flags
type Flags int

// Flags constants
const (
	FlagDefault = iota
	FlagPending
	FlagFinalizeRootName
	FlagSetRootStatus
)

// LogLevel defines the CAL log level
type LogLevel int

// LogLevel constants
const (
	calLogAlert = iota
	calLogWarning
	calLogInfo
	calLogDebug
	calLogVerbose
)
