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

package common

// Internal commands between worker and proxy
const (
	CmdControlMsg = 501
	CmdEOR        = 502 // end of response
)

// EOR codes
const (
	EORFree                     = 0
	EORInTransaction            = 1
	EORInCursorNotInTransaction = 2 /* not in transaction but not free because the cursor is open for ex */
	EORInCursorInTransaction    = 3 /* not in transaction but not free because the cursor is open for ex */
	EORMoreIncomingRequests     = 4 /* worker would be free, but it is not because there are more requests on the incomming buffer because
	they were pipelined by the client */
	EORBusyOther = 5 /* not used yet */
	EORRestart   = 6
)

// Reasons for stranded child
const (
	StrandedClientClose       = 4
	StrandedSaturationRecover = 5
	StrandedSwitch            = 6
	StrandedTimeout           = 7
	StrandedErr               = 8
)
