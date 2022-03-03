// Copyright 2022 PayPal Inc.
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
	"fmt"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility"
	"github.com/paypal/hera/utility/encoding/netstring"
)


func (crd *Coordinator) PreprocessQueryBindBlocker(requests []*netstring.Netstring) (bool, string) {
	qbb := GetQueryBindBlockerCfg()
	if qbb == nil {
		return false, ""
	}

	sz := len(requests)
	var sqltext string
	bindPairs := make([]string,0)
	for i := 0; i < sz; i++ {
		if (requests[i].Cmd == common.CmdPrepare) || (requests[i].Cmd == common.CmdPrepareV2) || (requests[i].Cmd == common.CmdPrepareSpecial) {
				sqltext = string(requests[i].Payload)
		}
		if requests[i].Cmd == common.CmdBindName {
			for j:=1; i+j<sz; j++ {
				if requests[i+j].Cmd == common.CmdBindNum {
					continue
				} else if requests[i+j].Cmd == common.CmdBindType {
					continue
				} else if requests[i+j].Cmd == common.CmdBindValueMaxSize {
					continue
				} else if requests[i+j].Cmd == common.CmdBindValue {
					bindPairs = append(bindPairs, string(requests[i].Payload))
					bindPairs = append(bindPairs, string(requests[i+j].Payload))
				} else {
					// i=3 i+j=5 ---- 3:name 4:value 5:name
					i += j-2
					break
				}
			}
		} // end if bind name
	}
	rv, reason := qbb.IsBlocked(sqltext, bindPairs)
	if rv {
		sqlhashStr := fmt.Sprintf("%d",uint32(utility.GetSQLHash(sqltext)))
		caltxn := cal.NewCalTransaction("DBA_QUERY_BIND_BLOCKER", sqlhashStr, /*status*/"1.DB.MANUAL.1", "", cal.DefaultTGName)
		caltxn.AddDataStr("val", reason)
		caltxn.AddDataStr("raddr", crd.conn.RemoteAddr().String())
		caltxn.Completed()
	}
	return rv, reason
}
