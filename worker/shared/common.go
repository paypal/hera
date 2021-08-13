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

package shared

import (
	"io"

	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding"
	"github.com/paypal/hera/utility/logger"
)

// func WriteAll(w io.Writer, ns *netstring.Netstring) error {
func WriteAll(w io.Writer, ns *encoding.Packet) error {
	if logger.GetLogger().V(logger.Verbose) {
		if ns.Cmd == common.CmdEOR {
			logger.GetLogger().Log(logger.Verbose, "worker writing EOR to mux >>> ", len(ns.Serialized), ":", common.CmdEOR, " ",
				byte(ns.Payload[0])-'0', "<",
				(uint32(ns.Payload[1])<<24)+(uint32(ns.Payload[2])<<16)+(uint32(ns.Payload[3])<<8)+uint32(ns.Payload[4]),
				"> ", DebugString(ns.Payload[5:]))
		} else {
			logger.GetLogger().Log(logger.Verbose, "worker writing to mux >>> ", DebugString(ns.Serialized))
		}
	}
	return writeAll(w, ns.Serialized)
}

// writeAll blocks until writing all the data
func writeAll(w io.Writer, data []byte) error {
	written := 0
	for written < len(data) {
		n, err := w.Write(data[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

// DebugString is used for debugging to truncate strings bigger than 200 bytes
func DebugString(data []byte) string {
	if len(data) > 200 {
		return string(data[0:200])
	}
	return string(data)
}
