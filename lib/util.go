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
	"bytes"
	"errors"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility"
	"github.com/paypal/hera/utility/encoding/netstring"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
)

// WriteAll writes data in a loop until all is sent
// TODO: is this needed? isn't net.Conn.Write() all or nothing?
func WriteAll(w io.Writer, data []byte) error {
	towrite := len(data)
	for towrite > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		towrite -= n
	}
	return nil
}

// ParseBool returns true of the string is one of "1", "t", "T", "true", "TRUE", "True", "y", "Y" and
// false if it is one of "0", "f", "F", "false", "FALSE", "False", "n", "N"
func ParseBool(str string) (value bool, err error) {
	switch str {
	case "1", "t", "T", "true", "TRUE", "True", "y", "Y":
		return true, nil
	case "0", "f", "F", "false", "FALSE", "False", "n", "N":
		return false, nil
	}
	return false, errors.New("ParseBool syntax error")
}

// DebugString truncates the string if it is larger than 200 bytes
func DebugString(data []byte) string {
	if len(data) > 200 {
		var buf bytes.Buffer
		buf.WriteString(string(data[0:90]))
		buf.WriteString("...")
		buf.WriteString(string(data[len(data)-90:]))
		return buf.String()
	}
	return string(data)
}

// IsPidRunning checks to see if pid still associates to a running process
func IsPidRunning(pid int) (isRunning bool) {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return (err == nil)
}

/*
1st return value: the number
2nd return value: the number of digits
*/
func atoi(bf []byte) (int, int) {
	sz := len(bf)
	ret := 0
	for i := 0; i < sz; i++ {
		digit := int(bf[i] - '0')
		if (digit < 0) || (digit > 9) {
			return ret, i
		}
		ret = ret*10 + digit
	}
	return ret, sz
}

/*
1st return value: the number
2nd return value: the number of digits
*/
func atoui(str string) (uint64, int) {
	sz := len(str)
	var ret uint64
	for i := 0; i < sz; i++ {
		digit := uint64(str[i] - '0')
		if (digit < 0) || (digit > 9) {
			return ret, i
		}
		ret = ret*10 + digit
	}
	return ret, sz
}

// IPAddrStr stringifies the IP address
func IPAddrStr(address net.Addr) string {
	str := address.String()
	if 0 == len(str) {
		return ""
	}
	lidx := strings.LastIndex(str, ":")
	if -1 == lidx {
		return str
	}
	return str[:lidx]
}

// NetstringFromBytes creates a netstring containing data as payload.
func NetstringFromBytes(data []byte) (*netstring.Netstring, error) {
	reader := bytes.NewReader(data)
	ns, err := netstring.NewNetstring(reader)
	if err != nil {
		return nil, err
	}
	return ns, err
}

// ExtractSQLHash parse request to see if it has an embedded PREPARE statement.
// if there is one, compute and return the sqlhash and true.
// otherwise, return 0 and false
func ExtractSQLHash(request *netstring.Netstring) (uint32, bool) {
	//
	// create a collection of flat netstrings (e.g. de-subnetstring)
	//
	var nss []*netstring.Netstring
	var err error
	if request.Cmd == (netstring.CodeSubCommand - '0') {
		nss, err = netstring.SubNetstrings(request)
		if err != nil {
			return 0, false
		}
	} else {
		nss = append(nss, request)
	}

	for _, v := range nss {
		switch v.Cmd {
		case common.CmdPrepare, common.CmdPrepareV2, common.CmdPrepareSpecial:
			if v.Payload == nil {
				return 0, false
			}
			return utility.GetSQLHash(string(v.Payload)), true
		}
	}
	return 0, false
}

// Contains This is utility method to check whether value present in list or not
func Contains[T comparable](slice []T, value T) bool {
	for _, val := range slice {
		if val == value {
			return true
		}
	}
	return false
}
