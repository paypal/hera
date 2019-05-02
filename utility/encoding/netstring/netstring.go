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

// Package netstring contains encoding and decoding functions in netstring format
package netstring

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

const (
	colon byte = ':'
	comma byte = ','
	space byte = ' '
	// CodeSubCommand is a special command used to define that the payload contains multiple netstrings
	CodeSubCommand = '0'
)

// Netstring is a netstring packed, which consists of a command plus a payload
type Netstring struct {
	Cmd        int
	Serialized []byte // the entire netstring array. e.g. "100:1 xxx...yyy,"
	Payload    []byte // the content section of a netstring. e.g. "xxx...yyy"
}

// NewNetstring creates a Netstring from the reader, reading exactly as many bytes as necessary
func NewNetstring(_reader io.Reader) (*Netstring, error) {
	ns := &Netstring{}

	var buff bytes.Buffer
	var tmp = make([]byte, 1)
	var digit int
	var err error
	// read length
	length := 0
	for {
		_, err = _reader.Read(tmp)
		b := tmp[0]
		if err != nil {
			return nil, err
		}
		buff.WriteByte(b)
		if b == colon {
			break
		} else {
			digit = int(b - '0')
			if (digit < 0) || (digit > 9) {
				return nil, errors.New("Expected digit reading length")
			}
			length = length*10 + digit
		}
	}
	//read the rest
	totalLen := length + buff.Len() + 1 /*comma*/
	ns.Serialized = make([]byte, totalLen)
	copy(ns.Serialized, buff.Bytes())
	bytesRead := buff.Len()
	var n int
	for bytesRead < totalLen {
		n, err = _reader.Read(ns.Serialized[bytesRead:])
		if err != nil {
			return nil, err
		}
		bytesRead += n
	}
	// read command
	next := buff.Len()
	for next < (totalLen - 1) {
		if ns.Serialized[next] == space {
			next++
			break
		}
		digit = int(ns.Serialized[next] - '0')
		if (digit < 0) || (digit > 9) {
			return nil, errors.New("Expected digit reading command")
		}
		ns.Cmd = ns.Cmd*10 + digit
		next++
	}

	ns.Payload = ns.Serialized[next : totalLen-1]
	return ns, nil
}

// NewNetstringFrom creates a Netstring from command and Payload
func NewNetstringFrom(_cmd int, _payload []byte) *Netstring {
	// TODO: optimize
	payloadLen := len(_payload)
	cmdStr := fmt.Sprintf("%d", _cmd)
	var str string
	if payloadLen == 0 {
		str = fmt.Sprintf("%d:%s,", len(cmdStr), cmdStr)
	} else {
		str = fmt.Sprintf("%d:%s %s,", payloadLen+len(cmdStr)+1 /*the space*/, cmdStr, string(_payload))
	}
	ns := new(Netstring)
	ns.Cmd = _cmd
	ns.Serialized = []byte(str)
	if payloadLen > 0 {
		totalLen := len(ns.Serialized)
		ns.Payload = ns.Serialized[totalLen-payloadLen-1 : totalLen-1]
	}

	return ns
}

// NewNetstringEmbedded embedds a set of Netstrings into a netstring
func NewNetstringEmbedded(_netstrings []*Netstring) *Netstring {
	// TODO: optimize
	payloadLen := 0
	for _, i := range _netstrings {
		payloadLen += len(i.Serialized)
	}
	lenStr := fmt.Sprintf("%d:", payloadLen+2 /*len("0 ")*/)
	totalLen := len(lenStr) + payloadLen + 2 /*len("0 ")*/ + 1 /*ending comma*/
	ns := new(Netstring)
	ns.Serialized = make([]byte, totalLen)
	copy(ns.Serialized, []byte(lenStr))
	next := len(lenStr)
	copy(ns.Serialized[next:], []byte{CodeSubCommand, space})
	next += 2
	for _, i := range _netstrings {
		copy(ns.Serialized[next:], i.Serialized)
		next += len(i.Serialized)
	}
	ns.Serialized[next] = byte(comma)
	ns.Payload = ns.Serialized[totalLen-payloadLen-1 : totalLen-1]
	return ns
}

// SubNetstrings parses the embedded Netstrings
func SubNetstrings(_ns *Netstring) ([]*Netstring, error) {
	//  TODO: optimize for zero-copy
	var nss []*Netstring
	reader := bytes.NewReader(_ns.Payload)
	var ns *Netstring
	var err error
	for {
		ns, err = NewNetstring(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		nss = append(nss, ns)
	}
	return nss, nil
}

// Reader decodes netstrings from a buffer
type Reader struct {
	reader io.Reader
	ns     *Netstring
	nss    []*Netstring
	next   int
}

// NewNetstringReader creates a Reader, that maintains the state for embedded Netstrings
func NewNetstringReader(_reader io.Reader) *Reader {
	nsr := new(Reader)
	nsr.reader = _reader
	return nsr
}

// ReadNext returns the next Netstring from the stream. Note: in case of embedded netstrings,
// the Reader will buffer some Netstrings
func (reader *Reader) ReadNext() (ns *Netstring, err error) {
	for {
		if reader.ns != nil {
			ns = reader.ns
			reader.ns = nil
			return
		}
		if reader.next < len(reader.nss) {
			ns = reader.nss[reader.next]
			reader.next++
			return
		}
		reader.ns, err = NewNetstring(reader.reader)
		if err != nil {
			return nil, err
		}
		if reader.ns.Cmd == (CodeSubCommand - '0') {
			reader.nss, err = SubNetstrings(reader.ns)
			if err != nil {
				return nil, err
			}
			reader.ns = nil
			reader.next = 0
		}
	}
}

// IsComposite returns if the netstring is compisite, embedding multiple netstrings in it
func (ns *Netstring) IsComposite() bool {
	return ns.Cmd == (CodeSubCommand - '0')
}
