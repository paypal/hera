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

// Package encoding provides the encoding functions such as netstring etc.,
package encoding

type Packet struct {
	Cmd          int    // Command byte in the payload
	Serialized   []byte // The entire packet
	Payload      []byte // The entire payload
	Length       int    // Length of Payload
	CommandID    []byte // identifies the type of PSQL message
	IsPostgreSQL bool   // indicates whether or not the packet is PostgreSQL
}

// IsComposite returns if the netstring is compisite, embedding multiple netstrings in it
func (ns *Packet) IsComposite() bool {
	return ns.Cmd == ('0' - '0')
}

// Interface for reader
type Reader interface {
	ReadNext() (*Packet, error)
}

type WRONG_PACKET struct {
}

func (wp WRONG_PACKET) Error() string {
	return "Wrong packet type. Did you mix netstring with mysql?"
}

type unknown struct {
}

func (wp unknown) Error() string {
	return "Unknown packet type. Neither netstring nor mysql"
}

var WRONGPACKET = new(WRONG_PACKET)
var UNKNOWNPACKET = new(unknown)
