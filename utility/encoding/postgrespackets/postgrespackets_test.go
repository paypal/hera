// // Copyright 2019 PayPal Inc.
// //
// // Licensed to the Apache Software Foundation (ASF) under one or more
// // contributor license agreements.  See the NOTICE file distributed with
// // this work for additional information regarding copyright ownership.
// // The ASF licenses this file to You under the Apache License, Version 2.0
// // (the "License"); you may not use this file except in compliance with
// // the License.  You may obtain a copy of the License at
// //
// //    http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

package postgrespackets

// import (
// 	"github.com/paypal/hera/utility/encoding"
// 	"github.com/paypal/hera/utility/encoding/netstring"

// 	//"fmt"
// 	"math/rand"
// 	// "net"

// 	"bytes"
// 	"reflect"
// 	"testing"

// 	"github.com/paypal/hera/common"
// )

// var codes map[int]string

// type nsCase struct {
// 	Serialized []byte
// 	ns         *encoding.Packet
// }

// func tcase(tcases []nsCase, t *testing.T) {

// 	for _, tcase := range tcases {
// 		t.Log("Testing for: ", tcase.Serialized)
// 		ns, err := NewMySQLPacket(bytes.NewReader(tcase.Serialized))
// 		if err != nil {
// 			t.Log(err.Error())
// 		}
// 		// fmt.Println(reflect.TypeOf(ns))
// 		if ns.Length != tcase.ns.Length {
// 			t.Log("Length expected", tcase.ns.Length, "instead got", ns.Length)
// 		}
// 		if ns.Sqid != tcase.ns.Sqid {
// 			t.Log("Length expected", tcase.ns.Sqid, "instead got", ns.Sqid)
// 		}
// 		if ns.Cmd != tcase.ns.Cmd {
// 			t.Log("Command expected", tcase.ns.Cmd, "instead got", ns.Cmd)
// 			t.Fail()
// 		}

// 		if !reflect.DeepEqual(ns.Serialized, tcase.ns.Serialized) {
// 			t.Log("Serialized expected", tcase.ns.Serialized, "instead got", ns.Serialized)
// 			t.Fail()
// 		}
// 		t.Log("Done testing for: ", tcase.Serialized)
// 	}
// }

// /* Make test cases for simple queries. */
// func tmake() []nsCase {

// 	cases := make([]nsCase, 6)
// 	// Initialize all the relevant codes.
// 	codes = make(map[int]string)
// 	codes[common.COM_SLEEP] = "COM_SLEEP"
// 	codes[common.COM_QUIT] = "COM_QUIT"
// 	codes[common.COM_INIT_DB] = "COM_INIT_DB"
// 	codes[common.COM_QUERY] = "COM_QUERY"
// 	codes[common.COM_FIELD_LIST] = "COM_FIELD_LIST"
// 	codes[common.COM_CREATE_DB] = "COM_CREATE_DB"
// 	codes[common.COM_DROP_DB] = "COM_DROP_DB"
// 	codes[common.COM_REFRESH] = "COM_REFRESH"
// 	codes[common.COM_SHUTDOWN] = "COM_SHUTDOWN"

// 	codes[common.COM_STMT_PREPARE] = "COM_STMT_PREPARE"
// 	codes[common.COM_STMT_EXECUTE] = "COM_STMT_EXECUTE"
// 	codes[common.COM_STMT_SEND_LONG_DATA] = "COM_STMT_SEND_LONG_DATA"
// 	codes[common.COM_STMT_CLOSE] = "COM_STMT_CLOSE"
// 	codes[common.COM_STMT_FETCH] = "COM_STMT_FETCH"

// 	// COMMAND PACKETS
// 	var query []byte //, payload []byte

// 	query = []byte{0x00, 0x12, 00, 00, 00, 3, 83, 84, 65, 82, 84, 32, 84, 82, 65, 78, 83, 65, 67, 84, 73, 79, 78}
// 	// payload = []byte{3,  83,  84,  65,  82,  84,  32,  84,  82,  65,  78,  83,  65,  67,  84,  73,  79,  78}
// 	cases[0] = nsCase{Serialized: query, ns: &encoding.Packet{Cmd: 3, Length: 18, Sqid: 0, Serialized: query, Payload: query[HEADER_SIZE+1:]}}

// 	query = []byte{0x00, 0x2b, 00, 00, 00, 22, 105, 110, 115, 101, 114, 116, 32, 105, 110, 116, 111, 32, 116, 101, 115, 116, 49, 32, 40, 105, 100, 44, 32, 118, 97, 108, 41, 32, 118, 97, 108, 117, 101, 115, 32, 40, 63, 44, 32, 63, 41, 59}
// 	// payload = []byte{22,  105,  110,  115,  101,  114,  116,  32,  105,  110,  116,  111,  32,  116,  101,  115,  116,  49,  32,  40,  105,  100,  44,  32,  118,  97,  108,  41,  32,  118,  97,  108,  117,  101,  115,  32,  40,  63,  44,  32,  63,  41,  59}
// 	cases[1] = nsCase{Serialized: query, ns: &encoding.Packet{Cmd: 22, Length: 43, Sqid: 0, Serialized: query, Payload: query[HEADER_SIZE+1:]}}

// 	query = []byte{0x00, 0x20, 00, 00, 00, 23, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 8, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}
// 	// payload = []byte{23, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 8, 0, 8, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0}
// 	cases[2] = nsCase{Serialized: query, ns: &encoding.Packet{Cmd: 23, Length: 32, Sqid: 0, Serialized: query, Payload: query[HEADER_SIZE+1:]}}

// 	query = []byte{0x00, 0x20, 00, 00, 00, 22, 100, 101, 108, 101, 116, 101, 32, 102, 114, 111, 109, 32, 116, 101, 115, 116, 49, 32, 119, 104, 101, 114, 101, 32, 105, 100, 32, 61, 32, 50, 59}
// 	// payload = []byte{22, 100, 101, 108, 101, 116, 101,  32,  102,  114,  111,  109,  32,  116,  101,  115,  116,  49,  32,  119,  104,  101,  114,  101,  32,  105,  100,  32,  61,  32,  50,  59}
// 	cases[3] = nsCase{Serialized: query, ns: &encoding.Packet{Cmd: 22, Length: 32, Sqid: 0, Serialized: query, Payload: query[HEADER_SIZE+1:]}}

// 	query = []byte{0x00, 5, 0, 0, 0, 25, 1, 0, 0, 0}
// 	// payload = []byte{25, 1, 0, 0, 0}
// 	cases[4] = nsCase{Serialized: query, ns: &encoding.Packet{Cmd: 25, Length: 5, Sqid: 0, Serialized: query, Payload: query[HEADER_SIZE+1:]}}

// 	query = []byte{0x00, 1, 00, 00, 00, 1}
// 	// payload = []byte{01}
// 	cases[5] = nsCase{Serialized: query, ns: &encoding.Packet{Cmd: 1, Length: 1, Sqid: 0, Serialized: query, Payload: query[HEADER_SIZE+1:]}}

// 	return cases
// }

// // Tests whether or not NewPacket properly reads in a single packet
// // from a buffered reader
// func TestBasic(t *testing.T) {
// 	t.Log("Start TestBasic ++++++++++++++")

// 	tcase(tmake(), t)

// 	t.Log("End TestBasic ++++++++++++++")
// }

// // Tests whether or not packets get their headers properly prepended
// // before they're written out to the net.Conn for the client.
// func TestNewPacketFrom(t *testing.T) {

// 	t.Log("Start TestNewPacketFrom +++++++++++++")
// 	// Get those go-to queries
// 	tcases := tmake()

// 	for _, tcase := range tcases {
// 		t.Log("Testing for: ", tcase.Serialized)
// 		ns := NewMySQLPacketFrom(0, tcase.ns.Serialized[HEADER_SIZE+1:])
// 		if ns.Length != tcase.ns.Length {
// 			t.Log("Length expected", tcase.ns.Length, "instead got", ns.Length)
// 		}
// 		if ns.Sqid != tcase.ns.Sqid {
// 			t.Log("Length expected", tcase.ns.Sqid, "instead got", ns.Sqid)
// 		}
// 		if ns.Cmd != tcase.ns.Cmd {
// 			t.Log("Command expected", tcase.ns.Cmd, "instead got", ns.Cmd)
// 			t.Fail()
// 		}
// 		if !reflect.DeepEqual(ns.Serialized, tcase.ns.Serialized) {
// 			t.Log("Serialized expected", tcase.ns.Serialized, "instead got", ns.Serialized)
// 			t.Fail()
// 		}
// 		t.Log("Done testing for: ", tcase.Serialized)
// 	}

// 	t.Log("End TestNewPacketFrom +++++++++++++")

// }

// /* Tests the read next function which reads multiple packets from a stream. */
// func TestPackagerReadNext(t *testing.T) {
// 	t.Log("Start TestReadNext +++++++++++++")
// 	// Pick random number of packets to be 'sent' over the reader
// 	numPackets := rand.Intn(48) + 2 // Rand between 2 and 50

// 	// Pick length of terminal packet + header
// 	endPacketLength := rand.Intn(MAX_PACKET_SIZE - 1)

// 	// Create expected test packet! Note that everything is all 0s
// 	buf := make([]byte, MAX_PACKET_SIZE)
// 	expectedPacket := NewMySQLPacketFrom(0, buf) // Stream packet

// 	buf = make([]byte, endPacketLength)
// 	endPacket := NewMySQLPacketFrom(numPackets-1, buf) // Terminal packet

// 	t.Log("Running with ", numPackets, " packets and ", endPacketLength, " length end packet")

// 	big_payload := make([]byte, 0)
// 	idx := 0
// 	for i := 0; i < numPackets-1; i++ {
// 		big_payload = append(big_payload, expectedPacket.Serialized...)
// 		expectedPacket.Sqid++
// 		t.Log(expectedPacket.Sqid)
// 		idx += expectedPacket.Length
// 	}
// 	big_payload = append(big_payload, endPacket.Serialized...)
// 	if len(big_payload) != (numPackets-1)*(MAX_PACKET_SIZE+4)+endPacketLength+4 {
// 		t.Log("Unexpected big payload length ", len(big_payload))
// 	}

// 	// Reset sequence id
// 	expectedPacket.Sqid = 0

// 	// Create a new packet reader
// 	reader := bytes.NewReader(big_payload)
// 	packager := &Packager{reader: reader}

// 	// Since we have two packets, use a general variable for test packet
// 	var testPacket *encoding.Packet

// 	// Return the next packet from the string!
// 	for {
// 		t.Log("reader.ReadNext() in mysql_packets test")
// 		ns, err := packager.ReadNext()
// 		if err != nil {
// 			break
// 		}
// 		if ns.Length != MAX_PACKET_SIZE {
// 			testPacket = endPacket
// 		} else {
// 			testPacket = expectedPacket
// 		}
// 		t.Log("Packet number: ", expectedPacket.Serialized[3])

// 		// Test that the next packet read is as expected!
// 		if ns.Length != testPacket.Length {
// 			t.Log("Length expected", testPacket.Length, "instead got", ns.Length)
// 		}
// 		if ns.Sqid != testPacket.Sqid {
// 			t.Log("Sequence id expected", testPacket.Sqid, "instead got", ns.Sqid)
// 		}
// 		if ns.Cmd != testPacket.Cmd {
// 			t.Log("Command expected", testPacket.Cmd, "instead got", ns.Cmd)
// 			t.Fail()
// 		}
// 		if !reflect.DeepEqual(ns.Serialized, testPacket.Serialized) {
// 			t.Log("Payload expected", testPacket.Serialized, "instead got", ns.Serialized)
// 			t.Fail()
// 		}

// 		expectedPacket.Sqid++
// 	}

// 	if int(expectedPacket.Sqid) != numPackets {
// 		t.Log("Expected number of packets", numPackets, "instead got", int(expectedPacket.Sqid))
// 		t.Fail()
// 	}

// 	t.Log("End TestReadNext +++++++++++++")
// }

// /* Tests the write multiple function which writes multiple packets to a stream. */
// func TestPackagerWriteMultiple(t *testing.T) {
// 	t.Log("Start TestPackagerWriteMultiple +++++++++++++")

// 	// Create a buffer to mimic read and write operations from a connection
// 	var b bytes.Buffer

// 	// writer := bufio.NewWriter(&b)
// 	packager := &Packager{writer: &b}

// 	// Pick random number of packets to be 'sent' over the reader
// 	numPackets := rand.Intn(48) + 2 // Rand between 2 and 50

// 	// Pick length of terminal packet + header
// 	endPacketLength := rand.Intn(MAX_PACKET_SIZE - 1)

// 	// Create expected test packet! Note that everything is all 0s
// 	buf := make([]byte, MAX_PACKET_SIZE)
// 	expectedPacket := NewMySQLPacketFrom(0, buf) // Stream packet

// 	buf = make([]byte, endPacketLength)
// 	endPacket := NewMySQLPacketFrom(numPackets-1, buf) // Terminal packet

// 	t.Log("Running with ", numPackets, " packets and ", endPacketLength, " length end packet")

// 	big_payload := make([]byte, 0)
// 	idx := 0
// 	for i := 0; i < numPackets-1; i++ {
// 		big_payload = append(big_payload, expectedPacket.Payload...)
// 		expectedPacket.Sqid++
// 		// t.Log(expectedPacket.Sqid)
// 		idx += expectedPacket.Length
// 	}
// 	big_payload = append(big_payload, endPacket.Payload...)
// 	if len(big_payload) != (numPackets-1)*MAX_PACKET_SIZE+endPacketLength {
// 		t.Log("Unexpected big payload length ", len(big_payload))
// 	}

// 	// Reset sequence id
// 	expectedPacket.Sqid = 0

// 	packets, _ := packager.WritePacket(big_payload)

// 	for _, tp := range packets {
// 		b.Write(tp.Serialized)
// 	}

// 	packager.reader = bytes.NewReader(b.Bytes())

// 	if len(packets) != numPackets {
// 		t.Log("Expected number of packets", numPackets, ", got", len(packets))
// 	}

// 	if len(b.Bytes()) != len(big_payload)+4*numPackets {
// 		t.Log("Length of bytes,", len(b.Bytes()), "but expected", len(big_payload))
// 	}

// 	// Since we have two packets, use a general variable for test packet
// 	var testPacket *encoding.Packet

// 	// Return the next packet from the string!
// 	for {
// 		t.Log("reader.ReadNext() in mysql_packets test")
// 		ns, err := packager.ReadNext()
// 		// t.Log(err.Error())
// 		if err != nil {
// 			break
// 		}
// 		if ns.Length != MAX_PACKET_SIZE {
// 			testPacket = endPacket
// 		} else {
// 			testPacket = expectedPacket
// 		}
// 		t.Log("Packet number: ", testPacket.Serialized[4])

// 		// Test that the next packet read is as expected!
// 		if ns.Length != testPacket.Length {
// 			t.Log("Length expected", testPacket.Length, "instead got", ns.Length)
// 		}
// 		if ns.Sqid != testPacket.Sqid {
// 			t.Log("Sequence id expected", testPacket.Sqid, "instead got", ns.Sqid)
// 		}
// 		if ns.Sqid != int(ns.Serialized[4]) {
// 			t.Log("Out of sync sqid with packet and load: expected", ns.Sqid, "instead got", ns.Serialized[4])
// 		}
// 		if ns.Cmd != testPacket.Cmd {
// 			t.Log("Command expected", testPacket.Cmd, "instead got", ns.Cmd)
// 			t.Fail()
// 		}
// 		if !reflect.DeepEqual(ns.Serialized, testPacket.Serialized) {
// 			// t.Log("Payload expected", testPacket.Serialized, "instead got", ns.Serialized)
// 			t.Log("Payload doesn't match")
// 			t.Fail()
// 		}

// 		expectedPacket.Sqid++
// 		expectedPacket.Serialized[4] = expectedPacket.Serialized[4] + 1
// 		t.Log("Just read one of these")
// 	}

// 	if expectedPacket.Sqid != numPackets {
// 		t.Log("Expected number of packets", numPackets, "instead got", int(expectedPacket.Sqid))
// 		t.Fail()
// 	}

// 	t.Log("End TestPackagerWriteMultiple +++++++++++++")
// }

// func reEncodeNetstring(str string) string {
// 	byteStr := []byte(str)
// 	return string(append([]byte{1}, byteStr...))
// }

// func TestWrongPacket(t *testing.T) {
// 	t.Log("TestWrongPacket Start+++++++++++++++")

// 	cases := make([]nsCase, 3)
// 	query := []byte{0x00, 0x12, 00, 00, 00, 3, 83, 84, 65, 82, 84, 32, 84, 82, 65, 78, 83, 65, 67, 84, 73, 79, 78}
// 	// payload = []byte{3,  83,  84,  65,  82,  84,  32,  84,  82,  65,  78,  83,  65,  67,  84,  73,  79,  78}
// 	cases[0] = nsCase{Serialized: query, ns: &encoding.Packet{Cmd: 3, Length: 18, Sqid: 0, Serialized: query, Payload: query[HEADER_SIZE+1:]}}

// 	netstr := reEncodeNetstring("1234567890?1234567890?1234567890?")

// 	cases[1] = nsCase{[]byte(netstr), netstring.NewNetstringFrom(25, []byte("1234567890?1234567890?1234567890?"))}

// 	query2 := []byte{0x02, 0x12, 00, 00, 00, 3, 83, 84, 65, 82, 84, 32, 84, 82, 65, 78, 83, 65, 67, 84, 73, 79, 78}
// 	cases[2] = nsCase{query2, &encoding.Packet{Cmd: 3, Length: 18, Sqid: 0, Serialized: query2, Payload: query2[HEADER_SIZE+1:]}}

// 	t.Log("Case 2 : Tried to read mysqlpacket using netstring")

// 	_, err := netstring.NewNetstring(bytes.NewReader(cases[0].Serialized))
// 	if err == encoding.WRONGPACKET {
// 		t.Log("Correctly identified wrong packet ")
// 	} else if err != nil {
// 		t.Log(err.Error())
// 		t.Fail()
// 	} else {
// 		t.Log("Failed to catch wrong packet")
// 		t.Fail()
// 	}

// 	t.Log("Case 2 : Tried to read netstring packet using mysql")
// 	_, err = NewMySQLPacket(bytes.NewReader(cases[1].Serialized))
// 	if err == encoding.WRONGPACKET {
// 		t.Log("Correctly identified wrong packet ")
// 	} else if err != nil {
// 		t.Log(err.Error())
// 		t.Fail()
// 	} else {
// 		t.Log("Failed to catch wrong packet")
// 		t.Fail()
// 	}

// 	t.Log("Case 3 MySQL : Tried to read unknown packet")
// 	_, err = NewMySQLPacket(bytes.NewReader(cases[2].Serialized))
// 	if err == encoding.WRONGPACKET {
// 		t.Log("Should be unknown packet")
// 		t.Fail()
// 	} else if err != encoding.UNKNOWNPACKET {
// 		t.Log("Failed to identify unknown packet")
// 		t.Fail()
// 	} else {
// 		t.Log("Successfully identified unknown packet!")
// 	}

// 	t.Log("Case 3 Netstring : Tried to read unknown packet")
// 	_, err = netstring.NewNetstring(bytes.NewReader(cases[2].Serialized))
// 	if err == encoding.WRONGPACKET {
// 		t.Log("Should be unknown packet")
// 		t.Fail()
// 	} else if err != encoding.UNKNOWNPACKET {
// 		t.Log("Failed to identify unknown packet")
// 		t.Fail()
// 	} else {
// 		t.Log("Successfully identified unknown packet!")
// 	}

// 	t.Log("TestWrongPacket End+++++++++++++++++")
// }
