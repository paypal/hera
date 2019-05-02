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

package netstring

import (
	"io"
	"strings"
	"testing"
)

type nsCase struct {
	Serialized string
	ns         *Netstring
}

func tcase(tcases []nsCase, t *testing.T) {
	for _, tcase := range tcases {
		t.Log("Testing for: ", tcase.Serialized)
		ns, _ := NewNetstring(strings.NewReader(tcase.Serialized))
		if ns.Cmd != tcase.ns.Cmd {
			t.Log("Command expected", tcase.ns.Cmd, "instead got", ns.Cmd)
			t.Fail()
		}
		if strings.Compare(string(ns.Payload), string(tcase.ns.Payload)) != 0 {
			t.Log("Payload expected", string(tcase.ns.Payload), "instead got", string(ns.Payload))
			t.Fail()
		}
		if strings.Compare(string(ns.Serialized), string(tcase.ns.Serialized)) != 0 {
			t.Log("Payload expected", string(tcase.ns.Serialized), "instead got", string(ns.Serialized))
			t.Fail()
		}
		t.Log("Done testing for: ", tcase.Serialized)
	}
}

func TestBasic(t *testing.T) {
	t.Log("Start TestBasic ++++++++++++++")

	basic := []nsCase{{Serialized: "5:502 0,", ns: &Netstring{Cmd: 502, Payload: []byte("0"), Serialized: []byte("5:502 0,")}},
		{Serialized: "3:502,", ns: &Netstring{Cmd: 502, Payload: []byte(""), Serialized: []byte("3:502,")}}}
	tcase(basic, t)

	t.Log("End TestBasic ++++++++++++++")
}

func TestWriteEmbedded(t *testing.T) {
	nss := make([]*Netstring, 3)
	nss[0] = NewNetstringFrom(502, []byte("abc"))
	nss[1] = NewNetstringFrom(5, []byte(""))
	nss[2] = NewNetstringFrom(25, []byte("1234567890?1234567890?1234567890?"))

	ns := NewNetstringEmbedded(nss)
	if ns.Cmd != 0 {
		t.Log("Command expected '0' instead got", ns.Cmd)
		t.Fail()
	}
	if strings.Compare(string(ns.Payload), "7:502 abc,1:5,36:25 1234567890?1234567890?1234567890?,") != 0 {
		t.Log("Payload expected '7:502 abc,1:5,36:25 1234567890?1234567890?1234567890?,' instead got ", string(ns.Payload))
		t.Fail()
	}
	if strings.Compare(string(ns.Serialized), "56:0 7:502 abc,1:5,36:25 1234567890?1234567890?1234567890?,,") != 0 {
		t.Log("Serialized expected '56:0 7:502 abc,1:5,36:25 1234567890?1234567890?1234567890?,,' instead got", string(ns.Serialized))
		t.Fail()
	}
}

func TestReadEmbedded(t *testing.T) {
	nss := make([]*Netstring, 3)
	nss[0] = NewNetstringFrom(502, []byte("xyzwx*abcdef"))
	nss[1] = NewNetstringFrom(5, []byte(""))
	nss[2] = NewNetstringFrom(25, []byte("1234567890*1234567890"))

	ns := NewNetstringEmbedded(nss)

	t.Log("NS::::", string(ns.Serialized))

	nss2, _ := SubNetstrings(ns)
	if len(nss2) != 3 {
		t.Log("Expected 3 sub-netstrings, instead got", len(nss2))
		t.Fail()
	}
	for idx, i := range nss2 {
		t.Log("Cmd:", i.Cmd, ", Payload:", string(i.Payload), ", Serialized:", string(i.Serialized))

		if i.Cmd != nss[idx].Cmd {
			t.Log("Command expected", nss[idx].Cmd, "instead got", i.Cmd)
			t.Fail()
		}
		if strings.Compare(string(i.Payload), string(nss[idx].Payload)) != 0 {
			t.Log("Payload expected", string(nss[idx].Payload), "instead got", string(i.Payload))
			t.Fail()
		}
		if strings.Compare(string(i.Serialized), string(nss[idx].Serialized)) != 0 {
			t.Log("Payload expected", string(nss[idx].Serialized), "instead got", string(i.Serialized))
			t.Fail()
		}
	}
}

func TestNetstringReader(t *testing.T) {
	reader := NewNetstringReader(strings.NewReader("54:0 16:502 xyzwx*abcdef,1:5,24:25 1234567890*1234567890,,55:0 17:502 xyzwx*WWWWWWW,1:5,24:25 1234567890*1234567890,,"))
	nss := make([]*Netstring, 6)
	nss[0] = NewNetstringFrom(502, []byte("xyzwx*abcdef"))
	nss[1] = NewNetstringFrom(5, []byte(""))
	nss[2] = NewNetstringFrom(25, []byte("1234567890*1234567890"))
	nss[3] = NewNetstringFrom(502, []byte("xyzwx*WWWWWWW"))
	nss[4] = NewNetstringFrom(5, []byte(""))
	nss[5] = NewNetstringFrom(25, []byte("1234567890*1234567890"))
	idx := -1
	var ns *Netstring
	var err error
	for {
		ns, err = reader.ReadNext()
		if err != nil {
			if err != io.EOF {
				t.Log("Error ReadNext: ", err.Error())
				t.Fail()
			}
			break
		}
		idx++
		t.Log("Cmd:", ns.Cmd, ", Payload:", string(ns.Payload), ", Serialized:", string(ns.Serialized))
		if ns.Cmd != nss[idx].Cmd {
			t.Log("Command expected", nss[idx].Cmd, "instead got", ns.Cmd)
			t.Fail()
		}
		if strings.Compare(string(ns.Payload), string(nss[idx].Payload)) != 0 {
			t.Log("Payload expected", string(nss[idx].Payload), "instead got", string(ns.Payload))
			t.Fail()
		}
		if strings.Compare(string(ns.Serialized), string(nss[idx].Serialized)) != 0 {
			t.Log("Payload expected", string(nss[idx].Serialized), "instead got", string(ns.Serialized))
			t.Fail()
		}
	}
	if idx != 5 {
		t.Log("Expected 6 Netstrings to be read, instead found only:", idx+1)
		t.Fail()
	}
}

func TestBadInput(t *testing.T) {
	reader := NewNetstringReader(strings.NewReader("54:0 16:502 "))
	_, err := reader.ReadNext()
	if err != nil {
		t.Log("OK: expected error:", err.Error())
	} else {
		t.Log("Bad input should have failed - incomplete Netstring")
		t.Fail()
	}
	reader = NewNetstringReader(strings.NewReader("55:0 16:502 xyzwx*abcdef,50:5,24:25 1234567890*1234567890,,"))
	// first NS is fine
	_, err = reader.ReadNext()
	if err != nil {
		t.Log("First Netstring should have been OK")
		t.Fail()
	}
	// second is bad, length is "50" but much fewer bytes are available
	_, err = reader.ReadNext()
	if err != nil {
		t.Log("OK: expected error:", err.Error())
	} else {
		t.Log("Bad input should have failed - incomplete embedded Netstring")
		t.Fail()
	}
}

// per https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go, to avoid compiler optimizations
var result *Netstring

func BenchmarkEncode(b *testing.B) {
	var ns *Netstring
	nss := make([]*Netstring, 10)
	for i := 0; i < b.N; i++ {
		nss[0] = NewNetstringFrom(25, []byte("select id, int_val, str_val from test where id = :account_id and name = :name and address = :address  /*12345-123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890-123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901*/"))
		nss[1] = NewNetstringFrom(4, []byte("account_id"))
		nss[2] = NewNetstringFrom(3, []byte("1234567890"))
		nss[3] = NewNetstringFrom(4, []byte("name"))
		nss[4] = NewNetstringFrom(3, []byte("John Smith"))
		nss[5] = NewNetstringFrom(4, []byte("address"))
		nss[6] = NewNetstringFrom(3, []byte("2211 North First Street, San Jose"))
		nss[7] = NewNetstringFrom(4, []byte(""))
		nss[8] = NewNetstringFrom(22, []byte(""))
		nss[9] = NewNetstringFrom(7, []byte("0"))
		ns = NewNetstringEmbedded(nss)
	}
	result = ns
}

func BenchmarkEncodeOne(b *testing.B) {
	var ns *Netstring
	for i := 0; i < b.N; i++ {
		ns = NewNetstringFrom(25, []byte("select id, int_val, str_val from test where id = :account_id and name = :name and address = :address"))
	}
	result = ns
}

var results []*Netstring

func BenchmarkDecode(b *testing.B) {
	var nss2 []*Netstring
	nss := make([]*Netstring, 10)
	nss[0] = NewNetstringFrom(25, []byte("select id, int_val, str_val from test where id = :account_id and name = :name and address = :address  /*12345-123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890-123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901*/"))
	nss[1] = NewNetstringFrom(4, []byte("account_id"))
	nss[2] = NewNetstringFrom(3, []byte("1234567890"))
	nss[3] = NewNetstringFrom(4, []byte("name"))
	nss[4] = NewNetstringFrom(3, []byte("John Smith"))
	nss[5] = NewNetstringFrom(4, []byte("address"))
	nss[6] = NewNetstringFrom(3, []byte("2211 North First Street, San Jose"))
	nss[7] = NewNetstringFrom(4, []byte(""))
	nss[8] = NewNetstringFrom(22, []byte(""))
	nss[9] = NewNetstringFrom(7, []byte("0"))
	ns := NewNetstringEmbedded(nss)
	//	b.Log("Decoding:", len(ns.Serialized), ":", string(ns.Serialized))
	for i := 0; i < b.N; i++ {
		nss2, _ = SubNetstrings(ns)
	}
	results = nss2
}

func BenchmarkDecodeOne(b *testing.B) {
	var ns2 *Netstring
	ns := NewNetstringFrom(25, []byte("select id, int_val, str_val from test where id = :account_id and name = :name and address = :address"))
	for i := 0; i < b.N; i++ {
		ns2, _ = NewNetstring(strings.NewReader(string(ns.Serialized)))
	}
	result = ns2
}

/* on hyper
BenchmarkEncode-24                 50000             29067 ns/op
BenchmarkEncodeOne-24             500000              3027 ns/op
BenchmarkDecode-24                200000             11055 ns/op
BenchmarkDecodeOne-24            1000000              1249 ns/op
*/
/* 1.10
goos: darwin
goarch: amd64
BenchmarkEncode-4      	  200000	      8188 ns/op
BenchmarkEncodeOne-4   	 2000000	       606 ns/op
BenchmarkDecode-4      	  500000	      2719 ns/op
BenchmarkDecodeOne-4   	 5000000	       319 ns/op
*/
/* 1.11
goos: darwin
goarch: amd64
BenchmarkEncode-4      	  200000	      6561 ns/op
BenchmarkEncodeOne-4   	 2000000	       638 ns/op
BenchmarkDecode-4      	  500000	      2771 ns/op
BenchmarkDecodeOne-4   	 5000000	       322 ns/op
*/
/* 1.12
goos: darwin
goarch: amd64
pkg: github.com/paypal/hera/utility/encoding/netstring
BenchmarkEncode-4      	  300000	      5160 ns/op
BenchmarkEncodeOne-4   	 3000000	       548 ns/op
BenchmarkDecode-4      	  500000	      2449 ns/op
BenchmarkDecodeOne-4   	 5000000	       299 ns/op
*/
