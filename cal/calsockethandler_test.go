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

/**
 * go test -v calclient
 */
import (
	"encoding/binary"
	"log"
	"os"
	"testing"
	"time"
)

func TestEndian(t *testing.T) {
	log.SetOutput(os.Stderr)

	var size uint32 = 16
	var cap uint32 = 128
	dummy := make([]byte, 8)
	binary.LittleEndian.PutUint32(dummy[0:4], size)
	binary.LittleEndian.PutUint32(dummy[4:8], cap)
	dummyb := make([]byte, 8)
	binary.BigEndian.PutUint32(dummyb[0:4], size)
	binary.BigEndian.PutUint32(dummyb[4:8], cap)
	//t.Error("failed to clear item")
	log.Println("little:", dummy)
	log.Println("big:", dummyb)

	log.Println("16:", binary.BigEndian.Uint32(dummyb[0:4]))
	log.Println("128:", binary.LittleEndian.Uint32(dummy[4:8]))
}

func TestTime(t *testing.T) {
	tn := time.Now()
	var ts = tn.UnixNano() / int64(time.Second)
	var us = (tn.UnixNano() % int64(time.Second)) / (int64(time.Microsecond) / int64(time.Nanosecond))
	log.Println("tn:", tn, "ts:", ts, "us:", us)
}
