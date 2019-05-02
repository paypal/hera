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
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
	//"context"
)

func TestMain(m *testing.M) {
	rc := setup()
	if rc != 0 {
		log.Println("setup error")
		return
	}
	rc = m.Run()
	rc = teardown()
}

func setup() (rc int) {
	//if err := os.Chdir("../../.."); err != nil {
	//  log.Println("Chdir error:", err)
	//}
	rand.Seed(time.Now().UTC().UnixNano())

	flag.Set("alsologtostderr", "true")
	flag.Set("log_dir", ".")
	flag.Set("v", "5")

	return 0
}

func teardown() (rc int) {
	return 0
}

func TestInit(t *testing.T) {
	log.SetOutput(os.Stderr)
	kCALTypeBadInstrumentation := "kCALTypeBadInstrumentation"
	kCALNameCompletingParent := "kCALNameCompletingParent"

	//goctx := context.WithValue(context.Background(), CALThreadIdName, "main")
	calact := &calActivity{}
	calact.initialize("t1", kCALTypeBadInstrumentation, kCALNameCompletingParent, "1", "")
	calact.Completed()
	calevt := &calEvent{}
	calevt.init("t1", kCALTypeBadInstrumentation, kCALNameCompletingParent, "1", "")
	calevt.Completed()
	evtint := NewCalEvent("t1", kCALTypeBadInstrumentation, kCALNameCompletingParent, "1")
	evtint.AddDataStr("key", "loadprotected")
	evtint.Completed()
	caltxn := &calTransaction{}
	caltxn.init("t1", kCALTypeBadInstrumentation, kCALNameCompletingParent, "1", "")
	caltxn.Completed()
	txnint := NewCalTransaction("t1", kCALTypeBadInstrumentation, kCALNameCompletingParent, "1", "")
	txnint.Completed()

	//t.Error("failed to clear item")
	log.Println("msgclass ", calevt.mClass)
}

func TestTimestamp(t *testing.T) {
	for a := 0; a < 30; a++ {
		time.Sleep(time.Millisecond * 1)
		str := time.Now().Format("15:04:05.00")
		log.Println("timestamp ", str)
	}

	waitTime := time.Second * time.Duration(1)
	now := time.Now()
	time.Sleep(time.Millisecond * 100)
	waitTime -= time.Now().Sub(now)
	log.Println("waittime", waitTime)

	year, month, day := now.Date()
	ts := time.Date(year, month, day, 0, 0, 0, 0, now.Location()).Unix()*1000 + 15*10
	log.Println("logging ", year, month, day, ts, " hex=", fmt.Sprintf("%X", ts))
	now = time.Date(year, month, day-21, 0, 0, 0, 0, now.Location())
	year, month, day = now.Date()
	ts = now.Unix()*1000 + 15*10
	log.Println("logging ", year, month, day, ts, " hex=", fmt.Sprintf("%X", ts))
}

func TestZeroSlice(t *testing.T) {
	bag := make([]int, 0)
	log.Println("zerosize ", bag, len(bag))

	bag = append(bag, 1)
	log.Println("size1 ", bag, len(bag))
}

func TestWriteChann(t *testing.T) {
	tchann := make(chan int64)

	log.Println("writing ")
	select {
	case tchann <- 100:
	default:
		log.Println("failed to write ")
	}
	log.Println("wrote ")
}
