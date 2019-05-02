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
	"testing"
	/*
		"sync"
		"time"
		"math/rand"
		"io/ioutil"
	*/)

const (
	assertMaxPct = "Pct == tafMaxPct"
)

func TestTAFPct(t *testing.T) {
	//	SetLogVerbosity(LOG_VERBOSE)

	InitTAF(3 /*3 shards*/)
	tf := GetTAF(1)

	taflb := tf.(*taf)

	t.Log("Running TAF TestBasic")

	if (taflb.pct != tafMaxPct) || (taflb.deltaPct != 1) {
		t.Error(assertMaxPct)
	}

	tf.NotifyOK()
	if (taflb.pct != tafMaxPct) || (taflb.deltaPct != 1) {
		t.Error(assertMaxPct)
	}

	pcts := []uint32{tafMaxPct - 1, tafMaxPct - 1 - 2, tafMaxPct - 1 - 2 - 4, tafMaxPct - 1 - 2 - 4 - 8, tafMaxPct - 1 - 2 - 4 - 8 - 10, tafMaxPct - 1 - 2 - 4 - 8 - 10 - 10}
	deltaPcts := []uint32{2, 4, 8, 16, 32, 64}
	for i := 0; i < len(pcts); i++ {
		tf.NotifyError()
		if (taflb.pct != pcts[i]) || (taflb.deltaPct != deltaPcts[i]) {
			t.Errorf("i = %d, taflb.pct = %d ==? pcts[i] = %d, taflb.deltaPct = %d ==? deltaPcts[i] = %d\n", i, taflb.pct, pcts[i], taflb.deltaPct, deltaPcts[i])
		}
	}

	tf.NotifyOK()
	if (taflb.pct != pcts[len(pcts)-2]) || (taflb.deltaPct != deltaPcts[len(pcts)-2]) {
		t.Errorf("taflb.pct = %d ==? pcts[i] = %d, taflb.deltaPct = %d ==? deltaPcts[i] = %d\n", taflb.pct, pcts[len(pcts)-2], taflb.deltaPct, deltaPcts[len(pcts)-2])
	}

	for i := 0; i < 10; i++ {
		tf.NotifyError()
	}
	if (taflb.pct != tafMinPct) || (taflb.deltaPct != tafDeltaPctMax) {
		t.Error("Pct == tafMinPct")
	}

	tf.NotifyOK()
	if (taflb.pct != (tafMinPct + 10)) || (taflb.deltaPct != (tafDeltaPctMax / 2)) {
		t.Error("Pct == tafMinPct + 10")
	}

	taflb.pct = 51
	taflb.deltaPct = 4
	tf.NotifyOK()
	if (taflb.pct != 61) || (taflb.deltaPct != 2) {
		t.Error("Pct == 61")
	}

	taflb.pct = 51
	taflb.deltaPct = 4
	tf.NotifyError()
	if (taflb.pct != 47) || (taflb.deltaPct != 8) {
		t.Error("Pct == 47")
	}
}

func TestTAFPctSharding(t *testing.T) {
	InitTAF(3 /*3 shards*/)

	taflb0 := GetTAF(0).(*taf)
	taflb1 := GetTAF(1).(*taf)
	taflb2 := GetTAF(2).(*taf)

	t.Log("Running TAF TestTAFPctSharding")

	if (taflb0.pct != tafMaxPct) || (taflb0.deltaPct != 1) {
		t.Error(assertMaxPct)
	}
	if (taflb1.pct != tafMaxPct) || (taflb1.deltaPct != 1) {
		t.Error(assertMaxPct)
	}
	if (taflb2.pct != tafMaxPct) || (taflb2.deltaPct != 1) {
		t.Error(assertMaxPct)
	}

	GetTAF(1).NotifyError()

	if (uint32(GetTAF(0).GetPct()) != tafMaxPct) || (taflb0.deltaPct != 1) {
		t.Error(assertMaxPct)
	}
	if (uint32(GetTAF(1).GetPct()) != (tafMaxPct - 1)) || (taflb1.deltaPct != 2) {
		t.Error(assertMaxPct)
	}
	if (uint32(GetTAF(2).GetPct()) != tafMaxPct) || (taflb2.deltaPct != 1) {
		t.Error(assertMaxPct)
	}
}
