// Copyright 2021 PayPal Inc.
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

/*
run using
$GOROOT/bin/go test -c cfgFromTns_test.go && ./lib.test ; tail hera.log
*/

package lib

import (
	"github.com/paypal/hera/lib"
	"os"
	"testing"
)

func TestCfgFromTns(t *testing.T) {
	t.Log("basic cfg from tns")

	fh, err := os.Create("hera.txt")
	fh.WriteString("bind_port=2222\n")
	fh.Close()
	err = lib.InitConfig()
	if err != nil {
		t.Errorf("config initialization failure %s", err.Error())
		return
		//lib.GetConfig() = &Config{} // BacklogTimeoutMsec: 1, LifoScheduler: true, numWorkersCh: make(chan int, 1)}
		//gOpsConfig = &OpsConfig{numWorkers: 3}
		//lib.GetConfig().numWorkersCh <- int(gOpsConfig.numWorkers)
	}
	stateLogPrefix := "hera"
	lib.GetConfig().StateLogPrefix = stateLogPrefix
	os.Setenv("TNS_ADMIN", stateLogPrefix+"dbconfig")
	os.Mkdir(os.Getenv("TNS_ADMIN"), 0755)

	////////////////////////////////////////////////////////////

	fh, err = os.Create(stateLogPrefix+"dbconfig/tnsnames.ora")
	if err != nil {
		t.Errorf("test write tnsnamesOra fail %s", err.Error())
		return
	}
	fh.WriteString("WINKY=(x)\n")
	fh.Close()

	lib.GetConfig().NumOfShards = 1
	lib.GetConfig().EnableTAF = false
	lib.GetConfig().ReadonlyPct = 0
	lib.CfgFromTns("hera-winky-batch")
	if lib.GetConfig().NumOfShards != 1 {
		t.Errorf("sharding not expected")
	}
	if lib.GetConfig().EnableTAF {
		t.Errorf("taf not expected")
	}
	if lib.GetConfig().ReadonlyPct != 0 {
		t.Errorf("rwSplit not expected")
	}

	////////////////////////////////////////////////////////////

	fh, err = os.Create(stateLogPrefix+"dbconfig/tnsnames.ora")
	if err != nil {
		t.Errorf("test write tnsnamesOra fail %s", err.Error())
		return
	}
	fh.WriteString("WINKY=(x)\n")
	fh.WriteString("WINKY_HERA=(x)\n")
	fh.Close()

	lib.GetConfig().NumOfShards = 1
	lib.GetConfig().EnableTAF = false
	lib.GetConfig().ReadonlyPct = 0
	lib.CfgFromTns("hera-winky-batch")
	if lib.GetConfig().NumOfShards != 1 {
		t.Errorf("sharding not expected")
	}
	if lib.GetConfig().EnableTAF {
		t.Errorf("taf not expected")
	}
	if lib.GetConfig().ReadonlyPct == 0 {
		t.Errorf("rwSplit expected")
	}

	////////////////////////////////////////////////////////////

	fh, err = os.Create(stateLogPrefix+"dbconfig/tnsnames.ora")
	if err !=nil {
		t.Errorf("test write tnsnamesOra fail %s", err.Error())
		return
	}
	fh.WriteString("WINKY_R1=(x\n")
	fh.WriteString(")\n")
	fh.WriteString("WINKY_R2=(x)\n")
	fh.Close()

	lib.GetConfig().NumOfShards = 1
	lib.GetConfig().EnableTAF = false
	lib.GetConfig().ReadonlyPct = 0
	lib.CfgFromTns("hera-winky_r1")
	if lib.GetConfig().NumOfShards != 1 {
		t.Errorf("sharding not expected")
	}
	if lib.GetConfig().EnableTAF == false {
		t.Errorf("taf expected")
	}
	if lib.GetConfig().ReadonlyPct != 0 {
		t.Errorf("rwSplit not expected")
	}

	////////////////////////////////////////////////////////////

	fh, err = os.Create(stateLogPrefix+"dbconfig/tnsnames.ora")
	if err !=nil {
		t.Errorf("test write tnsnamesOra fail %s", err.Error())
		return
	}
	fh.WriteString("WINKY_SH0=(x)\n")
	fh.WriteString("WINKY_SH1=(x)\n")
	fh.Close()

	lib.GetConfig().NumOfShards = 1
	lib.GetConfig().EnableTAF = false
	lib.GetConfig().ReadonlyPct = 0
	lib.CfgFromTns("hera-winky-batch")
	if lib.GetConfig().NumOfShards <= 1 {
		t.Errorf("sharding expected")
	}
	if lib.GetConfig().EnableTAF {
		t.Errorf("taf not expected")
	}
	if lib.GetConfig().ReadonlyPct != 0 {
		t.Errorf("rwSplit not expected")
	}

	////////////////////////////////////////////////////////////

	fh, err = os.Create(stateLogPrefix+"dbconfig/tnsnames.ora")
	if err !=nil {
		t.Errorf("test write tnsnamesOra fail %s", err.Error())
		return
	}
	fh.WriteString("WINKY_SH0=(x)\n")
	fh.WriteString("WINKY_SH1=(x)\n")
	fh.WriteString("WINKY_HERA_SH0=(x)\n")
	fh.WriteString("WINKY_HERA_SH1=(x)\n")
	fh.Close()

	lib.GetConfig().NumOfShards = 1
	lib.GetConfig().EnableTAF = false
	lib.GetConfig().ReadonlyPct = 0
	lib.CfgFromTns("hera-winky-batch")
	if lib.GetConfig().NumOfShards <= 1 {
		t.Errorf("sharding expected")
	}
	if lib.GetConfig().EnableTAF {
		t.Errorf("taf not expected")
	}
	if lib.GetConfig().ReadonlyPct == 0 {
		t.Errorf("rwSplit expected")
	}

	////////////////////////////////////////////////////////////

	fh, err = os.Create(stateLogPrefix+"dbconfig/tnsnames.ora")
	if err !=nil {
		t.Errorf("test write tnsnamesOra fail %s", err.Error())
		return
	}
	fh.WriteString("WINKY_R1_SH0=(x)\n")
	fh.WriteString("WINKY_R1_SH1=(x)\n")
	fh.WriteString("WINKY_R2_SH0=(x)\n")
	fh.WriteString("WINKY_R2_SH1=(x)\n")
	fh.Close()

	lib.GetConfig().NumOfShards = 1
	lib.GetConfig().EnableTAF = false
	lib.GetConfig().ReadonlyPct = 0
	lib.CfgFromTns("hera-winky_r1")
	if lib.GetConfig().NumOfShards <= 1 {
		t.Errorf("sharding expected")
	}
	if lib.GetConfig().EnableTAF == false {
		t.Errorf("taf expected")
	}
	if lib.GetConfig().ReadonlyPct != 0 {
		t.Errorf("rwSplit not expected")
	}
}


