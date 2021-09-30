// Copyright 2021 PayPal
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
	"bufio"
	"fmt"
	"os"
	"strings"
)



// name hera-winky-batch 
// state log prefix hera
func CfgFromTns(name string) {
	if GetConfig().CfgFromTns == false {
		return
	}

	stateLogPrefix := GetConfig().StateLogPrefix
	rwSuffix := strings.ToUpper(stateLogPrefix)

	baseName := strings.ToUpper(name[len(stateLogPrefix)+1:])
	idx := strings.Index(baseName,"-")
	if idx > 0 {
		baseName = baseName[0:idx]
	}

	tnsEntries, err := FindTns()
	if err != nil {
		logErr(err.Error())
		return
	}

	twoTaskSuffix := os.Getenv("TWO_TASK_SUFFIX")
	var ok bool
	numShards := 0
	tafShards := 0
	rwShards := 0
	for {
		dbName := fmt.Sprintf("%s_SH%d%s", baseName, numShards, twoTaskSuffix)
		_,ok = tnsEntries[dbName]
		if !ok {
			break
		}
		os.Setenv(fmt.Sprintf("TWO_TASK_%d",numShards), dbName)

		dbName = fmt.Sprintf("%s2_SH%d%s", baseName[:len(baseName)-1], numShards, twoTaskSuffix)
		_,ok = tnsEntries[dbName]
		if ok && baseName[len(baseName)-2] == 'R' {
			tafShards++
			os.Setenv(fmt.Sprintf("TWO_TASK_STANDBY0_%d",numShards), dbName)
		}

		dbName = fmt.Sprintf("%s_%s_SH%d%s", baseName, rwSuffix, numShards, twoTaskSuffix)
		_,ok = tnsEntries[dbName]
		if ok {
			rwShards++
			os.Setenv(fmt.Sprintf("TWO_TASK_READ_%d",numShards), dbName)
		}

		numShards++
	}
	if numShards > 0 {
		GetConfig().EnableSharding=true
		GetConfig().NumOfShards=numShards
		// shard key must be configured
		logErr(fmt.Sprintf("numShards=%d taf:%d rw:%d",numShards,tafShards,rwShards))

		if numShards == tafShards {
			GetConfig().EnableTAF=true
			logErr("sh taf=true")
		}
		if numShards == rwShards {
			GetConfig().ReadonlyPct=50
			logErr("sh rw=true")
		}

	}

	if numShards == 0 {
		dbName := baseName[:len(baseName)-1]+"2"+twoTaskSuffix
		_,ok = tnsEntries[dbName] // taf
		if ok && baseName[len(baseName)-2] == 'R' {
			GetConfig().EnableTAF=true
			logErr("taf=true")
			os.Setenv("TWO_TASK_STANDBY0", dbName)
		}

		dbName = baseName+"_"+rwSuffix+twoTaskSuffix
		_,ok = tnsEntries[dbName]
		if ok {
			logErr("rw-split=true")
			GetConfig().ReadonlyPct=50
			os.Setenv("TWO_TASK_READ", dbName)
		}

		dbName = baseName+twoTaskSuffix
		_,ok = tnsEntries[dbName]
		if ok || "" == os.Getenv("TWO_TASK") {
			logErr("setting TWO_TASK "+dbName)
			os.Setenv("TWO_TASK", dbName)
		}
	}

	if GetConfig().CfgFromTnsOverrideNumShards != -1 {
		GetConfig().NumOfShards = GetConfig().CfgFromTnsOverrideNumShards
	}
	if GetConfig().CfgFromTnsOverrideTaf != -1 {
		GetConfig().EnableTAF = (GetConfig().CfgFromTnsOverrideTaf == 1)
	}
	if GetConfig().CfgFromTnsOverrideRWSplit != -1 {
		GetConfig().ReadonlyPct = GetConfig().CfgFromTnsOverrideRWSplit
	}
	if GetConfig().EnableTAF {
		InitTAF(GetConfig().NumOfShards)
	}
}

func logErr(msg string) {
	fmt.Println("cfgFromTns",msg)
}

func FindTns() (map[string]string, error) {
	tnsEntries, err := loadTns(os.Getenv("TNS_ADMIN")+"/tnsnames.ora")
	if err != nil {
		logErr("now trying ORACLE_HOME tnsnames")
		tnsEntries, err = loadTns(os.Getenv("ORACLE_HOME")+"/network/admin/tnsnames.ora")
		if err != nil {
			logErr(err.Error())
			return nil, err
		}
		return tnsEntries, err
	}
	return tnsEntries, err
}

func loadTns(tnsFname string) (map[string]string, error) {
	out := make(map[string]string)
	fh, err := os.Open(tnsFname)
	if err != nil {
		return nil,err
	}
	defer fh.Close()
	scanner := bufio.NewScanner(fh)
	parenCnt := 0
	lineCnt := 1
	for scanner.Scan() {
		line := scanner.Text()
		if parenCnt == 0 {
			// try to pick off name
			idx  := strings.Index(line,"=")
			idx2 := strings.Index(line,"<") // substitution delimiter
			if idx2 > 0 && idx2 < idx {
				idx = idx2
			}
			if idx > 0 {
				for ;line[idx-1] == ' ';idx-- {} // trim spaces before =
				name := line[0:idx]
				out[name] = line[idx+1:]
			}
		}
		for i:=0;i<len(line);i++ {
			if line[i] == '(' {
				parenCnt++
			} else if line[i] == ')' {
				parenCnt--
			}
		}
		lineCnt++
	}
	err = scanner.Err()
	if err != nil {
		logErr(err.Error())
		return nil,err
	}
	return out,nil
}

