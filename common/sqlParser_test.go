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

package common

import (
	"testing"
)

const (
	selectStr = "select"
	updateStr = "update"
	selForUpdateStr = "select for update"
)

func TestSQLParser(t *testing.T) {
	parser, err := NewRegexSQLParser()
	t.Log("++++Running TestSQLParser")
	if err != nil {
		t.Error("Fail to create the parser: " + err.Error())
	}
	if !parser.IsRead("select foo from bar") {
		t.Error(selectStr)
	}
	if !parser.IsRead("/*select foo from bar for update*/select foo from bar") {
		t.Error(selectStr)
	}
	if parser.IsRead("update foo set bar='5'") {
		t.Error(updateStr)
	}
	if parser.IsRead("/* select */update foo set bar='5'") {
		t.Error(updateStr)
	}
	if parser.IsRead("select foo from bar for update") {
		t.Error(selForUpdateStr)
	}
	if parser.IsRead("select foo from bar for update    ") {
		t.Error(selForUpdateStr)
	}
	if !parser.IsRead("select foo from bar for updateX") {
		t.Error(selForUpdateStr)
	}
	if parser.IsRead("select seq1.nextvaL from dual") {
		t.Error("nextval")
	}
	if parser.IsRead("select myseq.Nextval from dual") {
		t.Error("nextval")
	}
	t.Log("----Done TestSQLParser")
}

func TestDumySQLParser(t *testing.T) {
	parser := NewDummyParser()
	t.Log("++++Running TestSQLParser")
	if parser.IsRead("select foo from bar") {
		t.Error(selectStr)
	}
	// dumy parser always return false
	if parser.IsRead("update foo set bar='5'") {
		t.Error(updateStr)
	}
	t.Log("----Done TestSQLParser")
}
