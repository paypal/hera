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

package main

import (
	"log"
	"regexp"
	"testing"
)

func TestExtractAndReplaceBindVar(t *testing.T) {
	query := "SELECT account_number,flags,return_url,time_created,identity_token FROM wseller WHERE account_number=:account_number and flags=:flags and return_url = :return_url,"
	re := regexp.MustCompile(":\\w+")
	binds := re.FindAllString(query, -1)
	for i, val := range binds {
		log.Println("bindname", i, val)
	}
	log.Println(re.ReplaceAllString(query, "?"))

	query = "SELECT account_number,flags,return_url,time_created,identity_token FROM wseller WHERE account_number=:account_number and flags=:flags and return_url = :`return_url`,"
	binds = re.FindAllString(query, -1)
	for i, val := range binds {
		log.Println("bindname", i, val)
	}
	log.Println(re.ReplaceAllString(query, "?"))

	query = "/* WSeller--load */SELECT wseller.account_number,wseller.flags,wseller.return_url,wseller.time_created,wseller.identity_token FROM wseller WHERE wseller.account_number=:account_number"
	binds = re.FindAllString(query, -1)
	for i, val := range binds {
		log.Println("bindname", i, val)
	}
	log.Println(re.ReplaceAllString(query, "?$1"))
}
