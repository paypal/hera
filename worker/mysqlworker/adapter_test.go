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
	"errors"
	"log"
	"regexp"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/paypal/hera/worker/shared"
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

func TestProcessError(t *testing.T) {
	adapter := &mysqlAdapter{}
	queryScope := &shared.QueryScopeType{
		SqlHash: "test_sql_hash",
		NsCmd:   "test_cmd",
	}

	tests := []struct {
		name                 string
		err                  error
		expectedShutdownFlag bool
	}{
		{
			name:                 "Issue 77 missing bind value parameter error",
			err:                  errors.New("sql: expected 2 arguments, got 1"),
			expectedShutdownFlag: false,
		},
		{
			name:                 "Short error with early colon",
			err:                  errors.New("err: unexpected argument"),
			expectedShutdownFlag: false,
		},
		{
			name:                 "Driver bad connection error",
			err:                  errors.New("driver: bad connection"),
			expectedShutdownFlag: true,
		},
		{
			name:                 "Fatal MySQL packet too large string error",
			err:                  errors.New("Error 1153: Got a packet bigger than 'max_allowed_packet' bytes"),
			expectedShutdownFlag: true,
		},
		{
			name:                 "Non-fatal MySQL syntax string error",
			err:                  errors.New("Error 1064: You have an error in your SQL syntax"),
			expectedShutdownFlag: false,
		},
		{
			name: "Typed fatal MySQLError",
			err: &mysql.MySQLError{
				Number:  1153,
				Message: "Got a packet bigger than 'max_allowed_packet' bytes",
			},
			expectedShutdownFlag: true,
		},
		{
			name: "Typed non-fatal MySQLError",
			err: &mysql.MySQLError{
				Number:  1064,
				Message: "Syntax error",
			},
			expectedShutdownFlag: false,
		},
		{
			name:                 "Arbitrary error string without colon or error code",
			err:                  errors.New("unknown general failure"),
			expectedShutdownFlag: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			workerScope := &shared.WorkerScopeType{}
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("ProcessError panicked unexpectedly for error %q: %v", tc.err, r)
				}
			}()

			adapter.ProcessError(tc.err, workerScope, queryScope)
			if workerScope.Child_shutdown_flag != tc.expectedShutdownFlag {
				t.Errorf("ProcessError(%q): expected Child_shutdown_flag = %v, got %v",
					tc.err, tc.expectedShutdownFlag, workerScope.Child_shutdown_flag)
			}
		})
	}
}
