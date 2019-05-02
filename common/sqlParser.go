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
	"regexp"
)

// SQLParser is the interface grouping SQL parsing functions.
//
// IsRead tells is the SQL is doing a read, basically a SELECT but not a SELECT ... FOR UPDATE or nextval.
// Parse tells if the SQL is a select and if the SQL starts a transaction
type SQLParser interface {
	IsRead(string) bool
	Parse(sql string) (isSelect bool, transaction bool)
}

type regexSQLParser struct {
	matcher          *regexp.Regexp
	matcherForUpdate *regexp.Regexp
}

type dummyParser struct {
}

// NewRegexSQLParser creates a SQL parser based on regex
func NewRegexSQLParser() (SQLParser, error) {
	parser := &regexSQLParser{}
	var err error
	parser.matcher, err = regexp.Compile("(?i)^\\s*(/\\*.*\\*/)*\\s*select\\s+")
	if err != nil {
		return nil, err
	}
	parser.matcherForUpdate, err = regexp.Compile("(?i)^\\s*(/\\*.*\\*/)*\\s*select\\s+.*((for\\s+update(\\s|$))|(nextval(\\s|$)))")
	if err != nil {
		return nil, err
	}
	return parser, nil
}

// IsRead tells is the SQL is doing a read, basically a SELECT but not a SELECT ... FOR UPDATE or nextval
// SQL parser using two passes, first to check if it is a "SELECT", second to check that it is not
// a "SELECT .. FOR UPDATE" or "SELECT sequence.NEXTVAL from dual"
// Note: to have one pass parser we probably need to do it manually, since the implementation doesn't support lookaround
func (parser *regexSQLParser) IsRead(sql string) bool {
	if parser.matcher.MatchString(sql) {
		if parser.matcherForUpdate.MatchString(sql) {
			return false
		}
		return true
	}
	return false
}

// Parse a SQL and returns:
// - first return code tells if the query is a SELECT
// - second returns code tells the query starts a transaction, which is if the query is not a select or it is a select ... for update
func (parser *regexSQLParser) Parse(sql string) (bool, bool) {
	if parser.matcher.MatchString(sql) {
		if parser.matcherForUpdate.MatchString(sql) {
			return true, true
		}
		return true, false
	}
	return false, true
}

// NewDummyParser crestes a parser that always returns false
func NewDummyParser() SQLParser {
	return &dummyParser{}
}

func (parser *dummyParser) IsRead(sql string) bool {
	return false
}

func (parser *dummyParser) Parse(sql string) (bool, bool) {
	return false, false
}
