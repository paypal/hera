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
  "strings"
)

// TableNameParser will remove comment from the sql string and return keyword after FROM or JOIN
func TableNameParser(sql string) []string{
  var result []string
  tokenized := tokenizeSQL(removeComments(sql))
  for i := range tokenized {
    if(strings.Compare(tokenized[i], "FROM") == 0 || strings.Compare(tokenized[i], "JOIN") == 0){
      //grabbing next token by FROM or JOIN
      result = append(result, tokenized[i+1])
    }
  }
  return result
}

// Remove the /* */ comments
func removeComments(sql string) string{
  cmt := regexp.MustCompile(`/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/`)
  return string(cmt.ReplaceAll([]byte(sql), []byte("")))
}
 // Split on blanks, parens, semicolons and commas
func tokenizeSQL(sql string) []string{
  return strings.FieldsFunc(sql, Split)
}

func Split(r rune) bool {
  return r == ' ' || r == '(' || r == ')' || r == ';' || r == ','
}
