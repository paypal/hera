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

package main

import "testing"

func TestProcessResult(t *testing.T) {
	adapter := &postgresAdapter{}
	t.Log("++++Running TestProcessResult for postgres")
	actual := adapter.ProcessResult("DATE","1996-12-02T00:00:00Z")
	if actual != "02-12-1996 00:00:00.000" {
		t.Errorf("Expected 02-12-1996 00:00:00.000, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIME","0000-01-01T09:00:00Z")
	if actual != "01-01-0000 09:00:00.000" {
		t.Errorf("Expected 01-01-0000 09:00:00.000, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIMESTAMP","2021-09-13T17:10:25Z")
	if actual != "13-09-2021 17:10:25.000" {
		t.Errorf("Expected 13-09-2021 17:10:25.000, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIMESTAMPTZ","2021-09-13T17:10:25Z")
	if actual != "13-09-2021 17:10:25.000" {
		t.Errorf("Expected 13-09-2021 17:10:25.000, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIMESTAMPTZ","2021-09-13T07:10:25.12345Z")
	if actual != "13-09-2021 07:10:25.000" {
		t.Errorf("Expected 13-09-2021 17:10:25.000, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIMETZ","2021-01-01T08:00:00+05:30")
	if actual != "01-01-2021 08:00:00.000 +05:30" {
		t.Errorf("Expected 01-01-2021 08:00:00.000 +05:30, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIMETZ","2021-01-01T08:00:00.123+05:30")
	if actual != "01-01-2021 08:00:00.000 +05:30" {
		t.Errorf("Expected 01-01-2021 08:00:00.000 +05:30, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIMETZ","2021-01-01T08:00:00-07:00")
	if actual != "01-01-2021 08:00:00.000 -07:00" {
		t.Errorf("01-01-2021 08:00:00.000 -07:00, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIMETZ","2021-01-01T08:00:00.123-07:00")
	if actual != "01-01-2021 08:00:00.000 -07:00" {
		t.Errorf("Expected 01-01-2021 08:00:00.000 -07:00, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIMETZ","2021-01-01T08:00:00+00:00")
	if actual != "01-01-2021 08:00:00.000 +00:00" {
		t.Errorf("01-01-2021 08:00:00.000 +00:00, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIMETZ","2021-01-01T08:00:00.002+00:00")
	if actual != "01-01-2021 08:00:00.000 +00:00" {
		t.Errorf("01-01-2021 08:00:00.000 +00:00, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIMETZ","2021-09-13T17:10:25Z")
	if actual != "13-09-2021 17:10:25.000 +00:00" {
		t.Errorf("13-09-2021 17:10:25.000 +00:00, but got %v", actual)
	}
	actual = adapter.ProcessResult("TIMETZ","2021-01-01T08:00:00Z-07:00")
	if actual != "01-01-2021 08:00:00.000 +00:00" {
		t.Errorf("01-01-2021 08:00:00.000 +00:00, but got %v", actual)
	}
	t.Log("----Done TestProcessResult for postgres")
}
