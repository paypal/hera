// Copyright 2026 PayPal Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shared

import (
	"database/sql"
	"regexp"
	"testing"

	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
)

type mockAdapter struct {
	useBindNames bool
	useQuestion  bool
}

func (m *mockAdapter) MakeSqlParser() (common.SQLParser, error) {
	return common.NewRegexSQLParser()
}
func (m *mockAdapter) GetColTypeMap() map[string]int {
	return map[string]int{"INT": 3, "VARCHAR": 5}
}
func (m *mockAdapter) Heartbeat(db *sql.DB) bool { return true }
func (m *mockAdapter) InitDB() (*sql.DB, error)  { return nil, nil }
func (m *mockAdapter) ProcessError(err error, w *WorkerScopeType, q *QueryScopeType) {}
func (m *mockAdapter) ProcessResult(colType string, res string) string { return res }
func (m *mockAdapter) UseBindNames() bool        { return m.useBindNames }
func (m *mockAdapter) UseBindQuestionMark() bool { return m.useQuestion }

func newTestCmdProcessor(useBindNames bool, useQuestion bool) *CmdProcessor {
	cp := &CmdProcessor{
		adapter:       &mockAdapter{useBindNames: useBindNames, useQuestion: useQuestion},
		regexBindName: regexp.MustCompile(":([`]?[a-zA-Z])\\w*[`]?"),
		bindVars:      make(map[string]*BindValue),
	}
	return cp
}

func TestPreprocessCaseInsensitiveBinds(t *testing.T) {
	cp := newTestCmdProcessor(false, true)
	query := "insert into test_simple_table_2 (accountID, Name, Status) VALUES(:accountID, :Name, :Status)"
	transformed := cp.preprocess(query)

	expected := "insert into test_simple_table_2 (accountID, Name, Status) VALUES(?, ?, ?)"
	if transformed != expected {
		t.Fatalf("expected %q, got %q", expected, transformed)
	}

	expectedKeys := []string{":accountid", ":name", ":status"}
	for _, k := range expectedKeys {
		bv, ok := cp.bindVars[k]
		if !ok || bv == nil {
			t.Fatalf("expected bindVars key %q to exist", k)
		}
	}

	// Verify original case is preserved in BindValue.name
	if cp.bindVars[":accountid"].name != ":accountID" {
		t.Errorf("expected original name ':accountID', got %q", cp.bindVars[":accountid"].name)
	}
	if cp.bindVars[":name"].name != ":Name" {
		t.Errorf("expected original name ':Name', got %q", cp.bindVars[":name"].name)
	}
	if cp.bindVars[":status"].name != ":Status" {
		t.Errorf("expected original name ':Status', got %q", cp.bindVars[":status"].name)
	}
}

func TestPreprocessDuplicateBindsDifferentCasing(t *testing.T) {
	cp := newTestCmdProcessor(false, true)
	query := "SELECT * FROM t WHERE status = :Status OR status = :status"
	transformed := cp.preprocess(query)

	expected := "SELECT * FROM t WHERE status = ? OR status = ?"
	if transformed != expected {
		t.Fatalf("expected %q, got %q", expected, transformed)
	}

	// Should deduplicate in bindVars map under lowercase key
	if len(cp.bindVars) != 1 {
		t.Fatalf("expected 1 unique bind in bindVars, got %d", len(cp.bindVars))
	}
	if _, ok := cp.bindVars[":status"]; !ok {
		t.Fatalf("expected ':status' in bindVars")
	}

	// Both positions should reference :status
	if len(cp.bindPos) != 2 || cp.bindPos[0] != ":status" || cp.bindPos[1] != ":status" {
		t.Fatalf("expected bindPos to have 2 entries for :status, got %v", cp.bindPos)
	}
}

func TestProcessCmdBindNameCaseInsensitive(t *testing.T) {
	cp := newTestCmdProcessor(false, true)
	cp.preprocess("SELECT * FROM t WHERE accountID=:accountID AND status=:Status")
	// simulate prepared stmt active
	cp.stmt = &sql.Stmt{}

	cases := []struct {
		input       string
		expectedKey string
		shouldErr   bool
	}{
		{input: "accountID", expectedKey: ":accountid", shouldErr: false},
		{input: ":accountid", expectedKey: ":accountid", shouldErr: false},
		{input: "ACCOUNTID", expectedKey: ":accountid", shouldErr: false},
		{input: "status", expectedKey: ":status", shouldErr: false},
		{input: ":Status", expectedKey: ":status", shouldErr: false},
		{input: ":STATUS", expectedKey: ":status", shouldErr: false},
		{input: "nonexistent", expectedKey: "", shouldErr: true},
	}

	for _, tc := range cases {
		ns := netstring.NewNetstringFrom(common.CmdBindName, []byte(tc.input))
		err := cp.ProcessCmd(ns)
		if tc.shouldErr {
			if err == nil {
				t.Errorf("expected error for input %q, got nil", tc.input)
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error for input %q: %v", tc.input, err)
			}
			if cp.currentBindName != tc.expectedKey {
				t.Errorf("expected currentBindName %q for input %q, got %q", tc.expectedKey, tc.input, cp.currentBindName)
			}
			if cp.bindVars[cp.currentBindName] == nil {
				t.Errorf("expected bindVars[%q] to exist", cp.currentBindName)
			}
		}
	}
}
