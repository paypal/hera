package gosqldriver

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
)

func (m *mockHeraConnection) getCorrID() *netstring.Netstring {
	return m.corrID
}

func (m *mockHeraConnection) setCorrID(corrID *netstring.Netstring) {
	m.corrID = corrID
}

func (m *mockHeraConnection) getShardKeyPayload() []byte {
	return make([]byte, 1)
}

func (m *mockHeraConnection) finish() {
	m.finishCalled = true
}

func TestNewStmt(t *testing.T) {
	mockHera := &mockHeraConnection{}

	tests := []struct {
		sql         string
		expectedSQL string
	}{
		{"SELECT * FROM table WHERE col1 = ? AND col2 = ?", "SELECT * FROM table WHERE col1 = :p1 AND col2 = :p2"},
		{"INSERT INTO table (col1, col2) VALUES (?, ?)", "INSERT INTO table (col1, col2) VALUES (:p1, :p2)"},
		{"UPDATE table SET col1 = ? WHERE col2 = ?", "UPDATE table SET col1 = :p1 WHERE col2 = :p2"},
	}

	for _, test := range tests {
		st := newStmt(mockHera, test.sql)
		if st.sql != test.expectedSQL {
			t.Errorf("expected SQL to be %s, got %s", test.expectedSQL, st.sql)
		}
		if string(st.fetchChunkSize) != "0" {
			t.Errorf("expected fetchChunkSize to be '0', got %s", st.fetchChunkSize)
		}
	}
}

func TestStmtClose(t *testing.T) {
	mockHera := &mockHeraConnection{}
	st := newStmt(mockHera, "SELECT * FROM table")

	err := st.Close()
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	expectedErr := "stmt.Close() not implemented"
	if err.Error() != expectedErr {
		t.Errorf("expected error to be %s, got %s", expectedErr, err.Error())
	}
}

func TestStmtNumInput(t *testing.T) {
	mockHera := &mockHeraConnection{}
	st := newStmt(mockHera, "SELECT * FROM table")

	numInput := st.NumInput()
	expectedNumInput := -1
	if numInput != expectedNumInput {
		t.Errorf("expected NumInput to be %d, got %d", expectedNumInput, numInput)
	}
}

func TestStmtExec(t *testing.T) {
	tests := []struct {
		name          string
		args          []driver.Value
		setupMock     func(*mockHeraConnection)
		expectedError string
		expectedRows  int
	}{
		{
			name: "Exec with no args",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("1")},
					{Cmd: common.RcValue, Payload: []byte("1")},
				}
			},
			expectedError: "",
			expectedRows:  1,
		},
		{
			name: "Exec with int args",
			args: []driver.Value{1},
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("1")},
					{Cmd: common.RcValue, Payload: []byte("1")},
				}
			},
			expectedError: "",
			expectedRows:  1,
		},
		{
			name: "Exec with string args",
			args: []driver.Value{"test"},
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("1")},
					{Cmd: common.RcValue, Payload: []byte("1")},
				}
			},
			expectedError: "",
			expectedRows:  1,
		},
		{
			name: "Exec with shard key",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.shardKeyPayload = []byte("shard_key")
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("1")},
					{Cmd: common.RcValue, Payload: []byte("1")},
				}
			},
			expectedError: "",
			expectedRows:  1,
		},
		{
			name: "Exec with SQL error",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcSQLError, Payload: []byte("SQL error")},
				}
			},
			expectedError: "SQL error: SQL error",
			expectedRows:  0,
		},
		{
			name: "Exec with internal error",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcError, Payload: []byte("Internal error")},
				}
			},
			expectedError: "Internal hera error: Internal error",
			expectedRows:  0,
		},
		{
			name: "Exec with unknown code",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: 999, Payload: []byte("Unknown code")},
				}
			},
			expectedError: "Unknown code: 999, data: Unknown code",
			expectedRows:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHera := &mockHeraConnection{}
			if tt.setupMock != nil {
				tt.setupMock(mockHera)
			}

			st := &stmt{
				hera: mockHera,
				sql:  "INSERT INTO table (col1) VALUES (?)",
			}

			result, err := st.Exec(tt.args)
			if tt.expectedError != "" {
				if err == nil || err.Error() != tt.expectedError {
					t.Fatalf("expected error %v, got %v", tt.expectedError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if result == nil {
				t.Fatalf("expected result, got nil")
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				t.Fatalf("expected no error on RowsAffected, got %v", err)
			}
			if rowsAffected != int64(tt.expectedRows) {
				t.Errorf("expected %d rows affected, got %d", tt.expectedRows, rowsAffected)
			}
		})
	}
}

func TestStmtExecContext(t *testing.T) {
	tests := []struct {
		name          string
		args          []driver.NamedValue
		setupMock     func(*mockHeraConnection)
		expectedError string
		expectedRows  int
		cancelCtx     bool
	}{
		{
			name: "ExecContext with no args",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("1")},
					{Cmd: common.RcValue, Payload: []byte("1")},
				}
			},
			expectedError: "",
			expectedRows:  1,
		},
		{
			name: "ExecContext with int args",
			args: []driver.NamedValue{{Ordinal: 1, Value: 1}},
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("1")},
					{Cmd: common.RcValue, Payload: []byte("1")},
				}
			},
			expectedError: "",
			expectedRows:  1,
		},
		{
			name: "ExecContext with string args",
			args: []driver.NamedValue{{Ordinal: 1, Value: "test"}},
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("1")},
					{Cmd: common.RcValue, Payload: []byte("1")},
				}
			},
			expectedError: "",
			expectedRows:  1,
		},
		{
			name: "ExecContext with shard key",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.shardKeyPayload = []byte("shard_key")
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("1")},
					{Cmd: common.RcValue, Payload: []byte("1")},
				}
			},
			expectedError: "",
			expectedRows:  1,
		},
		{
			name: "ExecContext with SQL error",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcSQLError, Payload: []byte("SQL error")},
				}
			},
			expectedError: "SQL error: SQL error",
			expectedRows:  0,
		},
		{
			name: "ExecContext with internal error",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcError, Payload: []byte("Internal error")},
				}
			},
			expectedError: "Internal hera error: Internal error",
			expectedRows:  0,
		},
		{
			name: "ExecContext with unknown code",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: 999, Payload: []byte("Unknown code")},
				}
			},
			expectedError: "Unknown code: 999, data: Unknown code",
			expectedRows:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHera := &mockHeraConnection{}
			if tt.setupMock != nil {
				tt.setupMock(mockHera)
			}

			st := &stmt{
				hera: mockHera,
				sql:  "INSERT INTO table (col1) VALUES (?)",
			}

			ctx := context.Background()
			if tt.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			result, err := st.ExecContext(ctx, tt.args)
			if tt.expectedError != "" {
				if err == nil || err.Error() != tt.expectedError {
					t.Fatalf("expected error %v, got %v", tt.expectedError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if result == nil {
				t.Fatalf("expected result, got nil")
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				t.Fatalf("expected no error on RowsAffected, got %v", err)
			}
			if rowsAffected != int64(tt.expectedRows) {
				t.Errorf("expected %d rows affected, got %d", tt.expectedRows, rowsAffected)
			}
		})
	}
}

func TestStmtQuery(t *testing.T) {
	tests := []struct {
		name          string
		args          []driver.Value
		setupMock     func(*mockHeraConnection)
		expectedError string
		expectedCols  int
	}{
		{
			name: "Query with no args",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("2")}, // number of columns
					{Cmd: common.RcValue, Payload: []byte("1")}, // number of row
					{Cmd: common.RcValue, Payload: []byte("row1")},
					{Cmd: common.RcNoMoreData, Payload: []byte("")},
				}
			},
			expectedError: "",
			expectedCols:  2,
		},
		{
			name: "Query with int args",
			args: []driver.Value{1},
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("2")}, // number of columns
					{Cmd: common.RcValue, Payload: []byte("1")}, // number of rows
					{Cmd: common.RcValue, Payload: []byte("row1")},
					{Cmd: common.RcNoMoreData, Payload: []byte("")},
				}
			},
			expectedError: "",
			expectedCols:  2,
		},
		{
			name: "Query with string args",
			args: []driver.Value{"test"},
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("2")}, // number of columns
					{Cmd: common.RcValue, Payload: []byte("1")}, // number of rows
					{Cmd: common.RcValue, Payload: []byte("row1")},
					{Cmd: common.RcNoMoreData, Payload: []byte("")},
				}
			},
			expectedError: "",
			expectedCols:  2,
		},
		{
			name: "Query with shard key",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.shardKeyPayload = []byte("shard_key")
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("2")}, // number of columns
					{Cmd: common.RcValue, Payload: []byte("1")}, // number of rows
					{Cmd: common.RcValue, Payload: []byte("row1")},
					{Cmd: common.RcNoMoreData, Payload: []byte("")},
				}
			},
			expectedError: "",
			expectedCols:  2,
		},
		{
			name: "Query with SQL error",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcSQLError, Payload: []byte("SQL error")},
				}
			},
			expectedError: "SQL error: SQL error",
			expectedCols:  0,
		},
		{
			name: "Query with internal error",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcError, Payload: []byte("Internal error")},
				}
			},
			expectedError: "Internal hera error: Internal error",
			expectedCols:  0,
		},
		{
			name: "Query with unknown code",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: 999, Payload: []byte("Unknown code")},
				}
			},
			expectedError: "Unknown code: 999, data: Unknown code",
			expectedCols:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHera := &mockHeraConnection{}
			if tt.setupMock != nil {
				tt.setupMock(mockHera)
			}

			st := &stmt{
				hera: mockHera,
				sql:  "SELECT * FROM table WHERE col1 = ?",
			}

			rows, err := st.Query(tt.args)
			if tt.expectedError != "" {
				if err == nil || err.Error() != tt.expectedError {
					t.Fatalf("expected error %v, got %v", tt.expectedError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if rows == nil {
				t.Fatalf("expected rows, got nil")
			}

			columns := rows.Columns()
			if len(columns) != tt.expectedCols {
				t.Errorf("expected %d columns, got %d", tt.expectedCols, len(columns))
			}
		})
	}
}

func TestStmtQueryContext(t *testing.T) {
	tests := []struct {
		name          string
		args          []driver.NamedValue
		setupMock     func(*mockHeraConnection)
		expectedError string
		expectedCols  int
		cancelCtx     bool
	}{
		{
			name: "QueryContext with no args",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("2")}, // number of columns
					{Cmd: common.RcValue, Payload: []byte("1")}, // number of rows
					{Cmd: common.RcValue, Payload: []byte("row1")},
					{Cmd: common.RcNoMoreData, Payload: []byte("")},
				}
			},
			expectedError: "",
			expectedCols:  2,
		},
		{
			name: "QueryContext with int args",
			args: []driver.NamedValue{{Ordinal: 1, Value: 1}},
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("2")}, // number of columns
					{Cmd: common.RcValue, Payload: []byte("1")}, // number of rows
					{Cmd: common.RcValue, Payload: []byte("row1")},
					{Cmd: common.RcNoMoreData, Payload: []byte("")},
				}
			},
			expectedError: "",
			expectedCols:  2,
		},
		{
			name: "QueryContext with string args",
			args: []driver.NamedValue{{Ordinal: 1, Value: "test"}},
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("2")}, // number of columns
					{Cmd: common.RcValue, Payload: []byte("1")}, // number of rows
					{Cmd: common.RcValue, Payload: []byte("row1")},
					{Cmd: common.RcNoMoreData, Payload: []byte("")},
				}
			},
			expectedError: "",
			expectedCols:  2,
		},
		{
			name: "QueryContext with shard key",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.shardKeyPayload = []byte("shard_key")
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("2")}, // number of columns
					{Cmd: common.RcValue, Payload: []byte("1")}, // number of rows
					{Cmd: common.RcValue, Payload: []byte("row1")},
					{Cmd: common.RcNoMoreData, Payload: []byte("")},
				}
			},
			expectedError: "",
			expectedCols:  2,
		},
		{
			name: "QueryContext with SQL error",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcSQLError, Payload: []byte("SQL error")},
				}
			},
			expectedError: "SQL error: SQL error",
			expectedCols:  0,
		},
		{
			name: "QueryContext with internal error",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcError, Payload: []byte("Internal error")},
				}
			},
			expectedError: "Internal hera error: Internal error",
			expectedCols:  0,
		},
		{
			name: "QueryContext with unknown code",
			args: nil,
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: 999, Payload: []byte("Unknown code")},
				}
			},
			expectedError: "Unknown code: 999, data: Unknown code",
			expectedCols:  0,
		},
		{
			name: "QueryContext with cancelled context",
			args: []driver.NamedValue{{Ordinal: 1, Value: 1}},
			setupMock: func(mock *mockHeraConnection) {
				mock.responses = []netstring.Netstring{
					{Cmd: common.RcValue, Payload: []byte("2")},
				}
			},
			cancelCtx:     true,
			expectedError: "context canceled",
			expectedCols:  0,
		},
		{
			name: "QueryContext with execNs failure",
			args: []driver.NamedValue{{Ordinal: 1, Value: 1}},
			setupMock: func(mock *mockHeraConnection) {
				mock.execErr = errors.New("mock execNs error")
			},
			expectedError: "mock execNs error",
			expectedCols:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHera := &mockHeraConnection{}
			if tt.setupMock != nil {
				tt.setupMock(mockHera)
			}

			st := &stmt{
				hera: mockHera,
				sql:  "SELECT * FROM table WHERE col1 = ?",
			}

			ctx := context.Background()
			if tt.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			rows, err := st.QueryContext(ctx, tt.args)
			if tt.expectedError != "" {
				if err == nil || err.Error() != tt.expectedError {
					t.Fatalf("expected error %v, got %v", tt.expectedError, err)
				}
				if mockHera.execErr != nil && !mockHera.finishCalled {
					t.Fatalf("expected finish() to be called, but it was not")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if rows == nil {
				t.Fatalf("expected rows, got nil")
			}

			columns := rows.Columns()
			if len(columns) != tt.expectedCols {
				t.Errorf("expected %d columns, got %d", tt.expectedCols, len(columns))
			}
		})
	}
}

func TestStmtSetFetchSize(t *testing.T) {
	st := &stmt{}
	st.SetFetchSize(10)

	expected := "10"
	if string(st.fetchChunkSize) != expected {
		t.Errorf("expected fetchChunkSize to be %s, got %s", expected, st.fetchChunkSize)
	}
}
