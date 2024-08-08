package gosqldriver

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
)

type mockHeraConnection struct {
	heraConnection
	responses []netstring.Netstring
	execErr   error
}

func (m *mockHeraConnection) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
}

func (m *mockHeraConnection) Close() error {
	return nil
}

func (m *mockHeraConnection) Begin() (driver.Tx, error) {
	return nil, nil
}

func (m *mockHeraConnection) exec(cmd int, payload []byte) error {
	return m.execErr
}

func (m *mockHeraConnection) execNs(ns *netstring.Netstring) error {
	return m.execErr
}

func (m *mockHeraConnection) getResponse() (*netstring.Netstring, error) {
	if len(m.responses) == 0 {
		return &netstring.Netstring{}, io.EOF
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	return &response, nil
}

func (m *mockHeraConnection) SetShardID(shard int) error {
	return nil
}

func (m *mockHeraConnection) ResetShardID() error {
	return nil
}

func (m *mockHeraConnection) GetNumShards() (int, error) {
	return 0, nil
}

func (m *mockHeraConnection) SetShardKeyPayload(payload string) {
}

func (m *mockHeraConnection) ResetShardKeyPayload() {
}

func (m *mockHeraConnection) SetCalCorrID(corrID string) {
}

func (m *mockHeraConnection) SetClientInfo(poolName string, host string) error {
	return nil
}

func (m *mockHeraConnection) SetClientInfoWithPoolStack(poolName string, host string, poolStack string) error {
	return nil
}

func (m *mockHeraConnection) getID() string {
	return "mockID"
}

func TestNewRows(t *testing.T) {
	mockHera := &mockHeraConnection{
		responses: []netstring.Netstring{
			{Cmd: common.RcValue, Payload: []byte("value1")},
			{Cmd: common.RcValue, Payload: []byte("value2")},
			{Cmd: common.RcOK},
		},
	}

	cols := 2
	fetchChunkSize := []byte("10")
	rows, err := newRows(mockHera, cols, fetchChunkSize)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if rows == nil {
		t.Fatalf("expected rows to be non-nil")
	}
	if rows.cols != cols {
		t.Errorf("expected cols to be %d, got %d", cols, rows.cols)
	}
	if rows.currentRow != 0 {
		t.Errorf("expected currentRow to be 0, got %d", rows.currentRow)
	}
	if string(rows.fetchChunkSize) != string(fetchChunkSize) {
		t.Errorf("expected fetchChunkSize to be %s, got %s", fetchChunkSize, rows.fetchChunkSize)
	}
}

func TestColumns(t *testing.T) {
	mockHera := &mockHeraConnection{}
	rows := &rows{hera: mockHera, cols: 3}

	columns := rows.Columns()
	if len(columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(columns))
	}
	for _, col := range columns {
		if col != "" {
			t.Errorf("expected column name to be empty, got %s", col)
		}
	}
}

// TODO: Change unit test for Close() once it has been implemented
func TestClose(t *testing.T) {
	mockHera := &mockHeraConnection{}
	rows := &rows{hera: mockHera}

	err := rows.Close()
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	expectedErr := "Rows.Close() not yet implemented"
	if err.Error() != expectedErr {
		t.Errorf("expected error to be %s, got %s", expectedErr, err.Error())
	}
}

func TestNext(t *testing.T) {
	mockHera := &mockHeraConnection{
		responses: []netstring.Netstring{
			{Cmd: common.RcValue, Payload: []byte("value1")},
			{Cmd: common.RcValue, Payload: []byte("value2")},
			{Cmd: common.RcNoMoreData},
		},
	}
	rows, err := newRows(mockHera, 2, []byte("10"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	dest := make([]driver.Value, 2)
	err = rows.Next(dest)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	expectedDest := []driver.Value{[]byte("value1"), []byte("value2")}
	for i := range dest {
		if string(dest[i].([]byte)) != string(expectedDest[i].([]byte)) {
			t.Errorf("expected dest[%d] to be %s, got %s", i, expectedDest[i], dest[i])
		}
	}

	err = rows.Next(dest)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF error, got %v", err)
	}
}
