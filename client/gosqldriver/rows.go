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

package gosqldriver

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

// Implements sql/driver Rows interface.
// similar to JDBC's result set
// Rows is an iterator over an executed query's results.
type rows struct {
	hera           *heraConnection
	vals           []driver.Value
	cols           int
	colInfo        []string
	currentRow     int
	fetchChunkSize []byte
	completed      bool
}

var columnInfo = []string{"Name", "Type", "Width", "Precision", "Scale"}

// TODO: fetch chunk size
func newRows(hera *heraConnection, cols int, fetchChunkSize []byte) (*rows, error) {
	rs := &rows{hera: hera, cols: cols, currentRow: 0, fetchChunkSize: fetchChunkSize}
	err := rs.fetchResults()
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func (r *rows) fetchResults() error {
	var localVals []driver.Value
	var err error
	var ns *netstring.Netstring
outer:
	for {
		ns, err = r.hera.getResponse()
		if err != nil {
			break outer
		}
		switch ns.Cmd {
		case common.RcValue:
			localVals = append(localVals, ns.Payload)
		case common.RcOK:
			break outer
		case common.RcNoMoreData:
			r.completed = true
			break outer
		}
	}
	//process localVals if length > 0
	if len(localVals) > 0 {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, r.hera.id, "Rows: cols = ", r.cols, ", numValues =", len(localVals))
		}
		if len(localVals) >= (r.cols*5 + 1) {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, r.hera.id, "Column info present in data")
			}
			r.vals = localVals[r.cols*5+1:]
			//Create JSON string for column info
			localVals = localVals[1 : r.cols*5+1]
			for index := 0; index < len(localVals); {
				columnData := localVals[index : index+5] //Each column has column info of size 5
				columnInfoMap := make(map[string]string)
				for index2 := 0; index2 < len(columnData); index2++ {
					switch valType := columnData[index2].(type) {
					case string:
						columnInfoMap[columnInfo[index2]] = valType
					case []byte:
						columnInfoMap[columnInfo[index2]] = string(valType)
					default:
						if logger.GetLogger().V(logger.Verbose) {
							logger.GetLogger().Log(logger.Verbose, r.hera.id, "Invalid data: ", valType)
						}
						columnInfoMap[columnInfo[index2]] = "UNDEFINED"
					}
				}
				columnInfoStr, _ := json.Marshal(columnInfoMap)
				r.colInfo = append(r.colInfo, string(columnInfoStr))
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, r.hera.id, "Column info :", string(columnInfoStr))
				}
				index = index + 5
			}
		} else {
			r.vals = localVals
		}
	}
	return err
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	// TODO using hera column names command
	if len(r.colInfo) > 0 {
		return r.colInfo
	}
	return make([]string, r.cols)
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	return errors.New("Rows.Close() not yet implemented")
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
func (r *rows) Next(dest []driver.Value) error {
	if logger.GetLogger().V(logger.Verbose) {
		//		logger.GetLogger().Log(logger.Verbose, r.hera.id, "Rows.Next(): currentRow = ", r.currentRow, ", numValues =", len(dest))
	}
	if r.cols*r.currentRow == len(r.vals) {
		if r.completed {
			return io.EOF
		}
		// fetch the next rows
		ns := netstring.NewNetstringFrom(common.CmdFetch, r.fetchChunkSize)
		err := r.hera.execNs(ns)
		if err != nil {
			return err
		}
		r.vals = r.vals[:0]
		err = r.fetchResults()
		if err != nil {
			return err
		}
		r.currentRow = 0
		if len(r.vals) == 0 {
			return io.EOF
		}
	}
	if (r.currentRow+1)*r.cols > len(r.vals) {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, fmt.Sprintf("Rows.Next() failed len(r.vals)=%d, cols=%d, currentRow=%d", len(r.vals), r.cols, r.currentRow))
		}
		return fmt.Errorf("Rows.Next() failed len(r.vals)=%d, cols=%d, currentRow=%d", len(r.vals), r.cols, r.currentRow)
	}
	n := copy(dest, r.vals[r.currentRow*r.cols:(r.currentRow+1)*r.cols])
	if n != r.cols {
		return fmt.Errorf("Rows.Next() failed destsize=%d, n=%d, cols=%d, currentRow=%d", len(dest), n, r.cols, r.currentRow)
	}
	r.currentRow++
	return nil
}
