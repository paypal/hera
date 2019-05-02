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
	"errors"
	"fmt"

	"github.com/paypal/hera/common"
)

// implements sql/driver Tx interface
type tx struct {
	hera *heraConnection
}

func (t *tx) cmd(cmd int) error {
	if t.hera == nil {
		return errors.New("Invalid connection")
	}
	hera := t.hera
	t.hera = nil
	err := hera.exec(cmd, nil)
	if err != nil {
		return err
	}
	ns, err := hera.getResponse()
	if err != nil {
		return err
	}
	if ns.Cmd != common.RcOK {
		return fmt.Errorf("Got error=%d", ns.Cmd)
	}
	return nil
}

func (t *tx) Commit() error {
	return t.cmd(common.CmdCommit)
}

func (t *tx) Rollback() error {
	return t.cmd(common.CmdRollback)
}
