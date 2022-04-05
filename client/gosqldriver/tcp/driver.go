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

// Package tcp provides the Hera driver for Go's database/sql package.
//
// The driver should be used via the database/sql package:
//
//  import "database/sql"
//  import _ "github.com/paypal/hera/client/gosqldriver/tcp"
//
//  db, err := sql.Open("hera", "1:<ip>:<port>")
package tcp

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/paypal/hera/client/gosqldriver"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

type heraDriver struct {
}

var drv *heraDriver

func init() {
	drv = &heraDriver{}
	sql.Register("hera", drv)
}

func (driver *heraDriver) Open(url string) (driver.Conn, error) {
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "Dialing to hera server:", url)
	}
	conn, err := net.Dial("tcp", url)

	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Error connecting", err, " connecting to", url)
		}
		return nil, err
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Connected to hera server:", url)
	}

       reader := netstring.NewNetstringReader(conn)

	// send client info
	pid := os.Getpid()
	host, _ := os.Hostname()
	helloCmd := netstring.NewNetstringFrom(common.CmdClientInfo, []byte(fmt.Sprintf("PID: %d,HOST: %s, EXEC: %d@%s, Poolname: unset, Command: init, null, Name: GO_driver", pid, host, pid, host)))

	_, err = conn.Write(helloCmd.Serialized)
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Failed to send client info")
		}
		return nil, errors.New("Failed custom auth, failed to send client info")
	}
	ns, err := reader.ReadNext()
	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Failed to read server info")
		}
		return nil, errors.New("Failed to read server info")
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Server info:", string(ns.Payload))
	}

	return gosqldriver.NewHeraConnection(conn), nil
}
