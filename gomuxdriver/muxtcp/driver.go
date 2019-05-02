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

// Package muxtcp provides the Mux driver for Go's database/sql package.
//
// The driver should be used via the database/sql package:
//
//  import "database/sql"
//  import _ "github.com/paypal/hera/gomuxdriver/muxtcp"
//
//  db, err := sql.Open("occ", "1:<ip>:<port>")
package muxtcp

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/gomuxdriver"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
	"net"
	"os"
)

type occDriver struct {
}

var drv *occDriver

func init() {
	drv = &occDriver{}
	sql.Register("occ", drv)
}

func (driver *occDriver) Open(url string) (driver.Conn, error) {
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "Dialing to occ server:", url)
	}
	conn, err := net.Dial("tcp", url)

	if err != nil {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "Error connecting", err, " connecting to", url)
		}
		return nil, err
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Connected to occ server:", url)
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

	return gomuxdriver.NewOccConnection(conn), nil
}
