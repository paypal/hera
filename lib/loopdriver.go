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

package lib

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/gomuxdriver"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
	"net"
	"strings"
)

/*
 * database/sql driver to be used internally by occ. That way components reading the configration from the database
 * (like sharding configuration for example) can be coded in standard GO SQL.
 * It is simply a wrapper over mux/gomuxdriver
 */
type occLoopDriver struct {
}

// ConnHandlerFunc defines the signature of a fucntion that can be used as a callback by the loop driver
type ConnHandlerFunc func(net.Conn)

var connHandler ConnHandlerFunc

// RegisterLoopDriver installs the callback for the loop driver
func RegisterLoopDriver(f ConnHandlerFunc) {
	connHandler = f
	drvLoop := &occLoopDriver{}
	sql.Register("occloop", drvLoop)
}

/**
URL: <ShardID>:<PoolType>:<PoolID>
TODO: add another parameter for debugging/troubleshooting, IDing the client
*/
func (driver *occLoopDriver) Open(url string) (driver.Conn, error) {
	cli, srv := net.Pipe()
	go connHandler(srv)

	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "OCC loop driver driver, opening", url, ": ", cli)
	}
	if len(url) > 0 {
		// now set the shard ID
		fields := strings.Split(url, ":")
		if (len(fields) == 3) && (GetConfig().EnableSharding) {
			ns := netstring.NewNetstringFrom(common.CmdSetShardID, []byte(fields[0]))
			cli.Write(ns.Serialized)
			ns, err := netstring.NewNetstring(cli)
			if err != nil {
				return nil, fmt.Errorf("Failed to set shardID: %s", err.Error())
			}
			if ns.Cmd != common.RcOK {
				return nil, fmt.Errorf("OCC_SET_SHARD_ID response: %s", string(ns.Serialized))
			}
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "OCC loop driver driver, opened to shard", fields[0])
			}
		}
	}
	return gomuxdriver.NewOccConnection(cli), nil
}
