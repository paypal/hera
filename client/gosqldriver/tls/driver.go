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

// Package tls provides the Hera driver for Go's database/sql package.
//
// The driver should be used via the database/sql package:
//
//	import "database/sql"
//	import _ "github.com/paypal/hera/client/gosqldriver/tls"
//
//	db, err := sql.Open("hera", "1:<ip>:<port>")
package tls

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/paypal/hera/client/gosqldriver"
	"github.com/paypal/hera/common"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

type heraDriver struct {
	TLSCfg           *tls.Config
	Ssl              bool
	EncryptedAuthKey []byte
}

// HeraTLSDrv is a global object keeping the configuration
var HeraTLSDrv *heraDriver // TODO make it private, create the tlsConfig in Open, where parameters like InsecureSkipVerify are passed via the URL

func init() {
	HeraTLSDrv = &heraDriver{
		TLSCfg:           &tls.Config{},
		Ssl:              false,
		EncryptedAuthKey: nil,
	}
	sql.Register("heratls", HeraTLSDrv)
}

func (driver *heraDriver) Open(url string) (driver.Conn, error) {
	ipport := url[2:]
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "Dialing to hera server:", ipport)
	}
	conn, err := tls.Dial("tcp", ipport, HeraTLSDrv.TLSCfg)

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

	if driver.Ssl {
		var ns *netstring.Netstring
		nss := make([]*netstring.Netstring, 2)

		nss[0] = netstring.NewNetstringFrom(common.CmdClientProtocolName, []byte(fmt.Sprintf("occ 1")))
		nss[1] = netstring.NewNetstringFrom(common.CmdClientUsername, []byte(fmt.Sprintf("clocapp")))

		ns = netstring.NewNetstringEmbedded(nss)
		_, err = conn.Write(ns.Serialized)
		if err != nil {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "Failed to send protocol name")
			}
			return nil, errors.New("failed to send protocol name")
		}

		ns, err := reader.ReadNext()
		if err != nil {
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "Failed to read server info")
			}
			return nil, errors.New("failed to read server info")
		}

		if ns.Cmd == common.CmdServerChallenge {

			mac := hmac.New(sha256.New, driver.EncryptedAuthKey)
			_, err := mac.Write(ns.Payload)
			if err != nil {
				return nil, errors.New("challenge verification failed")
			}
			generatedMac := mac.Sum(nil)
			nss := make([]*netstring.Netstring, 2)

			nss[0] = netstring.NewNetstringFrom(common.CmdClientChallengeResponse, generatedMac)
			nss[1] = netstring.NewNetstringFrom(common.CmdClientCurrentClientTime, []byte(fmt.Sprintf("%d", time.Now().Unix())))

			ns = netstring.NewNetstringEmbedded(nss)
			_, err = conn.Write(ns.Serialized)
			if err != nil {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "Failed to send custom auth challenge")
				}
				return nil, errors.New("failed to send custom auth challenge\"")
			}

			ns, err := reader.ReadNext()
			if err != nil {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "Failed to read server response ", err)
				}
				return nil, errors.New("failed to read server response")
			}

			if ns.Cmd != common.CmdServerConnectionAccepted {
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "server did not accept connection")
				}
				return nil, errors.New("server did not accept connection")
			}
		}
	}

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
