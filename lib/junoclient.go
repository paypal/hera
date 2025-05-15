// Copyright 2023 PayPal Inc.
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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"juno/pkg/client"
	junocal "juno/pkg/logging/cal/config"
	"juno/pkg/util"
	"os"
	"path"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
)

var tlsConfig *tls.Config
var mutex sync.Mutex

var junoclientOnce sync.Once

type JunoClient struct {
	junoClient      client.IClient
	junoClientReady bool
}

var gCacheInstance *JunoClient

func GetTLSConfig() *tls.Config {
	mutex.Lock()
	defer mutex.Unlock()
	if tlsConfig != nil {
		return tlsConfig
	}

	cert, err := tls.LoadX509KeyPair(
		path.Join(GetConfig().CacheCertFilePath, "server.crt"), path.Join(GetConfig().CacheCertFilePath, "server.pem"),
	)
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error in junoclient::GetTLSConfig", err)
		return nil
	}

	caCert, err := os.ReadFile(path.Join(GetConfig().CacheCertFilePath, "ca.crt"))
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error in junoclient::GetTLSConfig", err)
		return nil
	}
	// rootCAs := x509.NewCertPool()
	// rootCAs.AppendCertsFromPEM(caCert)
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error in junoclient::GetTLSConfig", err)
		return nil
	}
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}
	rootCAs.AppendCertsFromPEM(caCert)

	tlsConfig = &tls.Config{
		ServerName:             "Juno-test-server",
		RootCAs:                rootCAs,
		Certificates:           []tls.Certificate{cert},
		InsecureSkipVerify:     true,
		SessionTicketsDisabled: false,
		ClientSessionCache:     tls.NewLRUClientSessionCache(0),
	}

	return tlsConfig
}

// Get metadata.
func GetInfo(ctx client.IContext) {
	logger.GetLogger().Log(logger.Info, fmt.Sprintf("version=%d creationTime=%d ttl=%d\n", ctx.GetVersion(), ctx.GetCreationTime(), ctx.GetTimeToLive()))
}

func GetJunoClient() (*JunoClient, error) {
	if gCacheInstance == nil {
		logger.GetLogger().Log(logger.Info, "GetJunoClient(), gCacheInstance is nil...")
		mutex.Lock()
		defer mutex.Unlock()
		var err error
		junoclientOnce.Do(
			func() {
				gCacheInstance = &JunoClient{}
				err = gCacheInstance.init() // 10.176.9.146:5080
				// gCacheInstance, err = InitJunoClient(GetConfig().CacheEndPoint, GetTLSConfig) // 10.176.9.146:5080
				if err != nil {
					gCacheInstance = nil
					logger.GetLogger().Log(logger.Alert, "GetJunoClient failed with error:", err)
					err = fmt.Errorf("GetJunoClient failed with error: %s", err.Error())
				}
			},
		)
		return gCacheInstance, err
	}
	return gCacheInstance, nil
}

func (cli *JunoClient) init() error {
	logger.GetLogger().Log(logger.Info, "Init() JunoClient with endpoint", GetConfig().CacheEndPoint)
	cfg := client.Config{
		Appname:           cal.GetCalClientInstance().GetPoolName(),
		Namespace:         GetConfig().CacheNamespace,
		DefaultTimeToLive: GetConfig().CacheDefaultTTL, // seconds
		ConnPoolSize:      GetConfig().CacheConnectionPoolSize,
		ConnectTimeout:    util.Duration{Duration: time.Duration(GetConfig().CacheConnectTimeoutMs) * time.Millisecond},
		ResponseTimeout:   util.Duration{Duration: time.Duration(GetConfig().CacheResponseTimeoutMs) * time.Millisecond},
		BypassLTM:         GetConfig().CacheBypassLTM,
		Cal: junocal.Config{
			// To-do: Move this to config
			Poolname:   cal.GetCalClientInstance().GetPoolName(),
			CalType:    "socket", // socket or file
			CalLogFile: "logCalClient.txt",
			Enabled:    true,
		},
	}

	cfg.Server.Addr = GetConfig().CacheEndPoint
	cfg.Server.SSLEnabled = GetConfig().CacheSSLEnabled // SSL
	var err error
	if GetConfig().CacheSSLEnabled {
		cli.junoClient, err = client.NewWithTLS(cfg, GetTLSConfig)
	} else {
		cli.junoClient, err = client.New(cfg)
	}
	cli.junoClientReady = err == nil
	// Internal patch
	// cli.junoClient, err = client.NewWithTLS(cfg, GetTLSConfigInternal)
	return err
}

func (cli *JunoClient) Set(key []byte, value []byte, ttl uint32, corrId string, calThreadGroupName string) error {
	if GetConfig().EnableCompression {
		compressedValue := snappy.Encode(nil, value)
		evt := cal.NewCalEvent("Encode", "SET", cal.TransOK, "", calThreadGroupName)
		evt.AddDataInt("compressedSize", int64(len(compressedValue)))
		evt.AddDataInt("rawSize", int64(len(value)))
		evt.Completed()
		logger.GetLogger().Log(logger.Verbose, "Set: Compression enabled, compressedSize:", len(compressedValue), "rawSize:", len(value))
		ctx, err := cli.junoClient.Create(key, compressedValue, client.WithTTL(ttl), client.WithCorrelationId(corrId))
		if err == nil {
			GetInfo(ctx)
		}
		return err
	} else {
		ctx, err := cli.junoClient.Create(key, value, client.WithTTL(ttl), client.WithCorrelationId(corrId))
		if err == nil {
			GetInfo(ctx)
		}
		return err
	}
}

func (cli *JunoClient) Get(key []byte, corrId string, calThreadGroupName string) ([]byte, error) {
	resp, ctx, err := cli.junoClient.Get(key, client.WithCorrelationId(corrId))
	if err != nil {
		return resp, err
	}
	if logger.GetLogger().V(logger.Verbose) {
		GetInfo(ctx)
	}
	if GetConfig().EnableCompression {
		decompressedResp, err := snappy.Decode(nil, resp)
		evt := cal.NewCalEvent("Decode", "GET", cal.TransOK, "", calThreadGroupName)
		evt.AddDataInt("respSize", int64(len(resp)))
		evt.AddDataInt("decompressedRespSize", int64(len(decompressedResp)))
		evt.Completed()
		logger.GetLogger().Log(logger.Verbose, "Get: Compression enabled: respSize:", len(resp), "decompressedResp size:", len(decompressedResp))
		return decompressedResp, err
	} else {
		return resp, err
	}
}
