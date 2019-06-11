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

// Builds the MySQL worker
package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
	workerservice "github.com/paypal/hera/worker/shared"
)

func main() {
	certdir := os.Getenv("certdir")
	finfos, err := ioutil.ReadDir(certdir)
	if err != nil {
		log.Print("could not read dir " + certdir)
	}
	for _, finfo := range finfos {
		if !strings.HasSuffix(finfo.Name(), ".pem") {
			continue
		}
		certfile := certdir + "/" + finfo.Name()
		data, err := ioutil.ReadFile(certfile)
		if err != nil {
			log.Print("could not read " + certfile)
			continue
		}
		block, _ := pem.Decode(data)
		if block == nil || block.Type != "PUBLIC KEY" {
			log.Print("not public key " + certfile)
			continue
		}
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			log.Print(err.Error() + " while loading pubCert " + certfile)
			continue
		}
		if rsaPubKey, ok := pub.(*rsa.PublicKey); ok {
			shortName := finfo.Name()[:len(finfo.Name())-4]
			mysql.RegisterServerPubKey(shortName, rsaPubKey)
		}
	}
	workerservice.Start(&mysqlAdapter{})
}
