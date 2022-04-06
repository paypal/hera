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
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"path/filepath"
	"github.com/go-sql-driver/mysql"
	workerservice "github.com/paypal/hera/worker/shared"
)

func main() {
	currentDir, abserr := filepath.Abs(filepath.Dir(os.Args[0]))
	
	if abserr != nil {
		currentDir = "./"
	} else {
		currentDir = currentDir + "/"
	}

	certdir := os.Getenv("certdir")
	certdir = currentDir + certdir
	finfos, err := ioutil.ReadDir(certdir)
	if err != nil {
		log.Print("could not read dir " + certdir)
	}
	for _, finfo := range finfos {
		if !strings.HasSuffix(finfo.Name(), ".pem") {
			continue
		}
		shortName := finfo.Name()[:len(finfo.Name())-4]
		certfile := certdir + "/" + finfo.Name()
		data, err := ioutil.ReadFile(certfile)
		if err != nil {
			log.Print("could not read " + certfile)
			continue
		}
		rootCertPool := x509.NewCertPool()
		if ok := rootCertPool.AppendCertsFromPEM(data); !ok {
			log.Print("could not add rt pem " + certfile)
			continue
		}
		serverName := os.Getenv("ServerCertCN")
		if serverName != "" {
			mysql.RegisterTLSConfig(shortName, &tls.Config{RootCAs: rootCertPool, ServerName: serverName})
			log.Print("added cert with name" + certfile)
		} else {
			mysql.RegisterTLSConfig(shortName, &tls.Config{RootCAs: rootCertPool})
			log.Print("added cert " + certfile)
		}
	}
	log.Print("searched certs " + certdir)
	workerservice.Start(&mysqlAdapter{})
}

/*
To test DB cert validation, I put the db's cert in $certdir/certOrCa.pem
export certdir=/path/to/dir/with/certs
export TWO_TASK='tcp(db.example.com:3306)/clocschema?timeout=9s&tls=certOrCa'

To generate a DB cert:
cd /etc/mysql

cat << EOF > db-cert.cfg
[ req ]
prompt = no
distinguished_name = ca_dn

[ ca_dn ]
organizationName = "Hera Test DB Cert"
commonName = "hera test db"
countryName = "US"
stateOrProvinceName = "California"
EOF
openssl req -x509 -nodes -config db-cert.cfg -newkey rsa:3072 -keyout server-key0.pem -out server-cert.pem -days 3000

openssl rsa -in server-key0.pem -out server-key.pem

if ! grep -q ^ssl-key mysql.conf.c/mysqld.cnf
then
    sed -e 's/^# ssl-key/ssl-key/;s/^# ssl-cert/ssl-cert/' -i mysql.conf.d/mysqld.cnf
fi

# for some installations, you'll also have to edit the bind_address to
# 0.0.0.0 in mysqld.conf and use a mysql client to adjust grants or permissions
# to allow the user to login from other ip's.

*/
