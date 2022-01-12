#!/bin/bash
# Copyright 2019 PayPal Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cd /srv
ln -sf $GOPATH/bin/mux .
ln -sf $GOPATH/bin/mysqlworker .

HERA_DB_VIP=${HERA_DB_VIP:-127.0.0.1}
HERA_DB_SCHEMA=${HERA_DB_SCHEMA:-testschema}
HERA_DB_USER=${HERA_DB_USER:-root}

export TWO_TASK="tcp($HERA_DB_VIP:3306)/$HERA_DB_SCHEMA"
# consider using dns so DBAs can move db nodes around

# for read-write split
#export TWO_TASK_READ=tcp($HERA_DB_VIP:3306)/$HERA_DB_SCHEMA

# for read-replica retry
#export TWO_TASK_STANDBY0=tcp($HERA_DB_VIP:3306)/$HERA_DB_SCHEMA

# for sharding
#export TWO_TASK_0=tcp($HERA_DB_VIP:3306)/$HERA_DB_SCHEMA
#export TWO_TASK_READ_0=tcp($HERA_DB_VIP:3306)/$HERA_DB_SCHEMA
#export TWO_TASK_STANDBY0_0=tcp($HERA_DB_VIP:3306)/$HERA_DB_SCHEMA
#export TWO_TASK_1=tcp($HERA_DB_VIP:3306)/$HERA_DB_SCHEMA
#export TWO_TASK_READ_1=tcp($HERA_DB_VIP:3306)/$HERA_DB_SCHEMA
#export TWO_TASK_STANDBY0_1=tcp($HERA_DB_VIP:3306)/$HERA_DB_SCHEMA

export username=$HERA_DB_USER
# docker command should pass in db password
#export password=SomethingHere
export TLS_KEY_PASSWD=35-Out
date
echo ==== starting ====
./mux --name hera-test 
echo ==== server stopped, sleeping a bit to help any startup issue debugging ====
date
sleep 999
