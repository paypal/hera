// Copyright 2020 PayPal Inc.
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
#ifndef _SERVERCOMMANDS_H_
#define _SERVERCOMMANDS_H_

#define SERVER_CHALLENGE                          1001
#define SERVER_CONNECTION_ACCEPTED                1002
#define SERVER_CONNECTION_REJECTED_PROTOCOL       1003
#define SERVER_CONNECTION_REJECTED_UNKNOWN_USER   1004
#define SERVER_CONNECTION_REJECTED_FAILED_AUTH    1005
#define SERVER_UNEXPECTED_COMMAND                 1006
#define SERVER_INTERNAL_ERROR                     1007
#define SERVER_PING_COMMAND                       1008
#define SERVER_ALIVE                              1009
#define SERVER_CONNECTION_REJECTED_CLIENT_TIME    1010
#define SERVER_INFO	                              1011
#define SERVER_INT_INFO                           1012

#define CLIENT_PROTOCOL_NAME_NOAUTH               2001
#define CLIENT_PROTOCOL_NAME                      2002
#define CLIENT_USERNAME                           2003
#define CLIENT_CHALLENGE_RESPONSE                 2004
#define CLIENT_CURRENT_CLIENT_TIME                2005


// For Cal correlation id during handshahing
#define CLIENT_CAL_CORRELATION_ID                 2006
#define CLIENT_INFO				  2007

#define PROTOCOL_VERSION			  2008

const char* const FRAMEWORK_VERSION = "NetString FW v-1.0";
#endif
