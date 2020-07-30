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
#include "CalConst.h"

/**
 * CalTransaction status code standard.
 *
 * For normal termination of a CalTransaction, the status code can simply be set to
 * CAL::TRANS_OK
 * For error termination, the format of the status code is as follow:
 *
 * <severity>.<module name>.<system error code>.<module return code>
 *
 */

// CalTransaction Types
// Predefined CAL Transactions TYPES, DON't REUSE these unless you are instrumenting
// new server or web application
const std::string CAL::TRANS_TYPE_CLIENT = std::string("CLIENT");
const std::string CAL::TRANS_TYPE_EXEC = std::string("EXEC");
const std::string CAL::TRANS_TYPE_FETCH = std::string("FETCH");
const std::string CAL::TRANS_TYPE_FETCH_F = std::string("FETCHF");
const std::string CAL::TRANS_TYPE_FETCH_BATCH = std::string("FETCHB");
const std::string CAL::TRANS_TYPE_URL = std::string("URL");
const std::string CAL::TRANS_TYPE_API = std::string("API");
const std::string CAL::TRANS_TYPE_REPLAY = std::string("REPLAY");

// Add any other Transaction TYPES which you might defined for your application
const std::string CAL::TRANS_TYPE_DCC_WEBBUG = std::string("DCC_WEBBUG");
const std::string CAL::TRANS_TYPE_IEFT_PROC = std::string("IEFT_PROC");
const std::string CAL::TRANS_TYPE_IEFT_SF = std::string("IEFT_SF");
const std::string CAL::TRANS_TYPE_AUTH_SETTLE = std::string("AUTH_SETTLE");
const std::string CAL::TRANS_TYPE_AUTH_PATH_TWO = std::string("AUTH_PATH_2");
const std::string CAL::TRANS_TYPE_PARTNER_ONBOARD = std::string("PARTNER_ONBOARD");
const std::string CAL::TRANS_TYPE_ATTACK_CLIENT = std::string("ATTACK_CLIENT");
const std::string CAL::TRANS_TYPE_MF_BATCH_D = std::string("MF_BATCH_D");

// severity code, Don't add extra level of SEVERITY
const std::string CAL::TRANS_OK = std::string("0");
const std::string CAL::TRANS_FATAL = std::string("1");
const std::string CAL::TRANS_ERROR = std::string("2");
const std::string CAL::TRANS_WARNING = std::string("3");

// Addition data field name
const std::string CAL::ERR_DESCRIPTION = std::string("ERR_DESCRIPTION");
const std::string CAL::ERR_ACTION = std::string("ERR_ACTION");

// Module names
const std::string CAL::MOD_NONE = std::string("");
const std::string CAL::MOD_OCC = std::string("OCC");
const std::string CAL::MOD_GENERIC_CLIENT = std::string("CLIENT");
const std::string CAL::MOD_GENERIC_SERVER = std::string("SERVER");

const std::string CAL::SYS_ERR_NONE = std::string("");
const std::string CAL::SYS_ERR_CONFIG = std::string("CONFIG");
const std::string CAL::SYS_ERR_INTERNAL = std::string("INTERNAL");
const std::string CAL::SYS_ERR_MARKED_DOWN = std::string("MARKED DOWN");
const std::string CAL::SYS_ERR_OCC = std::string("OCC");
const std::string CAL::SYS_ERR_ORACLE = std::string("ORACLE");
const std::string CAL::SYS_ERR_SQL = std::string("SQL");

// Event types
const std::string CAL::EVENT_TYPE_FATAL = std::string("FATAL");
const std::string CAL::EVENT_TYPE_ERROR = std::string("ERROR");
const std::string CAL::EVENT_TYPE_WARNING = std::string("WARNING");
const std::string CAL::EVENT_TYPE_EXCEPTION = std::string("EXCEPTION");
const std::string CAL::EVENT_TYPE_CLIENTINFO = std::string("ClientInfo");
const std::string CAL::EVENT_TYPE_BACKTRACE = std::string("Backtrace");
const std::string CAL::EVENT_TYPE_PAYLOAD = std::string("Payload");
const std::string CAL::EVENT_TYPE_MARKUP = std::string("MarkUp");
const std::string CAL::EVENT_TYPE_MARKDOWN = std::string("MarkDown");
const std::string CAL::EVENT_TYPE_TL = std::string("TL");
const std::string CAL::EVENT_TYPE_EOA = std::string("EOA");
const std::string CAL::EVENT_TYPE_MESSAGE = std::string("MSG");
const std::string CAL::EVENT_TYPE_ATTACKCLIENT = std::string("ATTACKCLIENT");

//The name used by infrastructure to log client related information not to be used in product code.
const std::string CAL::EVENT_TYPE_CLIENT_INFO = std::string("CLIENT_INFO");
const std::string CAL::EVENT_TYPE_SERVER_INFO = std::string("SERVER_INFO");

//All cal messages for business monitoring should be done with this event type. It will be mainly used by product code.
const std::string CAL::EVENT_TYPE_BIZ = std::string("BIZ");

const std::string CAL::TRANS_TYPE_CLIENT_SESSION = std::string("CLIENT_SESSION"); 

// Failure status 
const std::string CAL::SYSTEM_FAILURE = std::string("1");
const std::string CAL::INPUT_FAILURE = std::string("2");

std::string CAL::get_trans_ok() { return std::string("0"); }
