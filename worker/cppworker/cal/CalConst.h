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
#ifndef CAL_CONST_H
#define CAL_CONST_H

#include <string>

namespace CAL
{
	extern const std::string TRANS_TYPE_CLIENT;

	// Predefined CAL Transactions TYPES, DON't REUSE these unless you are instrumenting
	// new server or web application
	extern const std::string TRANS_TYPE_EXEC;
	extern const std::string TRANS_TYPE_FETCH;
	extern const std::string TRANS_TYPE_FETCH_F;
	extern const std::string TRANS_TYPE_FETCH_BATCH;
	extern const std::string TRANS_TYPE_URL;
	extern const std::string TRANS_TYPE_API;
	extern const std::string TRANS_TYPE_REPLAY;

	//The name used by infrastructure to log client related information, not to be used in product code
	extern const std::string TRANS_TYPE_CLIENT_SESSION;

	// Add any other Transaction TYPES which you might defined for your application
	extern const std::string TRANS_TYPE_DCC_WEBBUG;
	extern const std::string TRANS_TYPE_IEFT_PROC;
	extern const std::string TRANS_TYPE_IEFT_SF;
	extern const std::string TRANS_TYPE_AUTH_SETTLE;
	extern const std::string TRANS_TYPE_AUTH_PATH_TWO;
	extern const std::string TRANS_TYPE_PARTNER_ONBOARD;
	extern const std::string TRANS_TYPE_ATTACK_CLIENT;
    extern const std::string TRANS_TYPE_MF_BATCH_D;

	// severity code, Don't add extra level of SEVERITY
	extern const std::string TRANS_OK;
	extern const std::string TRANS_FATAL;
	extern const std::string TRANS_ERROR;
	extern const std::string TRANS_WARNING;

	extern const std::string ERR_DESCRIPTION;
	extern const std::string ERR_ACTION;

	extern const std::string MOD_NONE;
	extern const std::string MOD_OCC;
	extern const std::string MOD_GENERIC_CLIENT;
	extern const std::string MOD_GENERIC_SERVER;

	extern const std::string SYS_ERR_NONE;
	extern const std::string SYS_ERR_CONFIG;
	extern const std::string SYS_ERR_INTERNAL;
	extern const std::string SYS_ERR_MARKED_DOWN;
	extern const std::string SYS_ERR_OCC;
	extern const std::string SYS_ERR_ORACLE;
	extern const std::string SYS_ERR_SQL;

	extern const std::string EVENT_TYPE_FATAL;
	extern const std::string EVENT_TYPE_ERROR;
	extern const std::string EVENT_TYPE_WARNING;
	extern const std::string EVENT_TYPE_EXCEPTION;
	extern const std::string EVENT_TYPE_BACKTRACE;
	extern const std::string EVENT_TYPE_CLIENTINFO;
	extern const std::string EVENT_TYPE_PAYLOAD;
	extern const std::string EVENT_TYPE_MARKUP;
	extern const std::string EVENT_TYPE_MARKDOWN;
	extern const std::string EVENT_TYPE_TL;
	extern const std::string EVENT_TYPE_EOA;
	extern const std::string EVENT_TYPE_MESSAGE;
	extern const std::string EVENT_TYPE_ATTACKCLIENT;

	//The name used by infrastructure to log client related information, not to be used in product code
	extern const std::string EVENT_TYPE_CLIENT_INFO;
	extern const std::string EVENT_TYPE_SERVER_INFO;

	//All cal messages for business monitoring should be done with this event type. It will be mainly used by product code.
	extern const std::string EVENT_TYPE_BIZ;

	// One of these constants should be used during the failure  
	extern const std::string SYSTEM_FAILURE;
	extern const std::string INPUT_FAILURE;

	// use this method to initialize to CAL Transaction OK for static or global variables
	// to avoid problem described in
	// http://www.parashift.com/c++-faq-lite/ctors.html#faq-10.15
	std::string get_trans_ok();
};

#endif //CAL_CONST_H
