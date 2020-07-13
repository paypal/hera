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
#include "config/CDBConfig.h"
#include "config/MultiConfig.h"
#include "CalClient.h"
#include "CalConst.h"
#include "CalMessages.h"
#include "CalURLSession.h"

#include <string>

bool CalURLSession::start(const char* poolname, const char* ppppname)
{
	// Add CAL-specific configuration
	static CDBConfig *cal_cfg = NULL;
 	if (!cal_cfg) 
	{
		cal_cfg = new CDBConfig("./cal_client.cdb");
	}

	std::string poolName;

	if (poolname == NULL) 
	{
	   cal_cfg->get_value("cal_pool_name", poolName);
	} 
	else
	{
		poolName = poolname;
	}

	if (ppppname != NULL) 
	{
		poolName.append("-");
		poolName.append(ppppname);
	}

	if (poolName.empty())
	{
		CalClient::init(cal_cfg, NULL);
	}
	else
	{
		CalClient::init(cal_cfg, poolName.c_str());
	}

	// Until SetName() is called, this transaction will have Name 'unset'
	CalClientSession *cal_client_session = CalURLSession::get_cal_client_session();
	if (cal_client_session)
	{
		cal_client_session->start_session(CAL::TRANS_TYPE_URL, "");
		cal_client_session->generate_trace_log_id(poolName);
	}

	return true;
}

void CalURLSession::end()
{
	CalClientSession *cal_client_session = CalURLSession::get_cal_client_session();
	if (cal_client_session)
	{
		cal_client_session->end_session();
	}
}

bool CalURLSession::is_active()
{
	CalClientSession *cal_client_session = CalURLSession::get_cal_client_session();
	if (cal_client_session)
	{
		return cal_client_session->is_session_active();
	}
	return false;
}

void CalURLSession::set_status(const std::string &_status)
{
	CalClientSession *cal_client_session = CalURLSession::get_cal_client_session();
	if (cal_client_session)
	{
		cal_client_session->set_status(_status);
	}
}

CalTransaction* CalURLSession::get_url_transaction() 
{
	CalClientSession *cal_client_session = CalURLSession::get_cal_client_session();
	if (cal_client_session)
	{
		return cal_client_session->get_session_transaction();
	}
	return NULL;
}

CalClientSession* CalURLSession::get_cal_client_session()
{
	static CalClientSession *s_cal_client_session = NULL;
	if (s_cal_client_session==NULL)
	{
		s_cal_client_session = new CalClientSession();
	}
	return s_cal_client_session;
}

