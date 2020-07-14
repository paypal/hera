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

#include "CalClient.h"
#include "CalLog.h"
#include "CalConfig.h"
#include "CalSocketHandler.h"
#include "CalFileHandler.h"
#include <config/Config.h>
#include <utility/StringUtil.h>

#include <string>

pid_t  CalClient::s_pid = 0;
std::string CalClient::m_label_affix = "";

bool CalClient::init(Config *_config,
                     const char *_poolname,
                     bool _read_version_cdb)
{
	CalClient *client = get_instance();
	if (!client)
		return false;

	client->initialize(_config, _poolname, _read_version_cdb);

	if (s_pid != ::getpid())
	{
		client->reset(_config, _poolname, _read_version_cdb);
		s_pid=::getpid();
					
	}


	return true;
}

bool CalClient::is_initialized()
{
	CalClient *cal_client = CalClient::get_instance();
	if(cal_client)
	{
		return cal_client->get_is_already_initialized();
	}
	return false;
}

bool CalClient::is_enabled()
{
	CalClient *cal_client = CalClient::get_instance();
	if(cal_client)
	{
		CalConfig *cal_config = cal_client->get_config_instance();
		return cal_config ? cal_config->get_cal_enabled() : false ;
	}
	else
		return false;
}

void CalClient::set_label_affix(const std::string _lable_affix)
{
	m_label_affix = _lable_affix;
}

std::string CalClient::get_poolname()
{
	CalClient *cal_client = CalClient::get_instance();
	if(cal_client)
	{
		CalConfig *cal_config = cal_client->get_config_instance();
		return cal_config ? cal_config->get_poolname() : std::string("");
	}
	else
		return std::string("");
}

bool CalClient::is_poolstack_enabled()
{
	CalClient *cal_client = CalClient::get_instance();
	if(cal_client)
	{
		CalConfig *cal_config = cal_client->get_config_instance();
		if(cal_config)
			return cal_config->get_poolstack_enabled();
	}
	return false;
}


CalClient::CalClient()
	: m_is_already_initialized(false)
	, m_config(NULL)
	, m_handler(NULL)
	, m_current_transaction(NULL)
	, m_root_transaction(NULL)
	, m_pending_flag (false)
	, m_pending_message_buffer()
	, m_session_id()
{
}

CalClient* CalClient::get_instance()
{
	static CalClient* s_instance = NULL;
	if (s_instance == NULL)
	{
		s_instance = new CalClient();
		s_pid=::getpid();
	}
	return s_instance;
}
 
void CalClient::atexit_callback()
{
	CalClient *cal_client = CalClient::get_instance();
	if(cal_client)
	{
		CalConfig *cal_config = cal_client->get_config_instance();
		if(cal_config)
		{
			cal_config->disable_cal();
		}
	}
}


/**
 * Initializes the config and handler object. Registers the atexit_callback() method to be called
 * when the process is exiting.
 */
 
void CalClient::initialize( Config *_config,
                       const char *_poolname,
                       bool _read_version_cdb)
{

	//  return if config pointer is NULL to avoid wrong usage of this pointer in CalConfig 
	if (!_config)
		return ;

	//  if already initialized just return
	if(m_is_already_initialized)
		return;


	if(!m_config)
		m_config = new CalConfig(_config, _read_version_cdb, _poolname, m_label_affix.c_str());

	//  initialize socket or file handler based on config
	if(!m_handler)
	{
		std::string handler_type = m_config->get_handler_type();
		StringUtil::to_lower_case(handler_type);

		if(handler_type == "file")
		{
			m_handler = new CalFileHandler(m_config, m_config->get_logger());
		}
		else
		{
			m_handler = new CalSocketHandler(m_config, m_config->get_logger());
		}
	}
	//Connection to caldaemon has been established in this process. Lets store its pid.
	s_pid = ::getpid();

	/*
	 * Destructors of some objects(e.g. BaseConnection) may call CAL. 
	 * It is unsafe if the objects are static/smart ones since some other
	 * static/smart variables directly or indirectly used by CAL might
	 * have been freed already. See bug PPSCR00089793.
	 */
	::atexit( atexit_callback );
	m_is_already_initialized = true;
}

void CalClient::reset(Config *_config, const char *_poolname, bool _read_version_cdb)
{
	m_current_transaction = NULL;
	m_root_transaction = NULL;
	m_pending_flag = false;
	m_pending_message_buffer.clear();
	m_session_id.clear();
	 
	// delete should be in this order since handler caches the logger which is stored in the
	// config
	delete m_handler;
	m_handler = NULL;
	delete m_config;
	m_config = NULL;

	m_config = new CalConfig(_config, _read_version_cdb, _poolname);


	std::string handler_type = m_config->get_handler_type();
	StringUtil::to_lower_case(handler_type);
	if(handler_type == "file")
	{
		m_handler = new CalFileHandler(m_config, m_config->get_logger());
	}
	else
	{
		m_handler = new CalSocketHandler(m_config, m_config->get_logger());
	}
}


