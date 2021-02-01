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
#include <stdlib.h>
#include "OCCChild.h"
#include "OCCChildFactory.h"
#include "WorkerException.h"

#define ORA_CHARSET_UTF8         "UTF8"
#define ORA_CHARSET_AL32UTF8         "AL32UTF8"
#define ORA_CHARSET_WE8ISO8859P1 "WE8ISO8859P1"
#define OCC_SERVER_NAME "occ"
#define OCC_CONFIG_NAME OCC_SERVER_NAME ".cdb"

std::unique_ptr<Worker> OCCChildFactory::create(const InitParams& _params) const
{

	const char* lang_env = getenv("NLS_LANG");
	if (lang_env == 0)
	{
		LogFactory::get(DEFAULT_LOGGER_NAME)->write_entry(LOG_WARNING, "NLS_LANG environment not set, using default UTF8");
		lang_env = "." ORA_CHARSET_UTF8;
		setenv("NLS_LANG", lang_env, true);
	}
	else
	{
		//check if encoding is either UTF8 or AL32UTF8
		//if not, throw exception
		std::string charset_env = lang_env;	
		StringUtil::to_upper_case(charset_env);
		if ((charset_env != ("." ORA_CHARSET_UTF8)) &&
			 (charset_env != ("." ORA_CHARSET_AL32UTF8)))
		{
			std::ostringstream os;
			os << "NLS_LANG environment charset " << charset_env << " not allowed";
			throw WorkerCreationException(os.str());
		}
	}
	LogFactory::get(DEFAULT_LOGGER_NAME)->write_entry(LOG_INFO, "using %s character set", lang_env + 1);
	std::unique_ptr<Worker> worker((Worker*)new OCCChild(_params));
	std::string charset;
	if (0 == ((OCCChild*)(worker.get()))->get_db_charset(charset))
	{
		if (charset.compare(std::string(lang_env + 1)) == 0 ||
			std::string(lang_env).compare("." ORA_CHARSET_UTF8) == 0 && charset.compare(std::string(ORA_CHARSET_AL32UTF8)) == 0)
			return worker;
		else
		{
			std::ostringstream os;
			os << "DB charset mismatch: expected " << lang_env + 1 << " but found " << charset;
			throw WorkerCreationException(os.str());
		}
	}
	else
		throw WorkerCreationException("Could not get DB charset");
}

const char* OCCChildFactory::get_config_name() const
{
	return OCC_CONFIG_NAME;
}

const char* OCCChildFactory::get_server_name() const
{
	return OCC_SERVER_NAME;
}

