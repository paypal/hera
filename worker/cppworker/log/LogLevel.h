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
#ifndef _LOG_LEVEL_H_
#define _LOG_LEVEL_H_

#include <string>
#include <utility/PPException.h>

#undef LOG_OFF
#undef LOG_ALERT
#undef LOG_WARNING
#undef LOG_INFO
#undef LOG_DEBUG
#undef LOG_VERBOSE

class LogLevelError : public PPException
{
public:
	LogLevelError(const std::string &msg) : PPException(msg) {}
	std::string get_name(void) const { return "LogLevelError"; }
};

enum LogLevelEnum {
	LOG_OFF = -1,
	LOG_ALERT = 0, 
	LOG_WARNING, 
	LOG_INFO,
	LOG_DEBUG,
	LOG_VERBOSE,
	LOG_LEVEL_MAX
};

const extern std::string g_log_level_names[];

struct LogLevel
{
	int level;

	LogLevel(LogLevelEnum l) : level(l)
	{
		//Clamp it into range to avoid an exception
		if (level < LOG_OFF)
			level = LOG_OFF;
		else if (level >= LOG_LEVEL_MAX)
			level = LOG_LEVEL_MAX - 1;
	}

	const std::string &get_name() const
	{
		return g_log_level_names[level + 1];
	}

	~LogLevel() {}
	bool operator==(const LogLevel &other) const { return (level == other.level); }
	bool operator==(LogLevelEnum l) const { return (l == LogLevelEnum(level)); }
	operator LogLevelEnum(void) const { return LogLevelEnum(level); }
};

#if 0
#undef ALERT
#undef WARNING
#undef INFO
#undef DEBUG
#undef VERBOSE

const LogLevel ALERT(LOG_ALERT);
const LogLevel WARNING(LOG_WARNING);
const LogLevel INFO(LOG_INFO);
const LogLevel DEBUG(LOG_DEBUG);
const LogLevel VERBOSE(LOG_VERBOSE);
#endif

#endif //_LOG_LEVEL_H_
