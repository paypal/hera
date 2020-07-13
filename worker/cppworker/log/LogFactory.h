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
#ifndef LOG_FACTORY_H
#define LOG_FACTORY_H

#include "LogLevel.h"
#include "LogFilter.h"
#include "LogFormatter.h"
#include "LogWriter.h"

#include "utility/PPException.h"

#include <string>
#include <unordered_map>

class Config;

#define DEFAULT_LOGGER_NAME     "DEFAULT_LOGGER"

#define DEBUG_BEACON(msg)        WRITE_LOG_ENTRY(LogFactory::get(), LOG_VERBOSE, "%s:%d  ", msg, __FILE__, __LINE__)
#define DEBUG_MESG(msg, args...) WRITE_LOG_ENTRY(LogFactory::get(), LOG_DEBUG, "%s:%d  ", msg, __FILE__, __LINE__, args)

typedef LogStreamWriter<LogFilter, LogFormatter> StreamLogger;
typedef LogFileWriter<LogFilter, LogFormatter> Logger;
typedef StderrWriter<LogFilter, LogFormatter> StderrLogger;
typedef NullWriter<LogFilter, LogFormatter> NullLogger;

class LogFactoryError : public PPException
{
public:
	LogFactoryError(const std::string &message);

	std::string get_name(void) const;
};

class LogFactory
{
private:
	typedef LogWriterBase LogEntry;
	typedef std::unordered_map<std::string, LogEntry*> LogRepository;

	static std::string s_last_log_name;	//!< This stored the most recently created Log name.
	static LogLevelEnum s_default_log_level;	//!< This is the default log level

	static LogRepository &get_repository(void);

public:
	static void set_default_log_level(LogLevelEnum level);
	static std::string get_log_level_cval(const Config &config, const std::string &log_name);
	static std::string get_log_filename_cval(const Config &config, const std::string &log_name);
	//!< @brief Use this method to get a previously created logger by name. It defaults to the last created logger.
	static LogWriterBase *get(const std::string &log_name=DEFAULT_LOGGER_NAME);
	static NullLogger *get_null_logger(void);

	template < class LogWriter > 
	static LogWriter *get(const std::string &log_name, bool create)
	{
		LogRepository &logger_instances = get_repository();
		LogRepository::iterator logger = logger_instances.find(log_name);
		LogWriter *specific = NULL;

		if ((logger == logger_instances.end()) && create)
		{
			specific = new LogWriter(log_name, s_default_log_level);
			logger_instances.insert(std::pair<std::string, LogEntry*>(log_name, specific));

			// Set this latest one as the default unless we are getting the NullLogger
			if (specific->class_name() != "NullWriter")
				s_last_log_name = log_name;
		}
		else if (logger != logger_instances.end())
		{
			specific = dynamic_cast<LogWriter *>(logger->second);
			if (specific == NULL)
			{
				std::ostringstream os;
				os << "Type mismatch: Logger '" << log_name << "' is a '" << logger->second->class_name() << "'.";
				throw LogFactoryError(os.str());
			}
		}

		return specific;
	}

	template < class LogWriter > 
	static LogWriter *get(const std::string &log_name) { return get<LogWriter>(log_name, true); }

	template < class LogWriter > 
	static LogWriter *get(void) { return get<LogWriter>(DEFAULT_LOGGER_NAME, true); }

	template < class LogWriter > 
	static LogWriter *get(const Config &config, const std::string &log_name)
	{
		std::string log_level = get_log_level_cval(config, log_name);
		LogWriter *logger = get<LogWriter>(log_name, true/*create*/);

		if (!log_level.empty())
		{
			logger->set_log_level(static_cast<LogLevelEnum>(StringUtil::to_int(log_level)));
		}

		return logger;
	}

	static LogEntry *find(const std::string &log_name);
};

template <> StderrLogger *LogFactory::get<StderrLogger>();
template <> Logger *LogFactory::get<Logger>(const Config &config, const std::string &log_name);

#endif //_LOG_FACTORY_H_
