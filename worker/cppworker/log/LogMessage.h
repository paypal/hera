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
#ifndef _LOG_MESSAGE_H_
#define _LOG_MESSAGE_H_

#include "cal/CalMessages.h"
#include "log/LogLevel.h"
#include "utility/Timer.h"

#include <stdarg.h>
#include <sys/time.h>
#include <sstream>

// Forward declaration
template <template <LogLevelEnum> class T> struct LogMessageFactory;

/**
 * @class Base class for all log messages.
 */
class LogMessageBase
{
protected:
	LogLevel m_level;
	std::string m_log_name;
	std::string m_message;
	va_list *m_ap;
	time_t m_seconds;
	time_t m_microseconds;
	mutable std::string m_output;
	mutable std::string m_raw_output;

	void timestamp(void);

public:
	virtual ~LogMessageBase() {}

	int level(void) const { return m_level.level; }
	void set_level(LogLevelEnum l) { m_level = l; }
	const std::string &level_name(void) const { return m_level.get_name(); }
	const std::string &log_name(void) const { return m_log_name; }
	void set_log_name(const std::string &log_name) { m_log_name = log_name; }
	const std::string &message(void) const { return m_message; }
	void set_message(const std::string &message) { m_message = message; }
	const va_list *ap(void) const { return m_ap; }
	void set_ap(va_list *ap) { m_ap = ap; }
	time_t seconds(void) const { return m_seconds; }
	time_t microseconds(void) const { return m_microseconds; }
	void set_time(void) { timestamp(); }

	std::string &output_buffer(void) const { return m_output; }
	void clear_output(void) const { m_output.clear(); }
	virtual const std::string &get_output(void) const;

protected:
	LogMessageBase(LogLevelEnum level, const std::string &log_name, const char *message, va_list *ap);

private:
	// Not allowed
	LogMessageBase();
};



/**
 * @class Factory class for generating LogMessage (or derivation of it)
 */
template <template <LogLevelEnum> class T>
struct LogMessageFactory
{
	static LogMessageBase *get(LogLevelEnum level, const std::string &log_name, const char *msg, va_list *ap=NULL)
	{
		switch (level)
		{
			case LOG_OFF:
				return NULL;
			case LOG_ALERT:
				return new T<LOG_ALERT>(log_name, msg, ap);
			case LOG_WARNING:
				return new T<LOG_WARNING>(log_name, msg, ap);
			case LOG_INFO:
				return new T<LOG_INFO>(log_name, msg, ap);
			case LOG_DEBUG:
				return new T<LOG_DEBUG>(log_name, msg, ap);
			case LOG_VERBOSE:
				return new T<LOG_VERBOSE>(log_name, msg, ap);
			default:
				std::ostringstream os;
				os << "Undefined log level " << level;
				throw LogLevelError(os.str());
		}
	}

	static LogMessageBase *get(LogLevelEnum level, const std::string &log_name, Timer &timer, const char *msg, va_list *ap=NULL)
	{
		std::ostringstream os;

		timer.mark();
		os << "<" << timer.get_string() << "> " << msg;
		return get(level, log_name, os.str().c_str(), ap);
	}

	static LogMessageBase *get_detailed(LogLevelEnum level, const std::string &log_name, Timer &timer, const char *msg, va_list *ap=NULL)
	{
		std::ostringstream os;

		timer.mark();
		os << "<" << timer.get_detailed_string() << "> " << msg;
		return get(level, log_name, os.str().c_str(), ap);
	}
};


/**
 * @class Basic log message class.
 */
template <LogLevelEnum l>
class LogMessage : public LogMessageBase
{
public:
	LogMessage(const std::string &log_name, const char *message, va_list *ap) :
		LogMessageBase(l, log_name, message, ap) { }
};

// Specialization for LOG_VERBOSE
template <>
class LogMessage<LOG_VERBOSE> : public LogMessageBase
{
private:

public:
	LogMessage(const std::string &log_name, const char *message, va_list *ap) :
		LogMessageBase(LOG_VERBOSE, log_name, message, ap) {}
};


/**
 * @class LogMessage that adds a CalEvent
 */
template <LogLevelEnum l>
class CALLogMessage : public LogMessage<l>
{
private:
	CalEvent *m_event;
	mutable bool m_cal_payload_set;

public:
	CALLogMessage(const std::string &log_name, const char *message, va_list *ap) :
		LogMessage<l>(log_name, message, ap)
	{
		m_event = new CalEvent("Msg", "Log", "0", NULL);
		m_cal_payload_set = false;
	}

	~CALLogMessage() { delete m_event; }

	const std::string& get_output(void) const {
		if (!m_cal_payload_set)
		{
			m_event->AddData(this->m_output);
			m_cal_payload_set = true;
		}

		return LogMessageBase::get_output();
	}
};

#endif //_LOG_MESSAGE_H_
