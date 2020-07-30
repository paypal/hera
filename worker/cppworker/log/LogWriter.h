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
#ifndef LOG_WRITER_H
#define LOG_WRITER_H

#include <stdarg.h>
#include "LogFilter.h"
#include "LogFormatter.h"
#include <string>
#include <fstream>

class LogMessageBase;
class Timer;


/**
 *
 * @macro WRITE_LOG_ENTRY
 *
 * This is a coding standard to output messages to logs.
 * 
 * It encapsulates external log level check and call to the actual write_log function.
 * Implemented as macro to avoid evaluation of parameters for function call
 * unless logging is actually in effect.
 * 
 */

#define WRITE_LOG_ENTRY(LOGGER, LEVEL, ARGS...) \
    do { if ((LOGGER) && (LOGGER)->get_log_level().level >= LEVEL) \
                (LOGGER)->write_entry(LEVEL, ##ARGS); } while (0)

#define WRITE_LOG_TIME(LOGGER, LEVEL, ARGS...) \
    do { if ((LOGGER) && (LOGGER)->get_log_level().level >= LEVEL) \
                (LOGGER)->log_time(LEVEL, ##ARGS); } while (0)

#define WRITE_LOG_TIME_DETAILED(LOGGER, LEVEL, ARGS...) \
    do { if ((LOGGER) && (LOGGER)->get_log_level().level >= LEVEL) \
                (LOGGER)->log_time_detailed(LEVEL, ##ARGS); } while (0)

class LogWriterBase
{
	friend class LogFactory;
	friend class BasicDisposalPolicy;

protected:
	std::string m_name;	//!< Log name

	LogWriterBase(const std::string &log_name) : m_name(log_name) {}
	virtual ~LogWriterBase() { }

public:
	void set_log_name(const std::string &name) { m_name = name; }
	const std::string &get_log_name(void) const { return m_name; }
	virtual int get_log_fd(void) const { return -1; }

	// Interfaces
	virtual std::string class_name(void) const = 0;

	virtual void set_log_level(LogLevelEnum l) = 0;
	virtual LogLevel get_log_level(void) const = 0;
	virtual void set_format(const char *format) = 0;
	virtual std::string get_format(void) const = 0;
	virtual void write_entry(const LogMessageBase &msg) = 0;
	virtual void write_entry(LogLevelEnum l, const char *msg, ...) = 0;
	virtual void vwrite_entry(LogLevelEnum l, const char *msg, va_list* _ap) = 0;
	virtual void log_time(LogLevelEnum l, Timer &t, const char *msg, ...) = 0;
	virtual void log_time_detailed(LogLevelEnum l, Timer &t, const char *msg, ...) = 0;
	virtual void set_enable_cal(bool state) = 0;
	virtual bool get_enable_cal() const = 0;

private:
	LogWriterBase(const LogWriterBase &src);
};

template <class T, class U>
class LogWriterTemplate : public LogWriterBase
{
	friend class LogFactory;
	friend class BasicDisposalPolicy;
	friend class MultiChannelLogWriter;

public:
	typedef T FilterType;
	typedef U FormatterType;

protected:
	FilterType m_filter;
	FormatterType m_formatter;
	bool m_enable_cal;

	LogWriterTemplate(const std::string &log_name, LogLevelEnum l) : LogWriterBase(log_name), m_filter(l), m_enable_cal(false) { set_format("%u %p %t: %s\n"); }
	virtual ~LogWriterTemplate() { }

	virtual void internal_write_entry(const LogMessageBase &msg) = 0;

public:
	LogFilterBase &get_filter(void) { return m_filter; }
	const LogFilterBase &get_filter(void) const { return m_filter; }
	LogFormatterBase &get_formatter(void) { return m_formatter; }
	const LogFormatterBase &get_formatter(void) const { return m_formatter; }
	void set_log_level(LogLevelEnum l) { m_filter.set_log_level(l); }
	LogLevel get_log_level(void) const { return m_filter.get_log_level(); }
	void set_format(const char *format) { m_formatter.set_format(format); }
	std::string get_format(void) const { return m_formatter.get_format(); }
	void write_entry(const LogMessageBase &msg);
	void write_entry(LogLevelEnum l, const char *msg, ...);
	void vwrite_entry(LogLevelEnum l, const char *msg, va_list* _ap);
	void log_time(LogLevelEnum l, Timer &t, const char *msg, ...);
	void log_time_detailed(LogLevelEnum l, Timer &t, const char *msg, ...);
	void set_enable_cal(bool state) { m_enable_cal = state; }
	bool get_enable_cal() const { return m_enable_cal; }
};

template <class T, class U>
class LogStreamWriter : public LogWriterTemplate<T, U>
{
	friend class LogFactory;
	friend class BasicDisposalPolicy;
	friend class MultiChannelLogWriter;

protected:
	std::ostream *m_stream;	

	LogStreamWriter(const std::string &log_name, LogLevelEnum l);
	explicit LogStreamWriter(const std::string &log_name, LogLevelEnum l, std::ostream *stream);
	virtual ~LogStreamWriter() { delete m_stream; }

	void internal_write_entry(const LogMessageBase &msg);


public:
	virtual std::string class_name(void) const { return "LogStreamWriter"; }
	void set_stream(std::ostream *stream);
	std::ostream *get_stream(void) const { return m_stream; }
};

template <class T, class U>
class LogFileWriter : public LogStreamWriter<T, U>
{
	friend class LogFactory;
	friend class BasicDisposalPolicy;
	friend class MultiChannelLogWriter;

protected:

protected:
	explicit LogFileWriter(const std::string &log_name, LogLevelEnum l);
	virtual ~LogFileWriter() { }

public:
	virtual std::string class_name(void) const { return "LogFileWriter"; }
	void set_stream(const std::string &filename, bool append=true, bool truncate=false);
};

template <class T, class U>
class StderrWriter : public LogFileWriter<T, U>
{
	friend class LogFactory;
	friend class BasicDisposalPolicy;
	friend class MultiChannelLogWriter;

protected:
	StderrWriter(const std::string &log_name, LogLevelEnum l);
	~StderrWriter() { }

public:
	std::string class_name(void) const { return "StderrWriter"; }
	void set_stream(std::ostream &stream) {}
	void set_stream(const std::string &filename) {}
};

template <class T, class U>
class NullWriter : public LogFileWriter<T, U>
{
	friend class LogFactory;
	friend class BasicDisposalPolicy;
	friend class MultiChannelLogWriter;

protected:
	NullWriter(const std::string &log_name, LogLevelEnum l);
	~NullWriter() { }

public:
	std::string class_name(void) const { return "NullWriter"; }
	void set_stream(std::ostream &stream) {}
	void set_stream(const std::string &filename) {}
};

#include "LogWriter.cpp"

#endif //_LOG_WRITER_H_
