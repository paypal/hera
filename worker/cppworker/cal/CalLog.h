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
#ifndef __CALLOG_H
#define __CALLOG_H

#include <stdio.h>
#include <string>
#include <config/Config.h>


class LogWriterBase;
class CalConfig;

/**
 * Enumeration of log levels used for logging
 */
enum CalLogLevel 
{
	 CAL_LOG_ALERT,
	 CAL_LOG_WARNING,
	 CAL_LOG_INFO,
	 CAL_LOG_DEBUG,
	 CAL_LOG_VERBOSE
};

/**
 * This class is used to write cal/trace messages in to the file.
 */
class CalLog
{
 public:
	/**
	 * Constructor chooses application log file if cal_enable_mlog flag is true or chooses 
	 * a local log file to write the messages
	 */
	CalLog (bool app_log_enabled, CalLogLevel loglevel, std::string logfile);
	~CalLog();

	/**
	 * This method is used to write the trace messages either in to application log file
	 * or local log file
	 */
	void write_trace_message(CalLogLevel _loglevel, int _errno, const char * _str, ...);

	/**
	 * This method is used to write the cal messages either in to application log file
	 * or local log file
	 */
	void write_cal_message(const std::string& cal_msg);

	/**
	 * This API is used to disable the CAL.
	 */
	static void set_exit_flag(bool val);

 protected:
	CalLogLevel    m_loglevel;
	LogWriterBase *m_logger;
	FILE	      *m_fp;
	unsigned int  m_max_file_size;

	/**
	 * No default construction allowed
	 */
	CalLog ();

	/**
	 * No copy construction allowed
	 */
	CalLog (const CalLog& other);

	/**
	 * No assignment allowed
	 */
	void operator= (const CalLog& other);
	
	/**
	 * Getter method for cal_enabled flag
	 */
	bool get_cal_enabled();

	/**
	 * This method will write the messages in to the log file.
	 */
	void write_log_file(const std::string& buffer);

	/**
	 * Getter method for config instance
	 */
	static CalConfig* get_config_instance();

	/**
	 * This method sets the string format of log level
	 */
	std::string str_level (CalLogLevel _loglevel);
};

#endif
