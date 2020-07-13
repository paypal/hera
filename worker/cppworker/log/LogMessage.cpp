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
#include <string.h>
#include "LogMessage.h"
#include <string>

LogMessageBase::LogMessageBase(LogLevelEnum level, const std::string &log_name, const char *message, va_list *ap) : 
	m_level(level), 
	m_log_name(log_name),
	m_message(message, strlen(message)),
	m_ap(ap),
	m_seconds(0),
	m_microseconds(0)
{
	timestamp();
}

void LogMessageBase::timestamp(void)
{
	struct timeval tv;

	gettimeofday(&tv, NULL);
	m_seconds = tv.tv_sec;
	m_microseconds = tv.tv_usec;
}

const std::string& LogMessageBase::get_output() const
{
	return m_output;
}
