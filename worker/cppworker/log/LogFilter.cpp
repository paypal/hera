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
#include "LogFilter.h"
#include "LogMessage.h"



bool LogFilterBase::check_log_level(const LogMessageBase &msg) const
{
	return (msg.level() <= m_logging_level.level);
}

void LogFilterBase::set_log_level(LogLevelEnum l)
{
	m_logging_level = LogLevel(l);
}

void LogFilter::FilterStack::push(FilterFunctorBase *filter)
{
	push_back(std::unique_ptr<FilterFunctorBase>(filter));
}

LogFilter::LogFilter(LogLevelEnum l) : LogFilterBase(l)
{
	register_default_filters();
}

LogFilter::~LogFilter()
{
}

bool LogFilter::non_empty_output(const LogMessageBase &msg) const
{
	return (!msg.output_buffer().empty());
}

void LogFilter::internal_register_filter(FilterStack &stack, FilterFunctorBase *const filter)
{
	stack.push(filter);
}

bool LogFilter::internal_process(const FilterStack &stack, const LogMessageBase &msg) const
{
	int stack_depth = stack.size();
	for (int i = 0; i < stack_depth; ++i)
	{
		// The first filter function that return false
		// will shortcircuit the stack.
		if (!(*stack[i])(msg))
		{
			return false;
		}
	}
	return true;
}

void LogFilter::register_default_filters(void)
{
	internal_register_filter(m_preformat_filter_stack, new FilterFunctor<LogFilter>(this, &LogFilter::check_log_level));
	internal_register_filter(m_postformat_filter_stack, new FilterFunctor<LogFilter>(this, &LogFilter::non_empty_output));
}
