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
#ifndef _LOG_FILTER_H_
#define _LOG_FILTER_H_

#include "LogLevel.h"

#include <stdarg.h>
#include <string>
#include <memory>
#include <vector>

class LogMessageBase;

class FilterFunctorBase
{
public:
	FilterFunctorBase() {}
	virtual ~FilterFunctorBase() {}

	virtual bool operator()(const LogMessageBase &msg) const = 0;
};

template<class T>
class FilterFunctor : public FilterFunctorBase
{
private:
	T *m_instance;
	bool (T::*m_func)(const LogMessageBase &msg) const;

public:
	FilterFunctor(T *obj, bool (T::*func)(const LogMessageBase &) const) : m_instance(obj), m_func(func) {}

	bool operator()(const LogMessageBase &msg) const { return (m_instance->*m_func)(msg); }
};

class LogFilterBase
{
protected:
	LogLevel m_logging_level;

public:
	LogFilterBase() : m_logging_level(LOG_ALERT) {}
	explicit LogFilterBase(LogLevelEnum l) : m_logging_level(l) {}
	virtual ~LogFilterBase() {}

	bool check_log_level(const LogMessageBase &msg) const;
	void set_log_level(LogLevelEnum l);
	const LogLevel &get_log_level(void) const { return m_logging_level; }

	// Interface
	virtual std::string class_name(void) const = 0;
	virtual bool preformat_process(const LogMessageBase &msg) const = 0;
	virtual bool postformat_process(const LogMessageBase &msg) const = 0;
};

class LogFilter : public LogFilterBase
{
private:
	class FilterStack : private std::vector<std::unique_ptr<FilterFunctorBase> >
	{
	public:
		typedef std::vector<std::unique_ptr<FilterFunctorBase> > StackImpl;

		using StackImpl::size;
		using StackImpl::operator[];

		void push(FilterFunctorBase *filter);
	} m_preformat_filter_stack, m_postformat_filter_stack;

	using LogFilterBase::check_log_level;
	bool non_empty_output(const LogMessageBase &msg) const;

protected:
	void internal_register_filter(FilterStack &stack, FilterFunctorBase *const filter);
	bool internal_process(const FilterStack &stack, const LogMessageBase &msg) const;
	virtual void register_default_filters(void);

public:
	explicit LogFilter(LogLevelEnum l);
	virtual ~LogFilter();

	virtual std::string class_name(void) const { return "LogFilter"; }
	void register_preformat_filter(FilterFunctorBase *const filter) { internal_register_filter(m_preformat_filter_stack, filter); }
	void register_postformat_filter(FilterFunctorBase *const filter) { internal_register_filter(m_postformat_filter_stack, filter); }
	bool preformat_process(const LogMessageBase &msg) const { return internal_process(m_preformat_filter_stack, msg); }
	bool postformat_process(const LogMessageBase &msg) const { return internal_process(m_postformat_filter_stack, msg); }
};

#endif //_LOG_FILTER_H_
