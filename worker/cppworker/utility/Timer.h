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
#ifndef _TIMER_H_
#define _TIMER_H_

#include <sys/time.h>
#include <sys/times.h>
#include <string>

/**
 * Timer measures 3 things:
 *     - wallclock time (via gettimeofday())
 *     - sys and user cpu time (via times())
 * 
 * In my experience, cpu time is measured at the 10 ms granularity.  I
 * don't usually have any trouble getting microsecond level time from
 * the wall clock.  Whether you will observe any delay from the wall
 * clock in just measuring a few operations, depends on the speed of
 * the machine and the overhead of the system call gettimeofday().  In
 * my experience on the lower hypers, two successive calls to
 * gettimeofday() usually advance the clock.
 * 
 * start() takes a current reading from gettimeofday() and times().
 * stop() calculates delta since last start() or mark().
 * mark() calculates delta since last start() or mark() and then calls start().
 * 
 * The methods get_delta() (wallclock), get_system_delta() (sys cpu),
 * and get_user_delta() (user cpu) return the delta calculated by the
 * last stop() or mark() call.  They do not report measurements based
 * on the current time.
 *
 * deltas are accumulated into a total, so you can use the Timer
 * start() and stop() within a loop and get some totals at the end
 * (see get_total(), get_system_total(), and get_user_total()).
 * Personally I like to use get_detailed_string() when i'm building my
 * report, because it shows all 3 values for delta and for total (if
 * they differ.)  When using get_detailed_string(), setting the
 * default precision can be important if your totals are less than a
 * millisecond (the default precision).
 */
class Timer
{
public:
	enum Precision {
		MICROSECOND,
		MILLISECOND,
		SECOND,
		MINUTE,
	};

private:
	struct DataStore {
		unsigned long long m_total;
		unsigned long long m_delta;
		unsigned long long m_marker;
		unsigned long long m_now;

		DataStore() : m_total(0), m_delta(0), m_marker(0), m_now(0) {}
		~DataStore() {}

		void clear(void) { m_total = m_delta = 0; m_marker = m_now; }
		void start(void) { m_marker = m_now; }
		void stop(void) { m_delta = m_now - m_marker; m_total += m_delta; }
		void mark(void) { m_delta = m_now - m_marker; m_total += m_delta; start(); }
	};

	uint m_id;          //!< ID of this timer.
	uint m_sample_count;//!< Store the number of sample stored in m_total
	DataStore m_wallclock;	//!< Data store for wallclock data (data in microseconds)
	DataStore m_sysclock;	//!< Data store for system clock data (data in ticks)
	DataStore m_userclock;	//!< Data store for user clock data (data in ticks)
	Precision m_default_precision;  //!< What default precision we should use to display the elapsed time.

public:
	Timer();
	Timer(Precision default_precision);
	virtual ~Timer() {}

	void clear(void);
	void start(void);
	unsigned long long stop(void);
	unsigned long long mark(void);

	//@{
	//!< @return A string representation of the elapsed time since last click and total elapsed time.
	std::string get_string(Precision p) const;
	std::string get_string(void) const;
	std::string get_detailed_string(Precision p) const;
	std::string get_detailed_string(void) const;
	//@}

	uint get_sample_count(void) const { return m_sample_count; }

	unsigned long long get_delta(void) const { return m_wallclock.m_delta; }
	unsigned long long get_delta_in_precision(Precision p) const { return m_wallclock.m_delta/get_precision_divisor(p); }
	unsigned long long get_delta_in_precision(void) const { return get_delta_in_precision(m_default_precision); }

	unsigned long long get_system_delta(void) const { return m_sysclock.m_delta; }
	unsigned long long get_system_delta_in_precision(Precision p) const;
	unsigned long long get_system_delta_in_precision(void) const { return get_system_delta_in_precision(m_default_precision); }

	unsigned long long get_user_delta(void) const { return m_userclock.m_delta; }
	unsigned long long get_user_delta_in_precision(Precision p) const;
	unsigned long long get_user_delta_in_precision(void) const { return get_user_delta_in_precision(m_default_precision); }

	unsigned long long get_total(void) const { return m_wallclock.m_total; }
	unsigned long long get_total_in_precision(Precision p) const { return m_wallclock.m_total/get_precision_divisor(p); }
	unsigned long long get_total_in_precision(void) const { return get_total_in_precision(m_default_precision); }

	unsigned long long get_system_total(void) const { return m_sysclock.m_total; }
	unsigned long long get_system_total_in_precision(Precision p) const;
	unsigned long long get_system_total_in_precision(void) const { return get_system_total_in_precision(m_default_precision); }

	unsigned long long get_user_total(void) const { return m_userclock.m_total; }
	unsigned long long get_user_total_in_precision(Precision p) const;
	unsigned long long get_user_total_in_precision(void) const { return get_user_total_in_precision(m_default_precision); }

	double get_average(void) const { return (m_sample_count == 0) ? m_wallclock.m_total : (m_wallclock.m_total/static_cast<double>(m_sample_count)); }
	double get_average_in_precision(Precision p) const { return get_average()/get_precision_divisor(p); }
	double get_average_in_precision(void) const { return get_average_in_precision(m_default_precision); }

	double get_system_average(void) const { return (m_sample_count == 0) ? m_sysclock.m_total : (m_sysclock.m_total/static_cast<double>(m_sample_count)); }
	double get_system_average_in_precision(Precision p) const;
	double get_system_average_in_precision(void) const { return get_system_average_in_precision(m_default_precision); }

	double get_user_average(void) const { return (m_sample_count == 0) ? m_userclock.m_total : (m_userclock.m_total/static_cast<double>(m_sample_count)); }
	double get_user_average_in_precision(Precision p) const;
	double get_user_average_in_precision(void) const { return get_user_average_in_precision(m_default_precision); }

	unsigned long long get_marker(void) const { return m_wallclock.m_marker; }
	unsigned long long get_system_marker(void) const { return m_sysclock.m_marker; }
	unsigned long long get_user_marker(void) const { return m_userclock.m_marker; }

	unsigned long long get_now(void) const { return m_wallclock.m_now; }
	unsigned long long get_system_now(void) const { return m_sysclock.m_now; }
	unsigned long long get_user_now(void) const { return m_userclock.m_now; }

	uint get_id(void) const { return m_id; }
	Precision get_default_precision(void) const { return m_default_precision; }
	void set_default_precision(Precision precision) { m_default_precision = precision; }
	uint get_sample_size(void) const { return m_sample_count; }

protected:
	virtual void set_marker(const struct timeval &tv);
	virtual void set_now(const struct timeval &tv);
	virtual void set_marker(const struct tms &ticks);
	virtual void set_now(const struct tms &ticks);
	virtual const char *get_unit(Precision p) const;
	virtual uint get_precision_divisor(Precision p) const;
	virtual std::string get_name(void) const;
	virtual void format_string(std::string &out, Precision p) const;
	virtual void format_detailed_string(std::string &out, Precision p) const;

private:
	void set_now(void);
	void set_id(void);	//!< This uses a static counter to autoincrement the ID
};

#endif //_TIMER_H_
