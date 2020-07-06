#include <sstream>
#include <unistd.h>
#include "utility/Assert.h"
#include "Timer.h"

namespace
{
	unsigned long long convert_ticks_to_seconds(unsigned long long ticks, uint divisor)
	{
		// Right here, it shows the problem with using '_' before identifier. It is reserved!
		static long clk_tck = ::sysconf(_SC_CLK_TCK);
		ASSERT( clk_tck > 0 );

		// Converts to microseconds before returning in precision.
		return ((ticks * 1000000) / clk_tck / divisor);
	}

	double convert_ticks_to_seconds(double ticks, uint divisor)
	{
		// Right here, it shows the problem with using '_' before identifier. It is reserved!
		static long clk_tck = ::sysconf(_SC_CLK_TCK);
		ASSERT( clk_tck > 0 );

		// Converts to microseconds before returning in precision.
		return ((ticks * 1000000.0) / clk_tck / divisor);
	}
};

Timer::Timer() :
	m_id(0),
	m_sample_count(0),
	m_default_precision(MILLISECOND)
{
	set_id();
	clear();
}

Timer::Timer(Precision default_precision) :
	m_id(0),
	m_sample_count(0),
	m_default_precision(default_precision)
{
	set_id();
	clear();
}

/**
 * @brief Reset the timer to initial state which means 
 * m_marker = m_now = current time (i.e. an implicit start() is called)
 * m_total = m_delta = 0 (clear the cumulative total time elapsed, and the last delta)
 * and m_sample_count = 0 (clear the sample counter)
 */
void Timer::clear()
{
	set_now();
	m_sample_count = 0;
	m_wallclock.clear();
	m_sysclock.clear();
	m_userclock.clear();
}

/**
 * @brief Starts the timer and sets the marker to now.
 */
void Timer::start()
{
	set_now();
	m_wallclock.start();
	m_sysclock.start();
	m_userclock.start();
}

/**
 * @brief Stop the timer, and report the time elapsed from marker
 * @return The time elapsed since the marker.
 */
unsigned long long Timer::stop()
{
	set_now();
	m_wallclock.stop();
	m_sysclock.stop();
	m_userclock.stop();
	++m_sample_count;
	return m_wallclock.m_delta;
}

/**
 * @brief Just like doing a stop() and then a start() immediately.
 * @return the time elapsed since the marker is set.
 */
unsigned long long Timer::mark()
{
	set_now();
	m_wallclock.mark();
	m_sysclock.mark();
	m_userclock.mark();
	++m_sample_count;
	return m_wallclock.m_delta;
}

/**
 * @brief Get the unit for the precision. Default is microsecond.
 * @return A string for the time unit.
 */
const char *Timer::get_unit(Precision p) const
{
	switch (p)
	{
		case MILLISECOND:
			return "ms";
		case SECOND:
			return "s";
		case MINUTE:
			return "min";
		case MICROSECOND:
		default:
			break;
	}

	return "us";
}

/**
 * @brief Get the divisor for returning in the precision unit.
 * @return An integer that we use to divide the deltas by. Default to 1.
 */
uint Timer::get_precision_divisor(Precision p) const
{
	switch (p)
	{
		case MILLISECOND:
			return 1000;
		case SECOND:
			return 1000000;
		case MINUTE:
			return 60000000;
		case MICROSECOND:
		default:
			break;
	}

	return 1;
}

/**
 * @brief Return the name of the timer
 */
std::string Timer::get_name() const
{
	std::ostringstream os;
	os << "Timer[" << m_id << "]: ";
	return os.str();
}

unsigned long long Timer::get_system_delta_in_precision(Precision p) const
{
	return convert_ticks_to_seconds(m_sysclock.m_delta, get_precision_divisor(p));
}

unsigned long long Timer::get_user_delta_in_precision(Precision p) const
{
	return convert_ticks_to_seconds(m_userclock.m_delta, get_precision_divisor(p));
}

unsigned long long Timer::get_system_total_in_precision(Precision p) const
{
	return convert_ticks_to_seconds(m_sysclock.m_total, get_precision_divisor(p));
}

unsigned long long Timer::get_user_total_in_precision(Precision p) const
{
	return convert_ticks_to_seconds(m_userclock.m_total, get_precision_divisor(p));
}

double Timer::get_system_average_in_precision(Precision p) const
{
	return convert_ticks_to_seconds(get_system_average(), get_precision_divisor(p));
}

double Timer::get_user_average_in_precision(Precision p) const
{
	return convert_ticks_to_seconds(get_user_average(), get_precision_divisor(p));
}

std::string Timer::get_string(Precision p) const
{
	std::string out;

	format_string(out, p);
	return out;
}

std::string Timer::get_string() const
{
	return get_string(m_default_precision);
}

std::string Timer::get_detailed_string(Precision p) const
{
	std::string out;

	format_detailed_string(out, p);
	return out;
}

std::string Timer::get_detailed_string() const
{
	return get_detailed_string(m_default_precision);
}

/**
 * @brief Generate the output string. To customize, override this method.
 * @param out Reference to the string that we'll append the timer message to.
 * @param p Precision we want the output to be in
 */
void Timer::format_string(std::string &out, Precision p) const
{
	std::string unit(get_unit(p));
	unsigned long long delta=get_delta_in_precision(p);
	unsigned long long total=get_total_in_precision(p);

	std::ostringstream os;
	os << get_name() << delta << unit;

	if (m_wallclock.m_delta != m_wallclock.m_total)
	{
		os << ",total=" << total << unit;
	}
	out += os.str();
}

/**
 * @brief Generate the detailed output string, including system + user clock numbers.
 * @param out Reference to the output buffer.
 */
void Timer::format_detailed_string(std::string &out, Precision p) const
{
	std::string unit(get_unit(p));
	unsigned long long delta=get_delta_in_precision(p);
	unsigned long long total=get_total_in_precision(p);

	out.append(get_name());

	std::ostringstream os;
	os << delta << unit << ",sys=" << get_system_delta_in_precision(p) << ",user=" << get_user_delta_in_precision(p);

	if (m_wallclock.m_delta != m_wallclock.m_total)
	{
		os << "; total=" << total << unit << ",sys=" << get_system_total_in_precision(p) << ",user=" << get_user_total_in_precision(p);
	}
	out.append(os.str());
}

void Timer::set_marker(const struct timeval &tv)
{ 
	m_wallclock.m_marker = (unsigned long long)tv.tv_sec * 1000000ULL + (unsigned long long)tv.tv_usec;
}

void Timer::set_now(const struct timeval &tv)
{
	m_wallclock.m_now = (unsigned long long)tv.tv_sec * 1000000ULL + (unsigned long long)tv.tv_usec;
}

void Timer::set_id()
{
	static unsigned int s_id = 0;
	m_id = ++s_id;
}

void Timer::set_marker(const struct tms &ticks)
{
	m_sysclock.m_marker = ticks.tms_stime;
	m_userclock.m_marker = ticks.tms_utime;
}

void Timer::set_now(const struct tms &ticks)
{
	m_sysclock.m_now = ticks.tms_stime;
	m_userclock.m_now = ticks.tms_utime;
}

void Timer::set_now()
{
	struct timeval tv;

	gettimeofday(&tv, NULL);

	set_now(tv);

	struct tms ticks;

	if (::times(&ticks) != -1)
		set_now(ticks);
}
