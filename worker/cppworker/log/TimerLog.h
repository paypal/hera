#ifndef _TIMER_LOG_H_
#define _TIMER_LOG_H_

#include "utility/Timer.h"
#include "log/LogLevel.h"
#include <string>

class LogWriterBase;
class CalActivity;

/**
* Class to keep logging methods in a common place to avoid code duplication across frameworks
* This will use data from its base class Timer
*/

class TimerLog : public Timer
{
public:
	/**
	* method to log CPU usage in CAL
	*/
	void log(CalActivity &_cal, Precision _p = MILLISECOND);

	/**
	* method to log CPU usage in system log file
	*/
	void log(LogWriterBase &_logger, Precision _p = MILLISECOND, LogLevelEnum _level = LOG_VERBOSE);

protected:
	/**
	* method to construct cpu usage string
	*/
	std::string get_time_string(unsigned long long _cpu_used_ticks, unsigned long long _cpu_used_sec, Precision _p) const;

	/**
	* method to calculate CPU percentage used
	*/
	float get_cpu_percentage() const;
};

#endif

