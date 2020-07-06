#include "log/TimerLog.h"
#include "cal/CalMessages.h"
#include "log/LogWriter.h"
#include "utility/Assert.h"

void TimerLog::log(CalActivity &_cal, Precision _p)
{
	static long clk_tck = ::sysconf(_SC_CLK_TCK);
	ASSERT( clk_tck > 0 );

	_cal.AddData("user_cpu_time", get_time_string(get_user_total(), get_user_total_in_precision(_p), _p));

	_cal.AddData("sys_cpu_time", get_time_string(get_system_total(), get_system_total_in_precision(_p), _p));

	//get_total() - m_wallclock.m_total is in microseconds
	_cal.AddData("real_time_elapsed", get_time_string((get_total() * clk_tck / 1000000),
	get_total_in_precision(_p), _p));

	char tmbuff[256];
	sprintf(tmbuff, "%0.2f", get_cpu_percentage());

	_cal.AddData("cpu_used_perc", tmbuff);
}

void TimerLog::log(LogWriterBase &_logger, Precision _p, LogLevelEnum _level)
{
	static long clk_tck = ::sysconf(_SC_CLK_TCK);
	ASSERT( clk_tck > 0 );

	WRITE_LOG_ENTRY(&_logger, _level, "cpu usage: user = %s sys = %s real_time = %s cpu_percentage = %0.2f",
		get_time_string(get_user_total(), get_user_total_in_precision(_p), _p).c_str(),
		get_time_string(get_system_total(), get_system_total_in_precision(_p), _p).c_str(),
		get_time_string((get_total() * clk_tck / 1000000), get_total_in_precision(_p), _p).c_str(),
		get_cpu_percentage());
}

float TimerLog::get_cpu_percentage() const
{
	//get_total() - m_wallclock.m_total is in microseconds, so get everything in micro sec
	 return (float)(get_user_total_in_precision(Timer::MICROSECOND) + get_system_total_in_precision(Timer::MICROSECOND)) * 100 / get_total();
}

std::string TimerLog::get_time_string(unsigned long long _cpu_used_ticks, unsigned long long _cpu_used_sec, Precision _p) const
{
	std::ostringstream os;
	os << _cpu_used_ticks << "ticks" << _cpu_used_sec, get_unit(_p);

	return os.str();
}

