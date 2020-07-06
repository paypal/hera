#ifndef _LOG_H_
#define _LOG_H_

/*
	logging class
*/

#include <sys/time.h>
#include <stdarg.h>
#include <string>
#include <vector>
#include <ostream>
#include "LogLevel.h"
#include "utility/Object.h"

class Timer;

//LogFormat object is a placeholder for future enhancements

enum LogFormatType { CustomText, LogEntry, UnixTime, Level, LevelText, UnixTimeMicro, HumanTime, HumanTimeMicro };

class LogFormat : public Object
{
public:
	LogFormatType type;
	std::string fmt;

	LogFormat(LogFormatType t);
	LogFormat(LogFormatType t, const std::string& special);
};

class Log
{
protected:
	std::ostream * output;
	// level 0 is maximum alert...goes down from there
	LogLevelEnum level;
	bool mEnable_cal;

public:
	static Log * instance;

	//use the outputstream for printing
	//with the given level...anything greater than l will not be logged
	Log(std::ostream * out,LogLevelEnum l);
	~Log();

	// %s - the entry to be logged
	// %u - the unix time of the event
	// %U - the unix time of the event, with microseconds
	// %h - the human-readable time
	// %H - the human-readable time, with microseconds
	// %l - the level (priority)
	// %t - textual version of level
	// more to come
	void set_format(const char * fmt);

	// adjust the log level
	void set_log_level(LogLevelEnum l) { level = l; }
	LogLevelEnum get_log_level(void) const { return level; }

	//write an entry to the log file with the given level
	void write_entry(LogLevelEnum l, const char * str, ...);
	void vwrite_entry(LogLevelEnum l, const char * str, va_list ap);
	int get_fd();
	void dup2_stderr();
	void log_time(LogLevelEnum l, Timer &t, const char * str, ...);
	void set_enable_cal(bool _switch){mEnable_cal = _switch;} 

private:
	std::string buffer;

	//a vector of LogFormat objects
	std::vector<LogFormat> format;
};


#endif
