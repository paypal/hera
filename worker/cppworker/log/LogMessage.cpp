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
