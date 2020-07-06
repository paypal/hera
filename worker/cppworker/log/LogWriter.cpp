#ifndef LOG_WRITER_CPP
#define LOG_WRITER_CPP

#include <log/LogMessage.h>
#include <log/LogFactory.h>
#include <cal/CalMessages.h>
#include <stdio.h>
#include <unistd.h>
#include <string>
#include <memory>

template <class T, class U>
 void LogWriterTemplate<T, U>::write_entry(const LogMessageBase &msg)
{
	if (m_filter.preformat_process(msg))
	{
		internal_write_entry(msg);
	}
}

template <class T, class U>
 void LogWriterTemplate<T, U>::vwrite_entry(LogLevelEnum l, const char *msg, va_list* _ap )
{
	if (get_log_level() < l) { return; }
 	std::unique_ptr<LogMessageBase> lmsg(LogMessageFactory<LogMessage>::get(l, m_name, msg, _ap));
	if (lmsg)
	{
		write_entry(*lmsg);
	}
}

template <class T, class U>
 void LogWriterTemplate<T, U>::write_entry(LogLevelEnum l, const char *msg, ...)
{
	if (get_log_level() < l) { return; }
	va_list ap;

	va_start(ap, msg);
 	std::unique_ptr<LogMessageBase> lmsg(LogMessageFactory<LogMessage>::get(l, m_name, msg, &ap));
	if (lmsg)
	{
		write_entry(*lmsg);
	}

	va_end(ap);
}

template <class T, class U>
 void LogWriterTemplate<T, U>::log_time(LogLevelEnum l, Timer &t, const char *msg, ...)
{
	if (get_log_level() < l) { return; }
	va_list ap;

	va_start(ap, msg);
	std::unique_ptr<LogMessageBase> lmsg(LogMessageFactory<LogMessage>::get(l, m_name, t, msg, &ap));
	if (lmsg)
	{
		write_entry(*lmsg);
	}

	va_end(ap);
}

template <class T, class U>
 void LogWriterTemplate<T, U>::log_time_detailed(LogLevelEnum l, Timer &t, const char *msg, ...)
{
	if (get_log_level() < l) { return; }
	va_list ap;

	va_start(ap, msg);
	std::unique_ptr<LogMessageBase> lmsg(LogMessageFactory<LogMessage>::get_detailed(l, m_name, t, msg, &ap));
	if (lmsg)
	{
		write_entry(*lmsg);
	}

	va_end(ap);
}


//------------------------------------------------------------------------------------------------


template <class T, class U>
 LogStreamWriter<T, U>::LogStreamWriter(const std::string &log_name, LogLevelEnum l) : 
	 LogWriterTemplate<T, U>(log_name, l), m_stream(NULL)
{
}

template <class T, class U>
 LogStreamWriter<T, U>::LogStreamWriter(const std::string &log_name, LogLevelEnum l, std::ostream *stream) : 
	 LogWriterTemplate<T, U>(log_name, l), m_stream(stream)
{
}

template <class T, class U>
 void LogStreamWriter<T, U>::internal_write_entry(const LogMessageBase &msg)
{
	this->m_formatter.format(msg);
	if (m_stream && this->m_filter.postformat_process(msg))
	{
		// Send alert to CAL but be careful not to fall into recursive loop
		if (msg.level() == LOG_ALERT && (msg.output_buffer().find(" [cal") == std::string::npos))
		{
			CalEvent t("LOGGER"); t.SetName("ALERT"); t.SetStatus("0");
			t.AddData("Data", msg.output_buffer());
		}
		*m_stream << msg.get_output();
		m_stream->flush();
	}
}

template <class T, class U>
 void LogStreamWriter<T, U>::set_stream(std::ostream *stream)
{
	m_stream = stream;
}

//------------------------------------------------------------------------------------------------


template <class T, class U>
 LogFileWriter<T, U>::LogFileWriter(const std::string &log_name, LogLevelEnum l) :
	 LogStreamWriter<T, U>(log_name, l)
{
}

template <class T, class U>
void  LogFileWriter<T, U>::set_stream(const std::string &filename, bool append, bool truncate)
{
	std::ios_base::openmode mode = std::fstream::out;
	if (append) mode |= std::ios_base::app;
	if (truncate) mode |= std::ios_base::trunc;
	LogStreamWriter<T, U>::set_stream(new std::ofstream(filename.c_str(), mode));
}

//------------------------------------------------------------------------------------------------


template <class T, class U>
 StderrWriter<T, U>::StderrWriter(const std::string &log_name, LogLevelEnum l) :
	 LogFileWriter<T, U>(log_name, l)
{
	LogFileWriter<T, U>::set_stream("/dev/stderr");
}


//------------------------------------------------------------------------------------------------


template <class T, class U>
 NullWriter<T, U>::NullWriter(const std::string &log_name, LogLevelEnum l) :
	 LogFileWriter<T, U>(log_name, l)
{
	LogFileWriter<T, U>::set_stream("/dev/null", true/*append*/);
}


//------------------------------------------------------------------------------------------------

#endif // LOG_WRITER_CPP