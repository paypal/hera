#include <errno.h>
#include <unistd.h>
#include <sys/resource.h>
#include <math.h>
#include <string.h>
#include "CalLog.h"
#include "CalClient.h"
#include "CalConfig.h"
#include <log/LogFactory.h>

CalConfig* CalLog::get_config_instance()
{
	CalClient* pClient = CalClient::get_instance();
	return pClient ? pClient->get_config_instance() : NULL ;
}

void CalLog::set_exit_flag (bool val)
{
	if (val == true && CalClient::is_initialized()) 
	{
		CalConfig* pConfig = get_config_instance();
		if (pConfig)
			pConfig->disable_cal();
	}
}

bool CalLog::get_cal_enabled()
{
	if (!CalClient::is_initialized()) 
		return false;

	CalConfig* pConfig = get_config_instance();
	return pConfig ? pConfig->get_cal_enabled() : false ;
}

CalLog::CalLog (bool app_log_enabled, CalLogLevel loglevel, std::string logfile)
:	 m_loglevel (loglevel), m_logger(NULL), m_fp(NULL)
	, m_max_file_size(0)
{
	if (app_log_enabled)
	{
		// Calclient will use its deployer's default log file,
		// last opened log file, or null logger. Don't change
		// any default logger setting since it may break some
		// monitoring tools.
		m_logger = LogFactory::get();
	}
	else if (!logfile.empty())
	{
		m_fp = fopen(logfile.c_str(), "a");
		if (!m_fp)
		{
			fprintf(stderr, "Couldn't open cal log file %s: %s (errno=%d)\n", logfile.c_str(), strerror(errno), errno);
		}
		else
		{
			struct rlimit rlim;
			unsigned int long_limit=0, rlimit_size=0;
			int ret = getrlimit(RLIMIT_FSIZE, &rlim);
			if (ret != -1)
			{
				/* We are taking the max_file_size as 5% lesser than actual RLIMIT value
				 * in order to avoid program crash when muliple processes are writing 
				 * in to a local log file
				 */
				long_limit = (unsigned int) pow(2.0, (int)(sizeof(long) * 8) - 1);
				rlimit_size = (unsigned int)rlim.rlim_cur;
				m_max_file_size = (unsigned int)(std::min(long_limit, rlimit_size) * 0.95);
			}
		}
	}
}

CalLog::~CalLog()
{
	if (m_fp != NULL)
		fclose (m_fp);
}

void CalLog::write_trace_message(CalLogLevel _loglevel, int _errno, const char * _str, ...)
{
	bool cal_enable_flag = get_cal_enabled();

	if (!cal_enable_flag || _loglevel > m_loglevel || (m_logger == NULL && m_fp == NULL))
		return;

	std::string buffer = "[calclient] ";

	va_list ap;
	va_start (ap,_str);
	StringUtil::vappend_formatted(buffer, _str, ap);
	va_end (ap);

	if (_errno)
	{
		buffer = buffer + std::string(" (") + std::string(strerror(_errno)) + std::string(")");
	}

	if (m_fp)
	{
		buffer = str_level (_loglevel) + ": " + buffer + "\n";
		write_log_file(buffer);
	}
	else
	{
		m_logger=LogFactory::get();
		if (m_logger)
			m_logger->write_entry((LogLevelEnum) _loglevel, "%s", buffer.c_str());
	}
}

//
// The function is used to redirect cal messages to
// a log file
//
void CalLog::write_cal_message(const std::string& cal_msg)
{
	bool cal_enable_flag = get_cal_enabled();

	if (!cal_enable_flag || (m_logger == NULL && m_fp == NULL))
		return;

	std::string buffer = "[calmsg] ";
	buffer.append(cal_msg);

	if (m_fp)
	{
		write_log_file(buffer);
	}
	else
	{
		m_logger=LogFactory::get();
		if (m_logger)
			m_logger->write_entry(LOG_WARNING, "%s", buffer.c_str());
	}
}

void CalLog::write_log_file (const std::string& buffer)
{
	if (m_fp==NULL)
	{
		return;
	}
	unsigned int local_log_file_size = ftell( m_fp );
	std::string final_msg="File size limit is reached, further messages will be ignored\n";

	if ((local_log_file_size+buffer.length()) <= (m_max_file_size - final_msg.length()))
	{
		fprintf (m_fp, "%s", buffer.c_str());
		local_log_file_size += buffer.length();
	}
	else if (local_log_file_size+final_msg.length() <= m_max_file_size)
	{
		fprintf (m_fp, "%s", final_msg.c_str());
		local_log_file_size += final_msg.length();
	}
	fflush(m_fp);
}

std::string CalLog::str_level (CalLogLevel _loglevel)
{
	std::string loglevel;

	switch (_loglevel) {
		case CAL_LOG_ALERT:
			loglevel = "ALERT";
			break;
		case CAL_LOG_WARNING:
			loglevel = "WARNING";
			break;
		case CAL_LOG_INFO:
			loglevel = "INFO";
			break;
		case CAL_LOG_DEBUG:
			loglevel = "DEBUG";
			break;
		case CAL_LOG_VERBOSE:
			loglevel = "VERBOSE";
			break;
		default:
			loglevel = "";
			break;
	}

	return loglevel;
}
