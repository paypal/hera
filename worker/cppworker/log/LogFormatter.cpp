#include <time.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include "LogFormatter.h"
#include "LogLevel.h"
#include "LogMessage.h"



void LogCustomText::format(const LogMessageBase &msg) const
{
	msg.output_buffer() += m_text;
}

void LogHumanTime::format(const LogMessageBase &msg) const
{
	static char buf[32];

	time_t t = msg.seconds();
	strftime(buf, sizeof(buf), "%m/%d/%Y %H:%M:%S", localtime(&t));
	msg.output_buffer() += buf;
}

void LogHumanTimeMicro::format(const LogMessageBase &msg) const
{
	LogHumanTime base_op;
	base_op.format(msg);
	char tmbuff[256];
	sprintf(tmbuff, ".%06u", (unsigned int)msg.microseconds());
	msg.output_buffer().append(tmbuff);
}

void LogLogEntry::format(const LogMessageBase &msg) const
{
	if (msg.ap())
		StringUtil::vappend_formatted(msg.output_buffer(), msg.message().c_str(), *(const_cast<va_list*>(msg.ap())));
}

void LogLogLevel::format(const LogMessageBase &msg) const
{
	char tmbuff[256];
	sprintf(tmbuff, "%d",msg.level());
	msg.output_buffer().append(tmbuff);
}

void LogLogLevelName::format(const LogMessageBase &msg) const
{
	msg.output_buffer() += msg.level_name();
}

void LogLogName::format(const LogMessageBase &msg) const
{
	msg.output_buffer() += msg.log_name();
}

void LogProcessID::format(const LogMessageBase &msg) const
{
	std::ostringstream os;
	os << getpid();
	msg.output_buffer().append(os.str());
}

void LogUnixTime::format(const LogMessageBase &msg) const
{
	std::ostringstream os;
	os << msg.seconds();
	msg.output_buffer().append(os.str());
}

void LogUnixTimeMicro::format(const LogMessageBase &msg) const
{
	// Yes, we know we could have used std::string::append_formatted(), but
	// this function is literally called a metric bazillion times a
	// second from occ.

	// std::string::append_formatted() is unfortunately not nearly as speedy as it
	// could be, so we wrote our own special function here.

	std::ostringstream os;
	os << msg.seconds() << ".";
	std::ostringstream osms;
	osms << msg.microseconds();
	std::string unix_time_microseconds = osms.str();

	if (unix_time_microseconds.length() > 6)
	{
		unix_time_microseconds.resize(6);
	}
	else // < 6 -- pad unix_time_seconds with 0s
	{
		uint num_zeros = (6 - unix_time_microseconds.length());
		for (int i = 0; i < num_zeros; i++)
			os << '0';
	}

	msg.output_buffer().append(os.str());
	msg.output_buffer().append(unix_time_microseconds);
}

LogFormatter::LogFormatter()
{
}

LogFormatter::LogFormatter(const char *fmt) : LogFormatterBase(fmt)
{
	set_format(fmt);
}

LogFormatter::~LogFormatter()
{
}

void LogFormatter::set_format(const char *fmt)
{
	unsigned int i = 0;
	unsigned int start = 0;
	std::string label;
	bool in_token = false;

	if (fmt==NULL) {
		return;
	}

	m_ops.clear();
	m_format = fmt;

	//go through and tokenize
	for (; fmt[i]; ++i)
	{
		if (in_token || ((fmt[i] == '%') && (fmt[i + 1] != 0)))
		{
			bool valid_token = true;

			if ((fmt[i] == '%'))
			{
				//a token break
				//see if we need to add special stuff
				if (i > start)
				{
					m_ops.push_back(std::unique_ptr<LogFormatBase>(new LogCustomText(std::string(fmt + start, i - start))));
				}
				//add the special item
				++i;
			}

			switch(fmt[i]) {
				case LogFormatBase::HumanTime:
					m_ops.push_back(std::unique_ptr<LogFormatBase>(new LogHumanTime()));
					break;
				case LogFormatBase::HumanTimeMicro:
					m_ops.push_back(std::unique_ptr<LogFormatBase>(new LogHumanTimeMicro()));
					break;
				case LogFormatBase::LogEntry:
					m_ops.push_back(std::unique_ptr<LogFormatBase>(new LogLogEntry()));
					break;
				case LogFormatBase::LogLevel:
					m_ops.push_back(std::unique_ptr<LogFormatBase>(new LogLogLevel()));
					break;
				case LogFormatBase::LogLevelName:
					m_ops.push_back(std::unique_ptr<LogFormatBase>(new LogLogLevelName()));
					break;
				case LogFormatBase::LogName:
					m_ops.push_back(std::unique_ptr<LogFormatBase>(new LogLogName()));
					break;
				case LogFormatBase::ProcessID:
					m_ops.push_back(std::unique_ptr<LogFormatBase>(new LogProcessID()));
					break;
#if 0
				{
					pid_t pid = getpid();
					std::string tmp;

					tmp.fmt_uint(pid);
					m_ops.append(SmartPointer<LogFormatBase>(new LogCustomText(tmp)));
					break;
				}
#endif
				case LogFormatBase::UnixTime:
					m_ops.push_back(std::unique_ptr<LogFormatBase>(new LogUnixTime()));
					break;
				case LogFormatBase::UnixTimeMicro:
					m_ops.push_back(std::unique_ptr<LogFormatBase>(new LogUnixTimeMicro()));
					break;
				case '{': // Begin label
				{
					unsigned int j = i + 1;
					// skip forward to closing brace.
					for (; (fmt[j] && fmt[j] != '}'); ++j);

					label.clear();
					if (fmt[j] == '}')
					{
						label = std::string(&(fmt[i+1]), j-i-1);

						start = j + 1;
						i = j;
						in_token = true;
						valid_token = false;
					}

					break;
				}
				default:
					valid_token = false;
					in_token = false;
					label.clear();
					break;
			} // switch

			if (valid_token)
			{
				label.clear();
				in_token = false;
				start = i + 1;
			}
		} //end if special
	} //end for
	//check if there was some left
	if (fmt[start] != 0)
	{
		m_ops.push_back(std::unique_ptr<LogFormatBase>(new LogCustomText(std::string(fmt + start, i - start))));
	}
}

void LogFormatter::format(const LogMessageBase &msg) const
{
	int ops_cnt = m_ops.size();

	// clear the buffer
	msg.clear_output();

	//iterate through the objects in the format
	for (int i = 0; i < ops_cnt; ++i)
	{
		m_ops[i]->format(msg);
	}
}
