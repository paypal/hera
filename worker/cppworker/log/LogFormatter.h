#ifndef _LOG_FORMATTER_H_
#define _LOG_FORMATTER_H_

#include <string>
#include <vector>
#include <memory>

class LogMessageBase;

class LogFormatterBase
{
protected:
	std::string m_format;

public:
	LogFormatterBase() {}
	explicit LogFormatterBase(const char *fmt) : m_format(fmt) {}
	virtual ~LogFormatterBase() {}

	const std::string &get_format(void) const { return m_format; }

	virtual std::string class_name(void) const = 0;
	virtual void set_format(const char *fmt) = 0;
	virtual void format(const LogMessageBase &msg) const = 0;

protected:
	//!< @brief This can be use to transform the log message after formatting
	virtual void postformat_transform(const LogMessageBase &msg) const = 0;
};


class LogFormatBase
{
protected:
	char m_token;

public:
	enum LogFormatType {
		Backtrace='b',
		CustomText='/',
		HumanTime='h',
		HumanTimeMicro='H',
		LogEntry='s',
		LogLevel='l',
		LogLevelName='t',
		LogName='N',
		ProcessID='p',
		UnixTime='u',
		UnixTimeMicro='U',
	};


	LogFormatBase(const char token) : m_token(token) {}
	virtual ~LogFormatBase() {}

	const char &token(void) const { return m_token; }
	virtual void format(const LogMessageBase &msg) const = 0;
};

class LogCustomText : public LogFormatBase
{
private:
	std::string m_text;

public:
	LogCustomText(const std::string &text) : LogFormatBase(LogFormatBase::CustomText), m_text(text) {}

	void format(const LogMessageBase &msg) const;
};

class LogHumanTime : public LogFormatBase
{
public:
	LogHumanTime() : LogFormatBase(HumanTime) {}

	void format(const LogMessageBase &msg) const;
};

class LogHumanTimeMicro : public LogFormatBase
{
public:
	LogHumanTimeMicro() : LogFormatBase(LogFormatBase::HumanTimeMicro) {}

	void format(const LogMessageBase &msg) const;
};

class LogLogEntry : public LogFormatBase
{
public:
	LogLogEntry() : LogFormatBase(LogFormatBase::LogEntry) {}

	void format(const LogMessageBase &msg) const;
};

class LogLogLevel : public LogFormatBase
{
public:
	LogLogLevel() : LogFormatBase(LogFormatBase::LogLevel) {}

	void format(const LogMessageBase &msg) const;
};

class LogLogLevelName : public LogFormatBase
{
public:
	LogLogLevelName() : LogFormatBase(LogFormatBase::LogLevelName) {}

	void format(const LogMessageBase &msg) const;
};

class LogLogName : public LogFormatBase
{
public:
	LogLogName() : LogFormatBase(LogFormatBase::LogName) {}

	void format(const LogMessageBase &msg) const;
};

class LogProcessID : public LogFormatBase
{
public:
	LogProcessID() : LogFormatBase(LogFormatBase::ProcessID) {}

	void format(const LogMessageBase &msg) const;
};

class LogUnixTime : public LogFormatBase
{
public:
	LogUnixTime() : LogFormatBase(LogFormatBase::UnixTime) {}

	unsigned int tokenize(const char *fmt);
	void format(const LogMessageBase &msg) const;
};

class LogUnixTimeMicro : public LogFormatBase
{
public:
	LogUnixTimeMicro() : LogFormatBase(LogFormatBase::UnixTimeMicro) {}

	unsigned int tokenize(const char *fmt);
	void format(const LogMessageBase &msg) const;
};



class LogFormatter : public LogFormatterBase
{
private:
	std::vector<std::unique_ptr<LogFormatBase> > m_ops;

//	void clean_ops(void);

public:
	void set_format(const char *fmt);
	void format(const LogMessageBase &msg) const;

	LogFormatter();
	explicit LogFormatter(const char *fmt);
	virtual ~LogFormatter();

	virtual std::string class_name(void) const { return "LogFormatter"; }

protected:
	virtual void postformat_transform(const LogMessageBase &msg) const { /* does nothing */ }
};

#endif //_LOG_FORMATTER_H_
