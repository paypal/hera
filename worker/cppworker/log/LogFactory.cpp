#include <stdio.h>
#include "config/Config.h"
#include "LogFactory.h"
#include "utility/Timer.h"
#include <sstream>

static const char *const LOG_NAME_CVAL = "log_name";
static const char *const LOG_LEVEL_SUFFIX = "log_level";
static const char *const LOG_FILENAME_SUFFIX = "log_file";

// Static initializers
LogLevelEnum LogFactory::s_default_log_level = LOG_ALERT;
std::string LogFactory::s_last_log_name;


namespace {
	std::string recursive_config_search(const Config &config, std::string prefix, const char *suffix)
	{
		std::string key, value;

		while (!prefix.empty())
		{
			std::ostringstream os;
			os << prefix << "." << suffix;
			key = os.str();

			if (config.get_value(key, value) && !value.empty())
				return value;

			unsigned int last_dot = prefix.find_last_of('.');
			if (last_dot < prefix.length())
			{
				prefix.resize(last_dot);
				// return to the loop
			}
			else
			{
				os.str("");
				// This is the base for this prefix
				os << prefix << "." << suffix;
				key = os.str();
				break;
			}
		}

		if (key.empty())
		{
			config.get_value(suffix, value);
		}
		else
		{
			config.get_value(suffix, value);
			// Always try the raw key
			if (!config.get_value(key, value) && value.empty())
				config.get_value(suffix, value);
		}

		return value;
	}
};


// ----------------------------------------------------------------------------------------------------


LogFactoryError::LogFactoryError(const std::string &msg) :
	PPException(msg)
{
}

std::string LogFactoryError::get_name() const
{
	static std::string NAME("LogFactoryError");

	return NAME;
}


// ----------------------------------------------------------------------------------------------------


LogFactory::LogRepository &LogFactory::get_repository()
{
	/*
	 * Using the memory from heap to prevent the global destructor
	 * to destroy the repository during process exit.
	 */
	static LogFactory::LogRepository *s_logger_instances = NULL;
	if (s_logger_instances == NULL)
	{
		s_logger_instances = new LogFactory::LogRepository;
	}

	return *s_logger_instances;
}

void LogFactory::set_default_log_level(LogLevelEnum level)
{
	s_default_log_level = level;
}

/**
 * Get log level value from config file
 * This support nested naming scheme.
 * e.g.
 *   say log_name is "TM.abc.def"
 *   This will look for these config keys
 *     TM.abc.def.log_level
 *     TM.abc.log_level
 *     TM.log_level
 *     log_level
 */
std::string LogFactory::get_log_level_cval(const Config &config, const std::string &log_name)
{
	return recursive_config_search(config, log_name, LOG_LEVEL_SUFFIX);
}

std::string LogFactory::get_log_filename_cval(const Config &config, const std::string &log_name)
{
	return recursive_config_search(config, log_name, LOG_FILENAME_SUFFIX);
}

template<>
 Logger *LogFactory::get<Logger>(const Config &config, const std::string &log_name)
{
	std::string log_level = LogFactory::get_log_level_cval(config, log_name);
	std::string log_file = LogFactory::get_log_filename_cval(config, log_name);

	Logger *logger = NULL;

	if (log_file.empty())
	{
		logger = LogFactory::get<StderrLogger>(log_name, true/*create*/);
	}
	else
	{
		logger = LogFactory::get<Logger>(log_name, true/*create*/);
		logger->set_stream(log_file);
	}

	if (!log_level.empty())
	{
		logger->set_log_level(static_cast<LogLevelEnum>(StringUtil::to_int(log_level)));
	}

	return logger;
}

template<>
 StderrLogger *LogFactory::get<StderrLogger>()
{
	return LogFactory::get<StderrLogger>("STDERR", true/*create*/);
}

NullLogger *LogFactory::get_null_logger()
{
	static NullLogger *s_null_logger = new NullLogger("NULL", LOG_OFF);

	return s_null_logger;
}

/**
 * @brief Returns the logger keyed by the given log name.
 * If no log name is given, the default logger which is one of these will be returned:
 *   - a named default logger (@see DEFAULT_LOGGER_NAME)
 *   - the last logger created
 *   - null logger
 */
LogWriterBase *LogFactory::get(const std::string &log_name)
{
	LogEntry *loggerh = find(log_name);

	if (loggerh == NULL)
	{
		loggerh = find(s_last_log_name);
	}

	return (loggerh) ? loggerh : get_null_logger();
}

/**
 * @return a Pointer to a ScopedLogWriter<LogWriterBase> if the log_name is there in the repository. otherwise NULL
 */
LogFactory::LogEntry *LogFactory::find(const std::string &log_name)
{
	LogRepository::iterator it = get_repository().find(log_name);
	if (it != get_repository().end()) 
		return it->second;
	else 
		return NULL;
}


