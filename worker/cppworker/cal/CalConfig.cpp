#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sstream>
#include "CalConfig.h"
#include <config/CDBConfig.h>
#include <config/MultiConfig.h>
#include <utility/StringUtil.h>

const int DefaultMetricsTimeout=1800; //seconds. 30 minutes
CalConfig::CalConfig(Config* _config, bool readVersionCdb, const char* _poolname, const char* _label_affix) :
	m_config(_config),
	m_logger (NULL),
	m_poolname(_poolname),
	m_label_affix(_label_affix),
	m_cal_enabled(false),
	m_backtrace_enabled(false),
	m_enable_pool_stack(false),
	m_maxPoolStackLength(2048),
	m_proxybased_corrid(false),
	m_connect_time_secs(5),
	m_ring_buffer_size(32000),
	m_msg_buffer_size(300),
	m_label(""),
	m_handler_type("socket"),
	m_loglevel(CAL_LOG_ALERT),
	m_app_log_enabled(true),
	m_logfile("logCalClient.txt"),
	m_host("127.0.0.1"),
	m_port("1118")
{
	create_multiconfig(_config, readVersionCdb);

	if (m_poolname.empty() && (m_config != NULL))
		m_config->get_value("cal_pool_name", m_poolname);

	initialize_logger();
	initialize_socket_config();
	initialize_other_parameters();
}

void CalConfig::create_multiconfig(Config* _config, bool readVersionCdb)
{
	std::string val;
	if(readVersionCdb && (!_config->get_value("release_product_number", val) ))
	{
		// This config will contain all configuration information.
		MultiConfig* cal_cfg = new MultiConfig;

		// Add version information
		CDBConfig *version_cfg = new CDBConfig("./version.cdb");
		cal_cfg->add_config(version_cfg);

		// add the _config parameter to the new MultiConfig.
		cal_cfg->add_config(_config);

		m_config = cal_cfg ;
	}
}

void CalConfig::initialize_logger()
{
	std::string str;
	m_config->get_value("cal_log_level", str);
	if (str.length() == 0)
		m_loglevel = CAL_LOG_WARNING;
	else
	{
		m_loglevel = (CalLogLevel) StringUtil::to_int(str);
		if (m_loglevel < CAL_LOG_ALERT)
			m_loglevel = CAL_LOG_ALERT;
		else if (m_loglevel > CAL_LOG_VERBOSE)
			m_loglevel = CAL_LOG_VERBOSE;
	}

	m_app_log_enabled = m_config->is_switch_enabled("cal_enable_mlog", true);
	std::string strLogFile;
	if (m_config->get_value("cal_log_file", strLogFile))
		m_logfile = strLogFile;

	m_logger = new CalLog (m_app_log_enabled, m_loglevel, m_logfile);
}

void CalConfig::initialize_socket_config()
{
	m_SiteViewAddr.sin_family		= AF_INET;
	std::string strPortNum, strConnectTime, strHostName, strRingBufSize;

	if (m_config->get_value("cal_socket_machine_port", strPortNum))
		m_port = strPortNum;;

	if (m_config->get_value("cal_socket_machine_name", strHostName))
		m_host = strHostName;

	if (m_config->get_value("cal_socket_connect_time_secs", strConnectTime))
		m_connect_time_secs = StringUtil::to_int(strConnectTime);

	if (m_config->get_value("cal_socket_ring_buffer_size", strRingBufSize))
		m_ring_buffer_size = StringUtil::to_int(strRingBufSize);


	unsigned short portNum = StringUtil::to_int(m_port);
	m_SiteViewAddr.sin_port	= htons (portNum);

	/* This use is obsolete on Linux, but the approved way, using
	   inet_aton(3, isn't available on Solaris.
	 */
	m_SiteViewAddr.sin_addr.s_addr = inet_addr (m_host.c_str());
	if (m_SiteViewAddr.sin_addr.s_addr == INADDR_NONE)
	{
		struct hostent * he = gethostbyname (m_host.c_str()); 
		if (he != NULL)
			memcpy (&m_SiteViewAddr.sin_addr.s_addr, he->h_addr, he->h_length);
	}
}

void CalConfig::initialize_other_parameters()
{
	m_cal_enabled = m_config->is_switch_enabled("enable_cal", false);
	m_enable_pool_stack = m_config->is_switch_enabled("cal_pool_stack_enable", false);
	m_backtrace_enabled = m_config->is_switch_enabled("cal_enable_backtrace", false);
	m_proxybased_corrid = m_config->is_switch_enabled("cal_enable_proxybased_corrid", false);

	std::string str;
	m_config->get_value("cal_max_pool_stack_size" , str);
	if(str.length() != 0) 
	{
		unsigned int msize = StringUtil::to_int(str);
		if (msize < m_maxPoolStackLength)
			m_maxPoolStackLength = msize;
	}

	std::string environment, msg_buf_size, release_product_number, release_build_number, handler_type;
	if (m_config->get_value("cal_handler", handler_type))
		m_handler_type = handler_type;

	if (m_config->get_value("cal_message_buffer_size", msg_buf_size))
		m_msg_buffer_size = StringUtil::to_int(msg_buf_size);

	if (!m_config->get_value("cal_environment", environment) || environment.empty())
		environment = "PayPal";

	const char* RELEASE_UNKNOWN = "unknown";
	if (!m_config->get_value("release_product_number", release_product_number)) {
		release_product_number = RELEASE_UNKNOWN;
	}
	if (!m_config->get_value("release_build_number", release_build_number)) {
		release_build_number = RELEASE_UNKNOWN;
	}

	create_label (environment, release_product_number, release_build_number);
}

void CalConfig::create_label (std::string& _environment, std::string& _version, std::string& _build)
{
	char host[40];
	if (::gethostname (host, sizeof(host)) != 0 && m_logger)
		m_logger->write_trace_message (CAL_LOG_INFO, errno, "GetHostName Failed.");
	host[sizeof(host)-1] = '\0';

	time_t t = time(NULL);        
	struct tm st;
	localtime_r(&t, &st);
	_build.append(m_label_affix);
#ifdef PPSB
	_build.append("-PPSB");
#endif
	std::ostringstream os;
	os << m_label;
	os << "SQLLog for " << m_poolname << ":" << host << "\r\n";
	os << "Environment: " << _environment << "\r\n";
	os << "Label: " << m_poolname << "-" << _version << "-" << _build;
	char buff[256];
	sprintf(buff, "Start: %02d-%02d-%04d %02d:%02d:%02d\r\n",
			st.tm_mday, st.tm_mon+1, st.tm_year+1900,
			st.tm_hour, st.tm_min, st.tm_sec);
	os << buff;
	os << "\r\n";
	m_label = os.str();

	if (m_logger)
		m_logger->write_trace_message (CAL_LOG_VERBOSE, 0, "LABEL \"%s\"", m_label.c_str());
}


CalConfig::~CalConfig()
{
	if (m_logger)
		delete m_logger;
}
