#ifndef __CALCONFIG_H
#define __CALCONFIG_H

#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string>

#include "CalLog.h"

class Config;
class CalLog;
/**
 * This class is used to read cal_client.cdb and version.cdb parameters
 */
class CalConfig
{
public:
	CalConfig(Config* _config, bool readVersionCdb, const char* _poolname=NULL, const char* _label_affix=NULL);
	~CalConfig();
	
	/**
	 * Getter method for Config instance.
	 */
	Config* get_config() const  { return m_config; }

	/**
	 * Getter method for poolname.
	 */
	std::string get_poolname() const { return m_poolname; }

	/**
	 * Getter method for logger instance.
	 */
	CalLog* get_logger() const { return m_logger; }

	/**
	 * Getter method for cal_enabled flag.
	 */
	bool get_cal_enabled() const { return m_cal_enabled; }

	/**
	 * Getter method for pool stack enable flag.
	 */
	bool get_poolstack_enabled() const { return m_enable_pool_stack; }

	/**
	 * Getter method for backtrace enable flag.
	 */
	bool get_backtrace_enabled() const { return m_backtrace_enabled; }

	/**
	 * Getter method for pool stack length.
	 */
	unsigned long get_poolstack_length() const { return m_maxPoolStackLength; }
	
	/**
	 * Getter method for pool stack length.
	 */
	unsigned long get_proxybased_corrid() const { return m_proxybased_corrid; }
	
	/**
	 * Getter method for socket ip and port.
	 */
	sockaddr_in get_siteview_addr() const { return m_SiteViewAddr; }


	/**
	 * Getter method for socket connect time, time at which calclient has to connect
	 * to caldaemon next time.
	 */
	int get_socket_connect_time()const { return m_connect_time_secs; }

	
	/**
	 * Getter method for ring buffer size.
	 */
	int get_ring_buffer_size()const { return m_ring_buffer_size; }
	
	/**
	 * Getter method for message buffer size.
	 */
	int get_msg_buffer_size()const { return m_msg_buffer_size; }
	
	/**
	 * Getter method for label.
	 */
	std::string get_label()const        { return m_label; }
	
	/**
	 * Getter method for handler type (file/socket).
	 */
	std::string get_handler_type()const { return m_handler_type; }
	
	/**
	 * Method to disable CAL.
	 */
	void disable_cal() { m_cal_enabled = false; }

	/**
	 * Getter method for loglevel.
	 */
	CalLogLevel get_loglevel()const { return m_loglevel; }

	/**
	 * Method to check cal_enable_mlog is enabled or not.
	 */
	bool is_app_log_enabled()const { return m_app_log_enabled; }

	/**
	 * Getter method for local log file.
	 */
	std::string get_logfile()const { return m_logfile; }

	/* This class is the tester class for testing */
	friend class CalClientBasicTester;

private:
	Config* m_config;
	CalLog* m_logger;
	std::string  m_poolname;
	std::string  m_label_affix;
	bool m_cal_enabled;
	bool m_backtrace_enabled;
	bool m_enable_pool_stack;
	unsigned long m_maxPoolStackLength;
	bool m_proxybased_corrid;

	sockaddr_in m_SiteViewAddr;
	int m_connect_time_secs;
	int m_ring_buffer_size;
	int m_msg_buffer_size;
	std::string m_label;
	std::string m_handler_type;

	CalLogLevel m_loglevel;
	bool m_app_log_enabled;
	std::string m_logfile;

	std::string m_host;
	std::string m_port;

	/**
	 * Method to create a label which will be sent as a first message from calclient 
	 * to caldaemon
	 */
	void create_label (std::string& _environment, std::string& _version, std::string& _build);

	/**
	 * Method to create multiconfig by combining cal_client.cdb and version.cdb
	 */
	void create_multiconfig(Config* _config, bool readVersionCdb);

	/**
	 * Method to read logger related parameters and create a logger instance.
	 */
	void initialize_logger();

	/**
	 * Method to read the socket parameters.
	 */
	void initialize_socket_config();

	/**
	 * Method to read the other parameters like poolstack enable, backtrace enable, 
	 * handler, message buffer size etc....
	 */
	void initialize_other_parameters();
};

#endif
