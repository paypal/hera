// Copyright 2020 PayPal Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include <climits>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <poll.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "Worker.h"
#include "WorkerApp.h"
#include "log/LogWriter.h"
#include "config/CDBConfig.h"
#include "config/MultiConfig.h"
#include "config/OPSConfig.h"
#include "utility/FileUtil.h"
#include "utility/encoding/NetstringReader.h"
#include "utility/encoding/NetstringWriter.h"
#include "utility/urandom.h"
#include "utility/StringUtil.h"
#include "worker/Util.h"
#include "worker/OCCMuxCommands.h"
#include "worker/OCCCommands.h"
#include "worker/ServerCommands.h"
#include "worker/EORMessage.h"

#define DEFAULT_RECONN_SLEEP_TIME 20
#define RAND_IN_RANGE(r) ((int)(rand() * ((double)r /((double)RAND_MAX + 1))))

Worker *Worker::the_child = NULL;

//-----------------------------------------------------------------------------
/**
 * @brief STATIC top-level function to handle all signals
 *
 * We need this intermediate signal handler so that we can invoke
 * a instance method for the actual signal handling.
 *
 * @param _sig Signal number
 */
void Worker::chld_sigfunc(int _sig)
{
	if (!the_child)
		return;
	the_child->sigfunc(_sig);
} // chld_sigfunc()

/**
 * @brief Signal Handler
 *
 * This is a method and so we have access to members.
 * It is indirectly invoked by the actual signal handler chld_sigfunc().
 *
 * @see chld_sigfunc
 * @param _sig Signal number
 */
void Worker::sigfunc(int _sig)
{
	switch (_sig)
	{
	case SIGINT:
	case SIGTERM:
	case SIGUSR1:
		child_shutdown_flag = 1;
		break;
	case SIGALRM:
		// if we're accepting, it means this was due to lifepsan expiring.  Just
		// set the shutdown flag and it will get caught right away
//		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Worker::SIGALRM");
		exit(0);
		break;
	case SIGSEGV:
		{
			signal(SIGSEGV, SIG_DFL);
			std::string bt = "backtrace";
			WRITE_LOG_ENTRY(logfile, LOG_WARNING, "SIGSEGV: %s", bt.c_str());
			CalEvent ev("SIGSEGV", bt, CAL::TRANS_ERROR);
			ev.Completed();
			raise(SIGSEGV);
		}
	}
} // sigfunc()

//-----------------------------------------------------------------------------

/**
 * @brief Constructor
 *
 * It sets the config values from CDB, set up random seed and
 * sets the signal handlers
 */
Worker::Worker(const InitParams& _params) :
	server_name(_params.server_name),
	last_protected_install_time(0),
	config(NULL),
	logfile(NULL),
	constructor_success(0),
	child_shutdown_flag(0),
	dedicated(false),
	state_timing_log_level(LOG_DEBUG),
	m_cal_enabled(false),
	opscfg_check_time(0),
	m_log_level(0),
	m_max_requests_allowed(0),
	m_opscfg_max_requests_allowed(0),
	m_max_lifespan_allowed(0),
	m_opscfg_lifespan(0),
	m_requests_cnt(0),
	m_start_time(time(NULL)),
	m_sid(0),
	m_saturation_recover(1), // enable setting txn time offset by default, OCCChild will load occ.cdb to get actual value.
	m_cal_client_session_name((_params.client_session)?:"nullCalClientSession"),
	m_recover(0),
	m_eor_free_sent(false),
	m_query_hash("NotSet"),
	m_connected_id(0),
	m_data_fd(3),
	m_ctrl_fd(4)
{
	std::string sval;

	// set up the log format
	logfile = LogFactory::get(DEFAULT_LOGGER_NAME);	// this logger is actually created by ServerManager

	// srand for PIP
	struct timeval tv;
	gettimeofday(&tv, NULL);
	srand(tv.tv_sec ^ tv.tv_usec ^ getpid());

	if (_params.mux_start_time_sec && _params.mux_start_time_usec)
	{
		m_mux_start_time.tv_sec = atoi(_params.mux_start_time_sec);
		m_mux_start_time.tv_usec = atoi(_params.mux_start_time_usec);
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "mux_start_time_ms is found %u %u", m_mux_start_time.tv_sec, m_mux_start_time.tv_usec); 
	}
	else
	{
		m_mux_start_time = tv;
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "mux_start_time_ms is not found, using current time %d, %d", m_mux_start_time.tv_sec, m_mux_start_time.tv_usec); 
	}

	std::string cfg_fname(_params.config_filename);
        if (cfg_fname[0] != '/') {
                cfg_fname = std::string(g_log_path) + "/" + cfg_fname;
        }
	config = new CDBConfig(cfg_fname);

	// This config will contain all CAL configuration information.
	MultiConfig* cal_cfg = new MultiConfig;

	// Add CAL-specific configuration
	CDBConfig *cal_client_cfg = new CDBConfig(std::string(g_cfg_path)+"/cal_client.cdb");
	cal_cfg->add_config(cal_client_cfg);

	// Add version information
	CDBConfig *version_cfg = new CDBConfig(std::string(g_cfg_path)+"/version.cdb");
	cal_cfg->add_config(version_cfg);

	CalClient::init (cal_cfg);
	logfile->set_enable_cal(true);
	m_cal_enabled = CalClient::is_enabled();

	// now see if the config file has any better values for us
	CalEvent cal_event("SERVER_CONFIG");
	cal_event.SetName("SERVER_LOADKEY");
	bool config_warning = false;

	if(config->get_value("state_timing_log_level", sval))
		state_timing_log_level = (LogLevelEnum)atoi(sval.c_str());
	if(config->get_value("log_level", sval))
		logfile->set_log_level((LogLevelEnum)atoi(sval.c_str()));
	m_log_level = (uint)logfile->get_log_level();

	if(config_warning)
	{
		cal_event.SetStatus(CAL::TRANS_WARNING);
	}
	else
	{
		cal_event.SetStatus(CAL::TRANS_OK);
	}

	cal_event.Completed();

	// set up signal handlers
	if (!the_child)
	{
		//for the nanny serverchild containing multiple serverchilds as members, take nanny as the_child.
		the_child = this;
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "the child %d", this);
		m_sig_catcher.push(SIGTERM, chld_sigfunc);
		m_sig_catcher.push(SIGINT, chld_sigfunc);
		m_sig_catcher.push(SIGUSR1, chld_sigfunc);
		m_sig_catcher.push(SIGUSR2, chld_sigfunc);
		m_sig_catcher.push(SIGALRM, chld_sigfunc);
		if (config->is_switch_enabled("enable_SIGSEGV_backtrace", false))
		{
			WRITE_LOG_ENTRY(logfile, LOG_INFO, "Enabled SIGSEGV backtrace");
			m_sig_catcher.push(SIGSEGV, chld_sigfunc);
		}

		sigset_t   signal_mask;
		sigemptyset (&signal_mask);
		sigaddset(&signal_mask, SIGHUP);
		pthread_sigmask (SIG_BLOCK, &signal_mask, NULL);
	}

	int opts = fcntl(m_data_fd, F_GETFL);
	if (opts < 0 && errno == EBADF) {
	    // file descriptor is invalid or closed
		WRITE_LOG_ENTRY(logfile, LOG_ALERT, "data FD is invalid");
		constructor_success = 0; // ensure flag it
		return;
	}
	opts &= ~O_NONBLOCK;
	if (fcntl(m_data_fd, F_SETFL, opts) < 0) {
		WRITE_LOG_ENTRY(logfile, LOG_INFO, "error %d setting socket flags", errno);
		constructor_success = 0; // ensure flag it
		return;
	}

	m_in = FileUtil::istream_from_fd(m_data_fd);
	m_out = FileUtil::ostream_from_fd(m_data_fd);

	m_reader = new NetstringReader(m_in);
	m_writer = new NetstringWriter(m_out);

	// constructor succeeded
	constructor_success = 1;
}

//-----------------------------------------------------------------------------

/**
 * @brief Destructor
 */
Worker::~Worker()
{
	delete config;
	the_child = NULL;
}

//-----------------------------------------------------------------------------

/**
 * @brief Main processing loop
 *
 * It accept connection, check for authorization of the client based on
 * IP and then sets up the stream reader and writer.
 */
void Worker::main()
{
	std::string cval;

	if (!constructor_success)
	{
		uint sleep_time;

		// sleep a bit to prevent mad cycling in the event of failed constructor success
		if (config->get_value("child_failure_delay", cval))
			sleep_time = StringUtil::to_int(cval);
		else
			sleep_time = DEFAULT_RECONN_SLEEP_TIME;

		sleep_time = urandom::rand(sleep_time);
		sleep(sleep_time);
		return;
	}

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "A child is born");

	run();
}

/**
 * @brief this function is called in the main loop to do any idle processing
 *
 */
void Worker::on_idle(void)
{
	if (check_max_requests_and_lifespan()) {
		//TODO
	}
}

//-----------------------------------------------------------------------------

/**
 * @brief Handle any per-connection initialization.
 *
 * It does nothing in this base class.
 *
 * @return 0 - Success, -1 - Failed
 */
int Worker::prepare_connection()
{
	return 0;
} // prepare_connection()

/**
 * @brief Handles client connection after handshaking passes
 *
 * Normally you should not have to override this method.
 * To customize override these methods
 * @see prepare_connection()
 * @see cleanup_connection()
 * @see handle_command()
 */
void Worker::run()
{
	int    rc, cmd;
	static const std::string API("API");
	static const std::string ZERO("0");
	static const std::string FAILED("FAILED");

	if (prepare_connection())
		return;

	std::ostringstream os;
	os << m_connected_id << " " << m_db_uname;
	if (-1 == m_writer->write(CMD_CONTROL_MSG, os.str())) {
		WRITE_LOG_ENTRY(logfile, LOG_ALERT, "Can't write the initial control message");
		return;
	}

	while (1)
	{

		if (!dedicated)
		{
			check_opscfg();
		}

		int select_errno = 0;
		if (m_reader->is_buffer_empty()) {
			WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Waiting for requests");
			int err = 0;
			while(err == 0)
			{
				FD_ZERO(&fdset);
				FD_SET(m_data_fd, &fdset);

				struct timeval timeout = {0};
				timeout.tv_sec = 0;
				timeout.tv_usec = 200000;
				err = select(m_data_fd + 1, &fdset, 0, 0, &timeout); // timeout

				if (m_recover)
					recover();
				if (err == 0)
				{
					if (!dedicated)
						on_idle();
					check_opscfg();
				}
			}

			if (err == -1)
				select_errno = errno;

			WRITE_LOG_ENTRY(logfile, LOG_VERBOSE,
				"select() exited with ret=%d and errno=%d", err, select_errno);
		}

		if (!dedicated)
		{
			prepare_connection();
		}

		if (select_errno == EINTR)
		{
			if (child_shutdown_flag)
			{
				if(dedicated)
				{
					WRITE_LOG_ENTRY(logfile, LOG_INFO, "Shutdown flag is set and ignored because the worker is dedicated");
				}
				else
				{
					WRITE_LOG_ENTRY(logfile, LOG_INFO, "Shutdown flag is set - exiting worker");
					break; // worker is done
				}
			}
			// otherwise
			continue;
		}
		else if (select_errno != 0)
		{
			WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Error (%d) waiting on select - exiting worker", errno);
			break; // some other error - worker is done
		}

		if (!FD_ISSET(m_data_fd, &fdset))
			continue;

		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "data available, read it");
		// read a command from the client
		m_handle_connection_buf.clear();

		cmd = m_reader->read(&m_handle_connection_buf);

		if (cmd == -1)
		{
			// exit
			if (is_remote_closed(m_data_fd))
			{
				WRITE_LOG_ENTRY(logfile, LOG_INFO, "Mux closed the data channel while reading Netstring command - exiting worker");
			}
			else
			{
				CalEvent ev("ERROR", "Netstring", CAL::TRANS_FATAL);
				ev.Completed();
				WRITE_LOG_ENTRY(logfile, LOG_INFO, "Error reading OCC command (bad protocol or disconnected)");
			}
			break;
		}
//		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "worker rq_ID = %d", m_reader->get_count());

		if (!dedicated && cmd != CLIENT_CAL_CORRELATION_ID)
		{
			m_eor_free_sent = false;
			set_dedicated(true);
			m_cpu_usage.start(); // PPSCR00797704 -Start monitoring cpu usage

			if (m_cal_enabled)
			{
				client_session.start_session("API", m_cal_client_session_name);
				client_session.get_session_transaction()->AddData("worker_pid", getpid());
				client_session.get_session_transaction()->AddData("sid", m_sid);
				client_session.set_status(CAL::TRANS_OK); // internal queries' error overwrite status so reset it.
			}
			WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Session started");
		}

		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Handling command %s", get_command_name(cmd).c_str());
		// handle the command
		if (cal_log_command(cmd))
		{
			CalTransaction caltrans("SQL");
			caltrans.SetName(get_command_name(cmd));
			std::string tmp;
			caltrans.AddData("CMD",StringUtil::fmt_int(tmp, cmd));
			caltrans.SetStatus(ZERO);
//			TransientMarkdown::get_instance().reset();
			rc = handle_command(cmd, m_handle_connection_buf);
			if (rc == -1) {
				caltrans.SetStatus(FAILED);
			}
		}
		else
		{
			rc = handle_command(cmd, m_handle_connection_buf);
		}

		if (rc == -1) {
			break;
		}
		
		//check_buffer();
		if (!dedicated && cmd != CLIENT_CAL_CORRELATION_ID)
		{
			end_session();
		}

		if(child_shutdown_flag)
		{
			if (dedicated)
			{
				WRITE_LOG_ENTRY(logfile, LOG_INFO, "Shutdown flag is set and ignored because the worker is dedicated(2)");
			}
			else
			{
				//finito
				WRITE_LOG_ENTRY(logfile, LOG_INFO, "Shutdown flag is set, exiting(2)");
				break;
			}
		}
	}

	cleanup_connection();
}

bool Worker::recover()
{
	ControlMessage::CtrlCmd param = recovery_param();
	m_recover = 0;

	if (param == ControlMessage::STRANDED_SATURATION_RECOVER)
	{
		CalEvent ev("EVICTION", m_query_hash, CAL::TRANS_OK);
		ev.Completed();
	}

	if (dedicated)
	{
		CalEvent ev("RECOVER");
		if (is_in_transaction())
			ev.SetName("dedicated");
		else
			ev.SetName("dedicated_no_trans");

		ev.SetStatus(CAL::TRANS_OK);
		ev.Completed();
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Mux asks to abort existing work. Worker prepare to become available. (%d)", param);
		uint32_t req_id = m_reader->get_count();
		if (req_id != m_reqid_to_abort) {
			std::ostringstream os;
			os <<  "Mux ID is:" << m_reqid_to_abort << ", Worker ID is : " << req_id;
			CalEvent evt("RECOVER", "REQID_MISMATCH", CAL::TRANS_WARNING, os.str());
			evt.Completed();
			WRITE_LOG_ENTRY(logfile, LOG_WARNING, "Race interrupting SQL during recover, wk_rq_ID is %d and mux_rq_ID is %d.", req_id, m_reqid_to_abort);
			return false; // If we proceed with end_session, we may end up in ROLLBACK of the new req and client wil not be aware of it (Should we atleast send EOR_FREE)
		} 
		eor(EORMessage::FREE);
		m_writer->write();
		end_session();
	}
	else
	{
		CalEvent ev("RECOVER", "not_dedicated", CAL::TRANS_WARNING);
		ev.Completed();
		WRITE_LOG_ENTRY(logfile, LOG_WARNING, "Mux asks to abort existing work, but worker is not dedicated. (%d)", param);
	}
	return true;
}

void Worker::end_session()
{
	m_cpu_usage.stop(); //PPSCR00797704 end CPU usage monitoring and log the usage

	cleanup_connection();

	if (client_session.is_session_active())
	{
		client_session.end_session();
	}

	WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Session ended");
}

/**
 * @brief Handles any post-connection cleanup.
 *
 * It does nothing in this base class.
 */
void Worker::cleanup_connection()
{
} // cleanup_connection()

/**
 * @brief (virtual) Default method to handle client commands
 *
 * Derived classes should override this to provide more
 * functionality. However, they should still call this method
 * as Worker::handle_command() if they want to support
 * the built-in commands as is.
 *
 * @param _cmd Command code extracted from buffer
 * @param _buffer Rest of the command line.
 * @return 0 - command processed successfully, -1 - unknown command
 */
int Worker::handle_command(const int _cmd, std::string &_buffer)
{
	int rc = 0;

	switch (_cmd)
	{
		case PROTOCOL_VERSION :
			m_protocol_version = std::string(_buffer.c_str());
			break;
		case SERVER_PING_COMMAND :
			eor(is_in_transaction() ? EORMessage::IN_TRANSACTION : EORMessage::FREE, SERVER_ALIVE);
			m_writer->write();
			break;
		case CLIENT_CAL_CORRELATION_ID:
		{
			std::string client_info_str = _buffer;
			m_corr_id.clear();
			StringUtil::tokenize(client_info_str, m_corr_id, '&');
			if (m_corr_id.length() == 0)
			{
				m_corr_id = _buffer; // we didn't get '&' format client_info_str
			}
			if(true)
			{
				std::string discard;
				StringUtil::tokenize(m_corr_id, discard, '=');
				CalTransaction::SetCorrelationID(m_corr_id);
				WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "set cal_correlation_id = %s", m_corr_id.c_str());
			}
			
			break; 
		}
		
		case CLIENT_INFO :
		{	//send server Info
			std::string client_info_str = _buffer;
			std::string server_info = CalTransaction::GetCurrentPoolInfo();
			m_writer->write(SERVER_INFO, server_info);
			WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Server Info: %s", server_info.c_str());
			
			if(!client_info_str.empty())
			{
				std::string discard;
				StringUtil::tokenize(client_info_str, discard, '=');
			}
			
			CalTransaction::SetParentStack(client_info_str, std::string("CLIENT_INFO"));
			CalEvent e(CAL::EVENT_TYPE_CLIENT_INFO, "unknown", CAL::TRANS_OK);
			e.AddPoolStack();
			break;
		}
		
		default :
		{	
			std::ostringstream msg;

			m_writer->write(SERVER_UNEXPECTED_COMMAND, "");
			msg << "err_msg=Unknown command " << _cmd << ": " << _buffer;
			WRITE_LOG_ENTRY(logfile, LOG_ALERT, msg.str().c_str());
			if (m_cal_enabled)
			{
				CalTransaction::Status s(CAL::TRANS_ERROR, CAL::MOD_GENERIC_SERVER, CAL::SYS_ERR_INTERNAL, msg.str().c_str());
				CalEvent e(CAL::EVENT_TYPE_ERROR, "Server Command", "Invalid command", msg.str());
			}
			// keep rc == 0 to allow the connection to continue
			break;
		}
	} // switch cmd
	return rc;
} // handle_command()

void Worker::exit(int _status)
{
	client_session.end_session();
	// No more write to CalLog's log file if CalLog is using LogWriter
	CalLog::set_exit_flag(true);
	::exit(_status);
}

bool Worker::cal_log_command(int _cmd)
{
	switch (_cmd)
	{
	case SERVER_PING_COMMAND:
	case CLIENT_CAL_CORRELATION_ID:
	case CLIENT_INFO:
	case OCC_SHARD_KEY:
		return false;
	default:
		return true;
	}
}

std::string Worker::get_command_name(int _cmd)
{
	switch (_cmd)
	{
	case SERVER_PING_COMMAND :
		return std::string("PING");
	case CLIENT_CAL_CORRELATION_ID:
		return std::string("SET_CAL_CORRELATION_ID");
	default:
		{	
			std::ostringstream cmd;
			cmd << "COMMAND_" << _cmd;
			return cmd.str();
		}
	}
}


bool Worker::check_max_requests_and_lifespan()
{
	bool restart = false; // only used for max_requests and max_lifespan
	if (m_max_requests_allowed > 0 && m_requests_cnt >= m_max_requests_allowed)
	{
		restart = true;
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "reached the max number of txns allowed, handled=%d, allowed=%d", 
						m_requests_cnt, m_max_requests_allowed);
		m_requests_cnt = 0;
	}
			
	time_t now = time(NULL);
	if (m_max_lifespan_allowed != 0 && (now - m_start_time) > m_max_lifespan_allowed)
	{
		restart = true;
	}

	return restart;

}

/*
 * @param _idle 
 *
 */
void Worker::check_buffer()
{
	if (!m_reader->is_buffer_empty())
	{
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Netstring buffer not empty");
		// overwrite the dedicated flag, in case it was false. the worker is not practically free anyway
		// because it did not tell proxy via the ctrl message
		set_dedicated(true);
		return;
	}
}

void Worker::check_opscfg()
{
	time_t now = time(0);
	if (now > opscfg_check_time)
	{
		OPSConfig& opscfg = OPSConfig::get_instance();
		// log level
		uint log_level = m_log_level;
		std::string val;
		if (opscfg.get_value("log_level", val)) {
			log_level = StringUtil::to_int(val);
		}
		if (m_log_level != log_level)
		{
			m_log_level = log_level;
			logfile->set_log_level(static_cast<LogLevelEnum>(m_log_level));
			WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "log_level=%u", m_log_level);
		}
		opscfg_check_time = now + 30; // check again in 30 secs
	}
}

bool Worker::is_remote_closed(int _fd)
{
	pollfd poll_struct;
	poll_struct.fd = _fd;
	poll_struct.events = POLLIN;
	if (poll(&poll_struct, 1, 0) == 1)
	{
		if (poll_struct.revents & POLLHUP)
			return true;
	}
	return false;
}

/*
 * @param _reset default to false; if true, it resets the offset value to 0.
 *
 */
void Worker::set_txn_time_offset(bool _reset)
{
	if (m_saturation_recover == 0)
		return;

	timeval now = {0};
	gettimeofday(&now, NULL);
	timeval offset = {0};
	uint txn_offset = 0; 
	
	if (_reset)
	{
		if (logfile->get_log_level() >= LOG_VERBOSE)
		{	
			timersub(&now, &m_mux_start_time, &offset);  
			uint tmp_offset = (uint)(offset.tv_sec*1000 + offset.tv_usec/1000);
			WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, 
				"txn end time: current time %d, %d, sql time offset %u", now.tv_sec, now.tv_usec, tmp_offset);
		}
	}
	else
	{
		timersub(&now, &m_mux_start_time, &offset);  
		txn_offset = (uint)(offset.tv_sec*1000 + offset.tv_usec/1000);
		if (txn_offset == 0)
		{
			txn_offset -= 1; // overflowed, to avoid arbitrary meaning of 0 as reset, make it 1 ms earlier (UINT_MAX).
		}

		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, 
			"txn start(current) time %d, %d, sql time offset %u", now.tv_sec, now.tv_usec, txn_offset);
	}
}

void Worker::eor(int _status, const std::string& _buffer)
{
	if ((!m_reader->is_buffer_empty()) && (_status == EORMessage::FREE))
	{
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Netstring buffer not empty, EOR status FREE overwritten to MORE_INCOMING_REQUESTS");
		// overwrite the dedicated flag, in case it was false. the worker is not practically free anyway
		set_dedicated(true);
		_status = EORMessage::MORE_INCOMING_REQUESTS;
	}
	if (_status ==  EORMessage::FREE) {
		set_dedicated(false);

		if (m_eor_free_sent) {
			// CAL event
			CalEvent ev("WARNING", "EOR_FREE_AGAIN", "0");
			ev.Completed();
			// overrite the status to re-start. This case should not happen, and there is the possibility of a
			// condition where mux and worker are not in sync, if data from mux to worker is comming after the
			// previous EOR FREE
			_status = EORMessage::RESTART;
		}
		m_eor_free_sent = true;
	}
	m_writer->separate();
	WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "EOR reqid is: %u", m_reader->get_count());
	EORMessage msg((EORMessage::Status)_status, m_reader->get_count(), _buffer);
	msg.dump(*logfile);
	std::string buf;
	msg.compose(buf);
	m_writer->add(CMD_EOR, buf);
}

void Worker::eor(int _status)
{
	static const std::string null_value;
	eor(_status, null_value);
}

void Worker::eor(int _status, int _cmd)
{
	static const std::string null_value;
	eor(_status, _cmd, null_value);
}

void Worker::eor(int _status, int _cmd, const std::string& _buffer)
{
	std::string buff;
	Util::netstring(_cmd,  _buffer, buff);
	eor(_status, buff);
}
