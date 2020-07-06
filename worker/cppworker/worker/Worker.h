//-----------------------------------------------------------------------------
//
// WORKER CHILD
//
// The definition of a class D derived from Worker is most of the work of
// creating a new server.  The framework of Proxy and Worker
// handle all the details of accepting connections.  Your derived class just
// has to process commands from an already-connected and authenticated client.
//
// The methods that can be overriden are:
//
// - virtual int handle_command()
//   Most servers do all of their work in this method.  After Worker has
//   accepted a connection, it will repeatedly call D::handle_command() with
//   the contents (command and body) of each netstring read from the client.  A
//   typical implementation of handle_command() would switch() on _cmd, do some
//   work dependent on which command was received, and then send a response to
//   the client using the member NetstringWriter* writer.  In the case of an
//   unrecognized value for _cmd, D::handle_command() should call
//   Worker::handle_command(_cmd, _buffer).  Return 0 on success, or -1 to
//   sever the connection.
//
// - virtual int prepare_connection()
//   This is called by Worker after a new connection is accepted, but
//   before the first call to handle_command(), in case any work needs to be
//   done at that time.  Return 0 on success, -1 to abort the connection.
//
// - virtual void cleanup_connection()
//   This is called by Worker after a connection is ended, either by
//   client disconnect or by a return of -1 from handle_command().  It is not
//   called if prepare_connection() fails.
//
//-----------------------------------------------------------------------------
//
// SERVER CHILD FACTORY
//
// After defining a class D derived from Worker, you need to define a
// class DFactory, derived from ServerChildFactory.  For most servers, this is
// fairly trivial.  The methods to be overridden are:
//
// - virtual Worker* create(const InitParams& _params) const = 0;
//   This is the important method.  DFactory::create() should return an instance
//   of D.  The Worker constructor requires a number of parameters which
//   will likely be required in turn by the D constructor.  Most of these are
//   provided as parameters to create(), and the last (config filename) can be
//   taken from DFactory::config_filename().
//
// - virtual const char* get_config_filename()
//   This returns the filename of the config used by the server.  e.g.
//   "finserv.cdb".
//
// - virtual const char* get_server_name()
//   This returns the name of the server, e.g. "finserv".  This server name is
//   used in some log messages.  It is also used to determine the protected
//   config filename and possibly some certificate filenames.
//

#ifndef _SERVERCHILD_H_
#define _SERVERCHILD_H_

class LogWriterBase;
class NetstringReader;
class NetstringWriter;

#include <stdint.h>
#include <sys/time.h>
#include <map>
#include <vector>
#include <fstream>

#include "worker/ControlMessage.h"
#include "worker/OCCGlobal.h"
#include "utility/signal_manage.h"
#include "log/TimerLog.h"
#include "config/Config.h"
#include "cal/CalClientSession.h"

//-----------------------------------------------------------------------------

struct InitParams
{
	std::string        config_filename;
	std::string        db_username;
	std::string        db_password;

	const char *       server_name;
	const char *       module;
	const char *       mux_start_time_sec;
	const char *       mux_start_time_usec;
	const char *       client_session;
	const char *       db_hostname;
};

class Worker
{
protected:
	const char *server_name;
	time_t last_protected_install_time;
	Config* config;
	LogWriterBase* logfile;

	// STATE
	int constructor_success;

	// signal flags
	SigCatcher m_sig_catcher;
	volatile int  child_shutdown_flag;
	// true - it is in a state where in can receive requests from a new client
	// false - it is in the middle servicing requests from the current client. Ex: in transaction, or expects a command
	//			which is a part of a chain of commands
	volatile bool  dedicated;

	// CONFIG
	// at what level should the state times be logged?  LOG_OFF means don't log.
	LogLevelEnum state_timing_log_level;

	// CAL session management
	CalClientSession client_session;

	bool m_cal_enabled;

	// the one and only child (per process)
	static Worker *the_child;

    // PPSCR00697704 -- Add more info in CAL logging
    TimerLog m_cpu_usage;
    std::string client_info;
    // PPSCR00797704 --

    time_t opscfg_check_time;
    uint m_log_level;

	fd_set fdset;

	uint m_max_requests_allowed;
	uint m_opscfg_max_requests_allowed;
	int m_max_lifespan_allowed;
	int m_opscfg_lifespan;
	uint m_requests_cnt;
	time_t m_start_time;
	uint32_t m_reqid_to_abort;
	
public:
	//need to pass in a server socket which is already bound to the correct port
	//the child will accept on the socket
	Worker(const InitParams& _params);
	virtual ~Worker();

	//main processing loop
	virtual void main();

	std::string get_protocol_version() const { return m_protocol_version; }
	void set_protocol_version(std::string& _version) { m_protocol_version = _version; }

	void trigger_recovery(char _param) { m_recover = ((unsigned char) _param) | (0xF000); };
	void set_id_to_abort(uint32_t _param) { m_reqid_to_abort = _param; };
	ControlMessage::CtrlCmd recovery_param() {return (ControlMessage::CtrlCmd)(m_recover ^ 0xF000);}

protected:
	int init_protected(const void*);
	int helper_load_protected_configs(const void* pin);
	std::string m_corr_id;
	int m_sid;
	timeval m_mux_start_time;
	uint m_saturation_recover;

	// the main "infinite" loop
	virtual void run();
	// called to do any idle processing
	virtual void on_idle();
	virtual int prepare_connection();
	//handle pieces of a connection.
	//these are called by Worker::run(), which may be overridden.
	virtual int handle_command(const int _cmd, std::string &_buffer);
	virtual void cleanup_connection();
	int get_state_index();

	void end_session();

	void exit(int _status);

	// handle signals
	static  void chld_sigfunc(int _sig);
	virtual void sigfunc(int _sig);

	// Children can specify which commands they want to log via CAL; the default
	// is to log all commands.  Specialize this method and return true for
	// commands you want to log via CAL, false for commands that you don't
	virtual bool cal_log_command(int _cmd);

	// convert the command ID into a human-readable string, returns "Unknown command"
	// string if an unrecognized command ID is passed in
	// Override this command in your Worker class to get readable commands
	virtual std::string get_command_name(int _cmd);


	void check_buffer();

	void set_dedicated(bool _dedicated) { dedicated = _dedicated; }

	virtual void check_opscfg();

	// triggered by Mux after a connection with client is suddenly lost,
	// try to restore the state to be available to receive commands.
	// returns true is recovery successfull
	virtual bool recover();

	static bool is_remote_closed(int _fd);

	// check if we're in transaction
	virtual bool is_in_transaction() { return true; };

	void set_txn_time_offset(bool _reset=false);

protected:
	bool check_max_requests_and_lifespan();
	std::string m_protocol_version;
	std::string m_cal_client_session_name; 

	sig_atomic_t m_recover;
	bool m_eor_free_sent;

	std::string m_query_hash;
	int m_connected_id;
	std::string m_db_uname;

	void eor(int _status);
	void eor(int _status, const std::string& _buffer);
	void eor(int _status, int _cmd);
	void eor(int _status, int _cmd, const std::string& _buffer);

private:
	std::istream *m_in;
	std::ostream *m_out;

	std::string m_handle_connection_buf;

protected:
	int m_data_fd;
	int m_ctrl_fd;
	NetstringReader* m_reader;
	NetstringWriter* m_writer;
};

#endif
