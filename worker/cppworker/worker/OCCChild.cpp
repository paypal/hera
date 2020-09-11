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
#include <dirent.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <math.h>
#include <string.h>

#include <oci.h>
#include <xa.h>

#include "config/Config.h"
#include "config/CDBConfig.h"
#include "config/OPSConfig.h"
#include "log/LogFactory.h"
#include "utility/encoding/NetstringReader.h"
#include "utility/encoding/NetstringWriter.h"
#include "worker/OCCBind.h"
#include "worker/OCCCachedResults.h"
#include "worker/OCCChild.h"
#include "worker/HBSender.h"
#include "worker/OCCCommands.h"
#include "worker/ServerCommands.h"
#include "worker/OCCDefine.h"
#include "worker/EORMessage.h"
#include "worker/OCCConfig.h"
#include "worker/ShardKeyUtil.h"
#include "utility/signal_manage.h"
#include "utility/TimeUtil.h"
#include "worker/Util.h"
#include "utility/HashUtil.h"


const std::string CAL_TRANSACTION_EXEC = "EXEC";
const std::string CAL_NAME_EXECUTE = "Execute";
const std::string CAL_DATA_QUERY = "QUERY";
const std::string CAL_EVENT_CLIENT_INFO = "Client_info"; // No longer used. As part of #PPSCR00833217 CAL event type for CLIENT_INFO is now the poolname
const std::string CAL_EVENT_BACKTRACE = "Client backtrace";
const std::string CAL_EVENT_ROLLBACK = "ROLLBACK";
const std::string CAL_NAME_RECOMAN = "RecoMan";
const std::string CAL_DATA_XID = "XID";
const std::string CAL_EVENT_DISTRIBUTED = "Distributed";
const std::string CAL_EVENT_LOCAL = "Local";
const std::string CAL_DATA_RC = "RC";
const std::string CAL_EVENT_COMMIT = "COMMIT";
const std::string CAL_EVENT_SQL_ERROR = "SQL_Error"; // #PPSCR00839468: Removing space from CAL event name and type
const std::string CAL_DATA_SQL_TEXT = "SQL_Text";
const std::string CAL_EVENT_TRANS_START = "TRANSSTART";
const std::string CAL_STATUS_SUCCESS_WITH_INFO = "Success With Info";
const std::string CAL_EVENT_ORACLE = "Oracle";
const int MAX_VSESSION_BIND_DATA = 63;
const uint DEFAULT_WINDOW = 240;
const std::string CAL_EVENT_DATETIME = "Datetime";
const std::string CAL_EVENT_FETCH_CHUNK_SZ_CHANGED = "FETCH_CHUNK_SZ_CHNG";
const std::string CAL_EVENT_FETCH_CACHE_TOO_MANY_ROWS = "CACHE_TOO_MANY_ROWS";
const std::string CAL_EVENT_STDBY = "STDBY";
const std::string CAL_EVENT_SCN = "SCN";
const uint DEFAULT_STBY_SCN_FETCH_INTERVAL = 1;
const int DEFAULT_RAC_SQL_INTERVAL = 10; // second

static const uint MAX_ORACLE_LOBPREFETCH_SIZE = 4000;

// somehow this is missing from 11.2.0.4, although in 11.2.0.2 and 19
// preffer not to wrap in an ifndef
#define OCI_ATTR_TRANSACTION_IN_PROGRESS 484

namespace
{
	unsigned long trans_type_to_oci(occ::TransType type)
	{
		switch (type)
		{
		case occ::LOOSELY_COUPLED:
			return OCI_TRANS_LOOSE;
		default:
			break;
		};

		return OCI_TRANS_TIGHT;
	}
};

using namespace occ;

//-----------------------------------------------------------------------------
#define DO_OCI_HANDLE_FREE(res, t, l) real_oci_handle_free(reinterpret_cast<dvoid*&>(res), t, #res, l)

#define NUM_INDICATOR_BUF   64
#define NUM_STR_SIZE_BUF	64
#define COL_DATA_BUF_SIZE   (100 * 1024)     // 100k
#define MAX_OUT_BIND_VAR_SIZE   64		//!< Amount of memory (in bytes) to allocate for an out-bound placeholder, 64, 64k has issue with amq LOB in/out bind, 32k appears to work
#define MAX_DYNAMIC_BIND_ROWS	1		//!< How many rows of data do we accept in an out-bound placeholder

#define DEFAULT_SEND_BUF_SIZE  (128*1024)   // 128k, useful especially for large insert payloads

static unsigned int MAX_ARRAY_DATA_SIZE = 65534;   //!< Size of the largest column for array bind
static const unsigned int DEFAULT_MAX_FETCH_BLOCK_SIZE = 20;	//!< Default number of rows to fetch at a time.
static const unsigned int DEFAULT_TRANS_TIMEOUT = 5;	//!< Default global transaction timeout value in seconds.
static const char *const DEFAULT_MODULE_NAME = "Unknown";	//!< Default module name

//!<Key that occ client sends client machine name, should match OCCClient::send_client_info
static const std::string SERVER_VERSION = "10g"; 
static const std::string SERVER_RELEASE_PREFIX = "Enterprise Edition Release ";
static const std::string SERVER_DB_PREFIX = "Oracle Database ";
static const std::string CLIENT_NAME_PREFIX = "Name: "; 
static const std::string CLIENT_HOST = "HOST: "; 
static const std::string CLIENT_EXEC = "EXEC: "; 
static const std::string CLIENT_COMMAND = "Command: "; 
static const std::string CLIENT_FLOW = "flow___state = ";

static const std::string COMMA = ",";
static const std::string SLASH = "/";
static const std::string POOLNAME_PREFIX = "Poolname: "; //!<Key that occ client sends client executable name, should match OCCClient::send_client_info. PPSCR00833217 
static const std::string SEMICOLON = ";";
static const std::string SPACE = " ";
static const std::string POOLSTACK_PREFIX = "PoolStack: "; // For defect PPSCR00847162

//-----------------------------------------------------------------------------

OCCChild::ClientSession::ClientSession()
	//	: m_db_txn_cnt(0)
{
}

OCCChild::ClientSession::~ClientSession()
{
}

void OCCChild::ClientSession::start_db_txn()
{
	end_db_txn();

	/*
	   if (CalClient::is_enabled())
	   {
	   std::string txn_name;
	   txn_name.copy_formatted("DB Txn %u", ++m_db_txn_cnt);
	   m_db_txn.start_session(CAL::TRANS_TYPE_URL, txn_name);
	   }
	 */
}

void OCCChild::ClientSession::end_db_txn()
{
	//m_db_txn.end_session();
	m_query.clear();
}

	OCCChild::TxnStartTime::TxnStartTime(OCCChild& _child)
:m_child(_child)
{
	m_child.set_txn_time_offset();
}

OCCChild::TxnStartTime::~TxnStartTime()
{
	m_child.set_txn_time_offset(true);
}


//-----------------------------------------------------------------------------

OCCChild::OCCChild(const InitParams& _params) : Worker(_params),
	current_markdowns(new std::vector<MarkdownStruct>()),
	envhp(NULL),
	srvhp(NULL),
	errhp(NULL),
	errhndl_batch(NULL),
	errhndl_batch2(NULL),
	svchp(NULL),
	authp(NULL),
	transhp(NULL),
	attached(false),
	has_session(false),
	m_oracle_init_called(false),
	use_nonblock(false),
	oracle_fd(-1),
	ping_interval(0),
	oracle_heartbeat_frequency(0),
	next_oracle_heartbeat_time(0),
	heartbeat_alarm_set(false),
	enable_hb_fix(false),
	hb_sender(NULL),
	enable_cache(true),
	stmt_cache(NULL),
	cur_stmt(NULL),
	max_cache_size(0),
	max_statement_age(0),
	cache_size(0),
	cache_size_peak(0),
	cache_hits(0),
	cache_misses(0),
	cache_expires(0),
	cache_dumps(0),
	cache_expire_frequency(0),
	next_cache_expire_time(0),
	enable_query_replace_nl(false),
	cur_results(NULL),
	results_valid(false),
	data_buf(NULL),
	indicator_bufs(NULL),
	str_size_bufs(NULL),
	bind_array(NULL),
	out_bind_array(NULL),
	new_fetch(true),
	in_trans(false),
	max_rows(0),
	max_fetch_block_size(0),
	current_row(0),
	backtrace_log_level(LOG_DEBUG),
	m_has_real_dml(false),
	m_in_global_txn(false),
	m_phase1_done(false),
	m_trans_role(POINT_SITE),
	m_default_trans_timeout(DEFAULT_TRANS_TIMEOUT),
	m_module_info(DEFAULT_MODULE_NAME),
	m_2pc_log(LogFactory::get_null_logger()),
	m_enable_session_variables(false),
	m_session_var_stmthp(NULL),
	oracle_lobprefetch_size(0),
	m_last_exec_rc(OCIR_OK),
	m_restart_window(DEFAULT_WINDOW),
	m_sql_rewritten(false),
	m_enable_sql_rewrite(false)
{

	std::string cval;

	if(!constructor_success)
		return;

	if (m_cal_enabled)
	{
		client_session.start_session(CAL::TRANS_TYPE_URL, "INITDB");
		std::string tmp;
		StringUtil::fmt_int(tmp, getpid());
		client_session.get_session_transaction()->AddDataToRoot("m_worker_pid", tmp);
		client_session.get_session_transaction()->AddDataToRoot("m_dbname", _params.db_hostname);
	}

	const char* tns_name = getenv("TWO_TASK");
	if (!tns_name)
	{
		WRITE_LOG_ENTRY(logfile, LOG_ALERT, "TWO_TASK is not defined");
		CalEvent ev(CAL::EVENT_TYPE_ERROR, "TWO_TASK", CAL::TRANS_OK);
		ev.Completed();
		client_session.get_session_transaction()->AddDataToRoot("m_err","TWO_TASK_NOT_DEFINED");
		client_session.get_session_transaction()->AddDataToRoot("m_errtype","CONNECT");
		client_session.set_status(CAL::SYSTEM_FAILURE); // internal queries' error overwrite status so reset it.
		client_session.get_session_transaction()->SetStatus(CAL::SYSTEM_FAILURE, CalActivity::CAL_SET_ROOT_STATUS); // internal queries' error overwrite status so reset it.
		client_session.end_session();
		constructor_success = 0; // ensure flag it
		return;
	}
	
	// initialize markdown system
	host_name = getenv("TWO_TASK");
	const char* envval = getenv("MARK_HOST_NAME");
	if (envval)
		mark_host_name = envval;
	if (!config->get_value("markdown_directory", markdown_directory)) {
		WRITE_LOG_ENTRY(logfile, LOG_INFO, "Couldn't find markdown_directory, not supporting markdowns");

		// we couldn't load it, so no markdowns can happen.
		markdown_directory = "";
	}
	if (!config->get_value("null_string", null_value))
	{
		// empty by default
		null_value.clear();
	}

	// get max fetch block size
	if (config->get_value("max_fetch_block_size", cval))
		max_fetch_block_size = StringUtil::to_int(cval);
	if (!max_fetch_block_size)
		max_fetch_block_size = DEFAULT_MAX_FETCH_BLOCK_SIZE;
	max_rows = max_fetch_block_size;

	// use non-blocking calls?
	use_nonblock = config->is_switch_enabled("use_nonblocking", FALSE);

	// send keepalive pings?
	if (config->get_value("keepalive_ping", cval))
		ping_interval = StringUtil::to_int(cval);

	// get backtrace log level (default: debug)
	if(config->get_value("backtrace_log_level", cval))
		backtrace_log_level = (LogLevelEnum)StringUtil::to_int(cval);

	if (config->get_value("default_trans_timeout", cval) == 0)
		m_default_trans_timeout = StringUtil::to_int(cval);

	if(config->get_value("oracle_heartbeat_frequency", cval))
		oracle_heartbeat_frequency = StringUtil::to_int(cval);

	// turn on OCI_ATTR_LOBPREFETCH_SIZE?
	if(config->get_value("oracle_lobprefetch_size", cval))
		oracle_lobprefetch_size = StringUtil::to_uint(cval);
	else
		oracle_lobprefetch_size = MAX_ORACLE_LOBPREFETCH_SIZE;

	if (oracle_lobprefetch_size > MAX_ORACLE_LOBPREFETCH_SIZE)  
		oracle_lobprefetch_size = MAX_ORACLE_LOBPREFETCH_SIZE;

	// initialize statement cache vars
	// check if statement caching is enabled
	enable_cache = config->is_switch_enabled("enable_cache", FALSE);
	enable_whitelist_test = config->is_switch_enabled("enable_whitelist_test", FALSE);

	// PPSCR00377721
	// if enable_cache==true, max_cache_size parameter must be > 0 to allow OCC-wide caching
	// if enable_cache==false, max_cache_size can still be set to allow session caching 
	// max_cache_size can be set to zero to explicitly disallow session caching
	if (config->get_value("max_cache_size", cval))
		max_cache_size = StringUtil::to_int(cval);

	if (enable_cache && max_cache_size < 1)
	{
		WRITE_LOG_ENTRY(logfile, LOG_ALERT, "max_cache_size undefined or invalid");
		constructor_success = 0;
		return;
	}

	// if global caching is enabled or session caching is enabled
	if (enable_cache || max_cache_size > 0)
	{
		// get some statement cache parameters
		if (config->get_value("max_statement_age", cval))
			max_statement_age = StringUtil::to_int(cval);
		if (max_statement_age < 1)
		{
			WRITE_LOG_ENTRY(logfile, LOG_ALERT, "max_statement_age undefined or invalid");
			constructor_success = 0;
			return;
		}
		if (config->get_value("expire_frequency", cval))
			cache_expire_frequency = StringUtil::to_int(cval);
		if (cache_expire_frequency < 1)
		{
			WRITE_LOG_ENTRY(logfile, LOG_ALERT, "expire_frequency undefined or invalid");
			constructor_success = 0;
			return;
		}
	}

	enable_query_replace_nl = config->is_switch_enabled("enable_query_replace_nl", TRUE);
	m_dbhost_name.clear();
	if (_params.module)
	{
		m_module_info = _params.module;
	}
	else
	{
		WRITE_LOG_ENTRY(logfile, LOG_ALERT, "Child failed to load module name");
	}
	if (_params.db_hostname) {
		m_dbhost_name = _params.db_hostname;
		WRITE_LOG_ENTRY(logfile, LOG_INFO, "HOST=%s", m_dbhost_name.c_str());
	} else {
		WRITE_LOG_ENTRY(logfile, LOG_ALERT, "Unknown host name");
		m_dbhost_name = DEFAULT_MODULE_NAME;
	}

	// setting session variables by occ child process
	m_enable_session_variables = config->is_switch_enabled( "enable_session_variables", FALSE );

	// initialize results cache vars
	if(connect(_params.db_username, _params.db_password)) {
		WRITE_LOG_ENTRY(logfile, LOG_ALERT,"Child failed to connect to oracle...bailing.");
		constructor_success = 0;
		client_session.end_session();
		return;
	}
	memset((void*)_params.db_password.c_str(), 'X', _params.db_password.length());

	m_start_time = time(NULL); // sort of more as start time
	WRITE_LOG_ENTRY(logfile, LOG_WARNING, "Worker start time %d", m_start_time);

	if (set_stored_outlines())
		WRITE_LOG_ENTRY(logfile, LOG_WARNING, "Failed to set use_stored_outlines");

	// pre-allocate some memory to be used for define arrays
	data_buf = new char[COL_DATA_BUF_SIZE];
	indicator_bufs = new sb2*[NUM_INDICATOR_BUF];
	memset(indicator_bufs, 0, NUM_INDICATOR_BUF * sizeof(sb2 *));
	for (int i = 0; i < NUM_INDICATOR_BUF; i++)
		indicator_bufs[i] = new sb2[max_fetch_block_size];
	str_size_bufs = new ub2*[NUM_STR_SIZE_BUF];
	memset(str_size_bufs, 0, NUM_STR_SIZE_BUF * sizeof(ub2 *));
	for (int i = 0; i < NUM_STR_SIZE_BUF; i++)
		str_size_bufs[i] = new ub2[max_fetch_block_size];

	if (use_nonblock)
	{
		// determine the file descriptor for oracle non-blocking calls
		oracle_fd = find_oracle_fd();
		if (oracle_fd < 0)
		{
			WRITE_LOG_ENTRY(logfile, LOG_WARNING, "Unable to determine file descriptor for Oracle. Non-blocking calls disabled.");
			use_nonblock = false;
			oracle_fd = 0;
		}
	}

	// resize send buffer size for oracle socket 
	resize_oracle_fd_buffer();

	// enable_hb_fix
	enable_hb_fix = config->is_switch_enabled("enable_heartbeat_fix", FALSE);

	// enable_hb_fix is meaningful in only blocking mode
	if (!use_nonblock && enable_hb_fix) {

		hb_sender = new HBSender(this, (ping_interval > 0 ? ping_interval : 60) , getpid(), m_ctrl_fd);
		hb_sender->start();
	}

	//Preserve the server default log format.
	m_log_format = logfile->get_format();

	if(config->get_value("rac_restart_window", cval))
		m_restart_window = StringUtil::to_int(cval);

	MAX_ARRAY_DATA_SIZE = config->get_ulong("max_batch_col_size", MAX_ARRAY_DATA_SIZE);

	m_enable_sharding = config->get_bool("enable_sharding", false);
	if (m_enable_sharding) {
		m_max_scuttle_buckets = config->get_int("max_scuttle", ABS_MAX_SCUTTLE_BUCKETS);
		m_scuttle_attr_name = config->get_string("scuttle_col_name", DEFAULT_SCUTTLE_ATTR_NAME);
		std::string algo = config->get_string("sharding_algo", DEFAULT_SHARDING_ALGO);
		StringUtil::to_lower_case(algo);
		if (algo.compare(MOD_ONLY_SHARDING_ALGO) == 0)
		{
			m_sharding_algo = MOD_ONLY;
		}
		else
		{
			m_sharding_algo = HASH_MOD;
		}
		config->get_value("shard_key_name", m_shard_key_name);
		if (!m_shard_key_name.empty()) {
			if (config->get_bool("use_shardmap", true) == false) {
				/* if we aren't loading shardmap, don't rewrite queries.
				this weird use case is for an old db to get log messages about possible 
				issues when they go to sharding */
				m_enable_sql_rewrite = false;
			} else {
				m_enable_sql_rewrite = true;
			}
			m_enable_sql_rewrite = config->get_bool("enable_sql_rewrite", m_enable_sql_rewrite);
			m_rewriter.init(m_shard_key_name, m_scuttle_attr_name);
		}
		// else ?
		
		m_shardcfg_postfix.clear();
		m_shardcfg_postfix = config->get_string("sharding_postfix", "");
	}
	client_session.set_status(CAL::TRANS_OK); // internal queries' error overwrite status so reset it.
	client_session.end_session(); // end the CalClientSession
}


//-----------------------------------------------------------------------------

OCCChild::~OCCChild()
{
	if (enable_cache && ((cache_hits > 0) || (cache_misses > 0)))
	{
		WRITE_LOG_ENTRY(logfile,
				LOG_INFO, "cache stats: hits = %lu, misses = %lu, hit ratio = %.2f%%, dumps = %lu, expires = %lu, peak size = %d",
				cache_hits, cache_misses,
				100 * (double) cache_hits / (cache_hits + cache_misses),
				cache_dumps,
				cache_expires,
				cache_size_peak);
	}

	disconnect();

	// free memory
	delete[] data_buf;
	delete current_markdowns;
	if (indicator_bufs != NULL)
	{
		for (int i = 0; i < NUM_INDICATOR_BUF; i++)
			delete[] indicator_bufs[i];
		delete[] indicator_bufs;
	}
	if (str_size_bufs != NULL)
	{
		for (int i = 0; i < NUM_STR_SIZE_BUF; i++)
			delete[] str_size_bufs[i];
		delete[] str_size_bufs;
	}
}

//-----------------------------------------------------------------------------

const std::vector<std::shared_ptr<OCCBind> >* OCCChild::get_bind_array()
{
	return bind_array;
}

const OCIError* OCCChild::get_errhp()
{
	return errhp;
}

const OCISvcCtx* OCCChild::get_svchp()
{
	return svchp;
}

/**
 * @brief this function does idle processing: check cache expiration and send Oracle heartbeat.
 *
 */
void OCCChild::on_idle(void)
{
	// expire old statements from the cache (if enough time has passed)
	cache_expire(false);

	// check that we're still connected to oracle (if enough time has passed)
	oracle_heartbeat();

	// call base version
	Worker::on_idle();
}

/**
 * @brief Per-connection initialization
 *
 * This sets up the OCI statement handle
 * return 0 - Success, -1 - Failed
 */
int OCCChild::prepare_connection()
{
	// just in case
	cur_stmt = NULL;
	cur_results = NULL;
	results_valid = false;
	in_trans = false;
	client_info.clear();
	command_info.clear();
	clear_2pc_state();
	// build_markdowns will clear the vector and put any markdowns into the vector.
	build_markdowns(); // read the directory, see if there are any files.

	return 0;
}

/**
 * @brief Per-connection cleanup
 *
 * This does a rollback to cleanup any un-committed changes.
 * and then it frees the OCI statement handle.
 * If it failed, it will restart the child.
 *
 * @return 0 - OK, -1 - Failed
 */
void OCCChild::cleanup_connection()
{
	// First reset cur_stmt. This way OCC will send the rollback command only if it has to (i.e. if it is really in txn)
	cur_stmt = NULL;

	//rollback
	// Only do roll back if we are in a transaction 

	if (is_in_transaction() && rollback(""))
	{
		// log the error
		WRITE_LOG_ENTRY(logfile, LOG_ALERT,"Failed to rollback after transaction!");
		CalTransaction::Status s(CAL::TRANS_ERROR, CAL::MOD_OCC, CAL::SYS_ERR_INTERNAL, -1);
		CalEvent e(CAL::EVENT_TYPE_ERROR, CAL::MOD_OCC, s, "m_err=Failed to rollback after client disconnects. Shutting down child.");
		client_session.set_status(CAL::SYSTEM_FAILURE);

		// this is a pretty serious problem... probably not connected to oracle
		// or something like that. fake a child shutdown so we reconnect to oracle
		child_shutdown_flag = 1;
	}

	// assume we've just recently been talking to oracle, and it's safe to reset
	// the heartbeat timer
	next_oracle_heartbeat_time = time(NULL) + oracle_heartbeat_frequency;

	if (enable_cache)
	{
		// expire our cache
		cache_expire(true);

		// write cache size
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "cache size is %d", cache_size);
	}
	else
	{
		// clean up statement handle
		free_stmt(&one_stmt);
	}

	// If we have the OCC client-name and if have the server default log-format
	// reset the log format to default one.
	if(!m_client_name.empty()) 
	{
		//clear the client-name so that we don't get into this loop for the
		//next connection without client-name
		m_client_name.clear();

		if (!m_log_format.empty())
			logfile->set_format(m_log_format.c_str());
	}
}

/**
 * @brief dump_session_cache is called at the session termination/cleanup,
 * when session caching is on 
 */
void OCCChild::dump_session_cache()
{
	// log and dump session cache
	if ((cache_hits > 0) || (cache_misses > 0))
	{
		WRITE_LOG_ENTRY(logfile,
				LOG_INFO, "session cache stats: hits = %lu, misses = %lu, hit ratio = %.2f%%, dumps = %lu, expires = %lu, peak size = %d",
				cache_hits, cache_misses,
				100 * (double) cache_hits / (cache_hits + cache_misses),
				cache_dumps,
				cache_expires,
				cache_size_peak);
	}

	if (stmt_cache != NULL && cache_size > 0 )
	{
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "dumping ALL=%d statements from session cache", cache_size);
		for (int i = 0; i < cache_size; i++)
		{
			DO_OCI_HANDLE_FREE(stmt_cache[i]->stmthp, OCI_HTYPE_STMT, LOG_WARNING);
			delete stmt_cache[i];
			stmt_cache[i] = NULL;
		}
		cache_size = 0;
	}

	// reset stats parameters
	cache_hits = cache_misses = cache_dumps = cache_expires = cache_size_peak = 0;
} 

/**
 * @brief Process each command from netstring reader.
 *
 * The default case calls the base class Worker::handle_command()
 *
 * @param _cmd Netstring Command code
 * @param _buffer Rest of command string
 * @return 0 - OK, -1 - Error encountered
 *	Note: returning a non-zero code causes the occ connection to be dropped.
 *	 this is a bad idea except in the very worst of cases.
 */
int OCCChild::handle_command(const int _cmd, std::string &_line)
{
	unsigned int type = 0;
	int rc = 0;
	int markedDown = 0; // if this turns non-zero, it means we are marked down, and should NOT execute

	static std::string bind_values;
	static ub2 bind_value_size[MAX_ARRAY_ROW_NUM];
	static unsigned int bind_value_max_size = 0;
	static unsigned int bind_num = 1;

	static std::string bind_value;
	static std::string transaction_data;

	hb_sender->set_rqid(m_reader->get_count());
	// check host level markdown for everything that could possibly go to the DB
	switch (_cmd) {
	case OCC_PREPARE_SPECIAL:
	case OCC_BIND_NAME:
	case OCC_BIND_OUT_NAME:
	case OCC_EXECUTE:
	case OCC_ROWS:
	case OCC_COLS:
	case OCC_FETCH:
	case OCC_ROLLBACK:
	case OCC_TRANS_PREPARE:
	case OCC_TRANS_START:
		markedDown = check_markdowns(MARKDOWN_HOST, std::string());
		markedDown |= check_markdowns(MARKDOWN_SQL, m_client_session.m_query);
		markedDown |= check_markdowns(MARKDOWN_TABLE, m_client_session.m_query);
		break;
	case OCC_COMMIT:
		markedDown = check_markdowns(MARKDOWN_HOST, std::string());
		markedDown |= check_markdowns(MARKDOWN_SQL, m_client_session.m_query);
		markedDown |= check_markdowns(MARKDOWN_TABLE, m_client_session.m_query);
		markedDown |= check_markdowns(MARKDOWN_COMMIT, std::string());
		break;
	default: // all non-DB commands go this path.
		break;
	}

	switch(_cmd) {
	case OCC_PREPARE:
	case OCC_PREPARE_V2:
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "worker rq_ID = %u", m_reader->get_count());
		// reset m_bind_data of v$session ACTION attribute
		m_bind_data.clear();
		m_scuttle_id.clear();
		// Mark the beginning of a DB txn.
		m_client_session.start_db_txn();

		markedDown = check_markdowns(MARKDOWN_HOST, std::string());
		markedDown |= check_markdowns(MARKDOWN_SQL, _line);
		markedDown |= check_markdowns(MARKDOWN_TABLE, _line);
		if (markedDown)
			break; // just skip the operation, pretend we are happy.
		{
			const std::string * rewritten_sql = 0;
			m_sql_rewritten = false;
			if (m_enable_sql_rewrite) {
				int err = 0;
				m_rewriter.rewrite(_line, rewritten_sql, m_sql_rewritten, err);
				WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "SQL rewrite: rw=%d, err=%d", m_sql_rewritten, err);
			}
			if (m_sql_rewritten) {
				prepare(*rewritten_sql, (_cmd == OCC_PREPARE_V2) ? occ::V2 : occ::V1);
				set_orig_query_hash(_line);
				// Keep a copy of the sql statement
				m_client_session.m_query = *rewritten_sql;
			}
			else {
				prepare(_line, (_cmd == OCC_PREPARE_V2) ? occ::V2 : occ::V1);
				// Keep a copy of the sql statement
				m_client_session.m_query = _line;
			}
		}
		break;
	case OCC_PREPARE_SPECIAL:
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "worker rq_ID = %u", m_reader->get_count());
		// reset m_bind_data of v$session ACTION attribute
		m_bind_data.clear();

	// Mark the beginning of a DB txn.
		m_client_session.start_db_txn();

		// Remember query
		m_client_session.m_query = _line;

		// we should not be marking down "special" statements... they are very special cases.
		if (markedDown)
			break;
		//prepare a special sql statement
		prepare_special(StringUtil::to_uint(_line));
		break;	
	case OCC_SHARD_KEY:
		{
			if (!m_enable_sharding || !m_scuttle_id.empty())
				break;

			std::string name;
			std::vector<std::string> values;
			ShardKeyUtil::parse_shard_key(_line.c_str(), name, values);
			// checking if both are true -
			// * name matches what is configured. parse_shard_key enforces key lower case
			// * values size greater than 0
			uint32_t scuttle_id_val = 0;
			if (values.size() && strcasecmp(name.c_str(), m_shard_key_name.c_str()) == 0)
			{
				// cast to long long
				unsigned long long shard_val = StringUtil::to_ullong(values[0]);
				scuttle_id_val = compute_scuttle_id(shard_val);
				StringUtil::fmt_ulong(m_scuttle_id, scuttle_id_val);
				if (logfile->get_log_level() >= LOG_DEBUG)
				{
					WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "%s OCC_SHARD_KEY, scuttle id %s", 
									_line.c_str(), m_scuttle_id.c_str());
				}
			}
		}

		break;
	case OCC_BIND_NAME:

		bind_value_max_size = 0;
		bind_num = 1;

		//bind a variable
		type = OCC_TYPE_STRING;
		bind_values.clear();
		rc = m_reader->read(&bind_values);
		if(rc==OCC_BIND_TYPE) {
			// Whups!  they sent the bind type first.  Copy off the type and get the actual bind value
			type = StringUtil::to_uint(bind_values);
			bind_values.clear();
			rc = m_reader->read(&bind_values);
		}

		if (rc==OCC_BIND_NUM) {
			// Whups again!  they sent the bind_num second.  Copy off the length and get the actual bind value
			bind_num = StringUtil::to_uint(bind_values);
			if (bind_num > MAX_ARRAY_ROW_NUM) {
				std::ostringstream error;
				error << "Can't array bind " << bind_num << " rows (>" << MAX_ARRAY_ROW_NUM << ") at one time!";
				occ_error(error.str().c_str());
				rc = -1; // This will close connection!
				break;
			}
			bind_values.clear();
			rc = m_reader->read(&bind_values);

			if (rc != OCC_BIND_VALUE_MAX_SIZE)
			{
				occ_error("Commands out of sync OCC_BIND_VALUE_MAX_SIZE!");
				rc = -1;
				break;
			}

			bind_value_max_size = StringUtil::to_uint(bind_values);
			if ((bind_num> 1) && (bind_value_max_size> MAX_ARRAY_DATA_SIZE)) {
				std::ostringstream error;
				error << "Size in array bind " << bind_value_max_size << " can't be more than " << MAX_ARRAY_DATA_SIZE << " bytes!";
				occ_error(error.str().c_str()); 
				rc = -1; // This will close connection! 
				break;
			}
			bind_values.clear();
			rc = m_reader->read(&bind_values);
		}

		if (bind_num == 1) bind_value_max_size= bind_values.length();
		if (bind_num >= 1) bind_value_size[0] = bind_values.length();

		for (unsigned int i=0; 1; ++i) {
			if (rc!=OCC_BIND_VALUE) {
				occ_error("Commands out of sync OCC_BIND_VALUE!");
				rc = -1;    // This will close connection!
				break;
			}

			if ((bind_num > 1) && (bind_value_size[i] > (int) bind_value_max_size))
			{
				std::ostringstream error;
				error << "Value length " << bind_value_size[i] << " is larger than max length " << bind_value_max_size << "!";
				occ_error(error.str().c_str()); 
				rc = -1;
				break;
			}

			if (i == bind_num-1) break;

			bind_value.clear();
			rc = m_reader->read(&bind_value);

			bind_values.append("\0", 1);
			bind_values.resize((i+1)*(bind_value_max_size+1));
			bind_values.append(bind_value);

			bind_value_size[i+1] = bind_value.length();
		}

		if (rc == -1 || markedDown) break;

		bind(_line, bind_values, bind_value_size, bind_value_max_size, bind_num, (DataType)type);
			
		if (m_sql_rewritten) {
			// check if the bind variable is the shard key
			const char* bind_var_name = _line.c_str();
			if (*bind_var_name == ':')
				bind_var_name++;
			
			uint sklen = m_shard_key_name.length();
			if (0 == strncasecmp(bind_var_name, m_shard_key_name.c_str(), sklen)) {
				// extra check, to accept names like party_id_0
				if ((bind_var_name[sklen] == 0) || (strncmp(bind_var_name + sklen, "_0", 2) == 0)) {
					// bind the scuttle_id
					std::string scuttle_id(m_scuttle_attr_name.c_str(), m_scuttle_attr_name.length());
					if (bind_values.length() == 0) {
						// null
						// m_scuttle_id should be set to null but it's just a buffer where empty means no scuttle_id at all
						bind(scuttle_id, bind_values, bind_value_size, 0, 1, (DataType)OCC_TYPE_STRING);
						if (logfile->get_log_level() >= LOG_VERBOSE)
							WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "m_scuttle_id null in sql rewrite to mirror null shard key value");
					} else {
						unsigned long long scuttle_id_val = StringUtil::to_ullong(bind_values);
						StringUtil::fmt_ulong(bind_values, compute_scuttle_id(scuttle_id_val));
						bind_value_max_size = bind_values.length();
						bind_num = 1;
	
						if (m_scuttle_id.empty()){
							m_scuttle_id = bind_values;
							if (logfile->get_log_level() >= LOG_VERBOSE)
								WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "m_scuttle_id %s in sql rewrite", m_scuttle_id.c_str());
						}
	
						type = OCC_TYPE_STRING;
						bind(scuttle_id, bind_values, bind_value_size, bind_value_max_size, bind_num, (DataType)type);
					} // non-null
				}
			}

		} else {
			
			if (m_enable_sharding && m_scuttle_id.empty()) // only do this when sharding is enabled.
			{
				// check if the bind variable is "scuttle_id"
				const char* bind_var_name = _line.c_str();
				if (*bind_var_name == ':')
					bind_var_name++;
			
				if (0 == strcasecmp(bind_var_name, m_scuttle_attr_name.c_str())) {
					m_scuttle_id = bind_values;
					if (logfile->get_log_level() >= LOG_VERBOSE)
						WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "m_scuttle_id %s in sql binding", m_scuttle_id.c_str());
				}
			}
		}
		break;
	case OCC_BIND_OUT_NAME:
		//bind an out-bound variable
		type = OCC_TYPE_STRING;
		if (markedDown)
			break;
		bind_out(_line, (DataType)type);
		break;
	case OCC_EXECUTE:
		if (mklist.doMarkdown(host_name, 
					mark_host_name, 
					m_client_session.m_query, 
					logfile))
		{
			markedDown = 1;
		}
		if (markedDown) {
			// send an error to the invoker.
			std::ostringstream results;
			results << "m_err=Markdown prevented operation of " << m_client_session.m_query;
			CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_MARKED_DOWN, -1);
			CalEvent e(CAL::EVENT_TYPE_MARKDOWN, CAL_NAME_EXECUTE, s, results.str());
			client_session.set_status(CAL::SYSTEM_FAILURE);
			m_writer->write(OCC_MARKDOWN);
			WRITE_LOG_ENTRY(logfile, LOG_WARNING, "%s", results.str().c_str());
			break;
		}
		{
			// execute a prepared statement uing a CAL txn as a timer
			// Note: to support n-Way SQL analysis and tuning - this timer is
			// active when either CAL switch is "ON"
			// Normalize the query.
			std::string m_query_str(m_client_session.m_query.c_str());
			StringUtil::normalizeSQL(m_query_str);

			ulong  hash_val = CalActivity::SendSQLData (m_query_str);
			StringUtil::fmt_ulong(m_query_hash, hash_val);

			CalTransaction* c = NULL;
			if (!(cur_results && results_valid))
			{
				c = new CalTransaction(CAL_TRANSACTION_EXEC);
				c->SetName(m_query_hash);
				c->AddData("HOST", m_dbhost_name);
				if (m_sql_rewritten) {
					c->AddData("sqlhash", m_orig_query_hash);
				}
			}
			OCIAttrSet((dvoid *)authp, OCI_HTYPE_SESSION, (dvoid *) const_cast<char*>(m_bind_data.c_str()), 
					   m_bind_data.length(), OCI_ATTR_CLIENT_IDENTIFIER, errhp);
			
			if (m_scuttle_id.empty())
				StringUtil::fmt_int(m_scuttle_id, -1);
			
			OCIAttrSet((dvoid *)authp, OCI_HTYPE_SESSION, (dvoid *) const_cast<char*>(m_scuttle_id.c_str()), 
						   m_scuttle_id.length(), OCI_ATTR_CLIENT_INFO, errhp);

			execute(rc);
			if (cur_stmt)
			{
				if ((cur_stmt->type != SELECT_STMT) && (cur_stmt->type != SELECT_FOR_UPDATE_STMT))
				{
					cur_stmt = NULL;
				}
			}

			if (c)
			{
				c->SetStatus(CAL::TRANS_OK); // SQL errors are logged to CAL separately
				delete c;
				c = NULL;
			}
			
			m_scuttle_id.clear();
		}
		m_requests_cnt++;
		break;
	case OCC_ROWS:
		if (markedDown)
		{
			CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_MARKED_DOWN, -1);
			CalEvent e(CAL::EVENT_TYPE_MARKDOWN, "Rows", s, "m_err=row_count() blocked.");
			client_session.set_status(CAL::SYSTEM_FAILURE);
			m_writer->write(OCC_MARKDOWN);
			break;
		}
		//return a row count
		row_count();
		break;
	case OCC_COLS:
		{
			if (markedDown)
			{
				CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_MARKED_DOWN, -1);
				CalEvent e(CAL::EVENT_TYPE_MARKDOWN, "Cols", s, "m_err=col_names() blocked.");
				client_session.set_status(CAL::SYSTEM_FAILURE);
				m_writer->write(OCC_MARKDOWN);
				break;
			}
			//return column count and column headings
			StmtCacheEntry *stmt = get_cur_stmt();
			if (stmt != NULL)
			{
				col_names(stmt->num_cols, stmt->columns);
			}
			break;
		}
	case OCC_COLS_INFO:
		{
			if (markedDown)
			{
				CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_MARKED_DOWN, -1);
				CalEvent e(CAL::EVENT_TYPE_MARKDOWN, "Cols", s, "m_err=col_types() blocked.");
				client_session.set_status(CAL::SYSTEM_FAILURE);
				m_writer->write(OCC_MARKDOWN);
				break;
			}
			//return column count and column headings
			StmtCacheEntry *stmt = get_cur_stmt();
			if (stmt != NULL)
			{
				col_info(stmt->num_cols, stmt->columns);
			}
			break;
		}
	case OCC_FETCH:
		if (markedDown)
		{
			CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_MARKED_DOWN, -1);
			CalEvent e(CAL::EVENT_TYPE_MARKDOWN, "Fetch", s, "m_err=fetch() blocked.");
			client_session.set_status(CAL::SYSTEM_FAILURE);
			m_writer->write(OCC_MARKDOWN);
			break;
		}
		
		{
			CalTransaction cal_trans("FETCH");
			cal_trans.SetName(m_query_hash);
			cal_trans.AddData("HOST", m_dbhost_name);
			
			//fetch a block of rows
			long long fetched_bsize = fetch(_line);
			if (fetched_bsize >= 0)
			{
				std::ostringstream pdata;
				pdata << "psize=" << fetched_bsize;
				WRITE_LOG_ENTRY(logfile, LOG_DEBUG, pdata.str().c_str());
				cal_trans.AddData(pdata.str());
			}
			else
				WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "fetch payload size < 0, write error occurred");
			if (cur_stmt != NULL) { // quietly check if stmt is there to get more data
				cal_trans.AddData("hasNext","true");
			}
			cal_trans.SetStatus(CAL::TRANS_OK);
			cal_trans.Completed();
		}
		break;
	case OCC_COMMIT:
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "worker rq_ID = %u", m_reader->get_count());
		// For stand-alone COMMIT call (e.g. recoman)
		m_client_session.start_db_txn();

		if (markedDown) {
			CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_MARKED_DOWN, -1);
			CalEvent e(CAL::EVENT_TYPE_MARKDOWN, "Commit", s, "m_err=commit() blocked.");
			client_session.set_status(CAL::SYSTEM_FAILURE);
			m_writer->write(OCC_MARKDOWN);
			WRITE_LOG_ENTRY(logfile, LOG_WARNING, "Markdown prevented commit.");
			break;
		}
		xid = _line;
		if(commit(xid)==0) {
			//send a success
			eor(EORMessage::FREE, OCC_OK);
			m_writer->write();
		}
		cur_stmt = NULL;
		break;
	case OCC_ROLLBACK:
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "worker rq_ID = %u", m_reader->get_count());
		// For stand-alone ROLLBACK call (by recoman, e.g.)
		m_client_session.start_db_txn();

		if (markedDown) {
			CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_MARKED_DOWN, -1);
			CalEvent e(CAL::EVENT_TYPE_MARKDOWN, "Rollback", s, "m_err=rollback() blocked.");
			client_session.set_status(CAL::SYSTEM_FAILURE);
			m_writer->write(OCC_MARKDOWN);
			break;
		}
		xid = _line;
		if(rollback(xid)==0) {
			//send a success
			eor(EORMessage::FREE, OCC_OK);
			m_writer->write();
		}
		cur_stmt = NULL;
		break;
	case OCC_TRANS_START:
		{
			markedDown |= check_markdowns(MARKDOWN_TRANS, std::string()); // this is the only place we care about trans markdown.

			unsigned int timeout = m_default_trans_timeout;
			// This assume OCITransStart will only be called
			// by participants not the point-site.
			TransRole role = PARTICIPANT;
			bool repeat = false;

			do
			{
				rc = m_reader->read(&transaction_data);
				if (rc == OCC_TRANS_TIMEOUT)
				{
					timeout = StringUtil::to_uint(transaction_data);
					repeat = !repeat;	// Do it one more time
				}
				else if (rc == OCC_TRANS_ROLE)
				{
					role = (TransRole)StringUtil::to_uint(transaction_data);
					repeat = !repeat;	// Do it one more time
				}
				else
				{
					break;
				}
			} while (repeat);

			if (markedDown) {
				CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_MARKED_DOWN, -1);
				CalEvent e(CAL::EVENT_TYPE_MARKDOWN, "TransStart", s, "m_err=trans_start() blocked.");
				client_session.set_status(CAL::SYSTEM_FAILURE);
				m_writer->write(OCC_MARKDOWN);
				WRITE_LOG_ENTRY(logfile, LOG_WARNING, "Transstart attempted, markdown blocking");
				break;
			}

			// For now, we don't want the client to decide the coupling type.
			xid = _line;
			trans_start(xid, timeout, role, occ::LOOSELY_COUPLED);
			break;
		}
	case OCC_TRANS_PREPARE:
		if (markedDown)
		{
			CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_MARKED_DOWN, -1);
			CalEvent e(CAL::EVENT_TYPE_MARKDOWN, "TransPrepare", s, "m_err=trans_prepare() blocked.");
			client_session.set_status(CAL::SYSTEM_FAILURE);
			m_writer->write(OCC_MARKDOWN, "");
			break;
		}
		trans_prepare(_line);
		break;
	case OCC_INT_CLIENT_INFO:
		{
			if (markedDown)
			{
				CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_MARKED_DOWN, -1);
				CalEvent e(CAL::EVENT_TYPE_MARKDOWN, "ClientInfo", s, "m_err=client_info() blocked.");
				client_session.set_status(CAL::SYSTEM_FAILURE);
				m_writer->write(OCC_MARKDOWN);
				break;
			}
			//send server Info
			std::string server_info = CalTransaction::GetCurrentPoolInfo();
			m_writer->write(SERVER_INT_INFO, server_info);

			WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Client info: %s", _line.c_str());
			WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Server Info: %s", server_info.c_str());
			//m_writer->write(OCC_OK); // Make client to proceed.


			// Set the client info only if it's not already set
			if(client_info.empty())
			{
				client_info = _line;
				process_pool_info(client_info);

				unsigned int last_idx = client_info.rfind(CLIENT_NAME_PREFIX);
				if (last_idx != std::string::npos)
				{
					m_client_name.clear(); // Clear previous data if-any, little paranoia.
					m_client_name = client_info.substr(last_idx + CLIENT_NAME_PREFIX.length());
					StringUtil::trim(m_client_name); // Remove any white-spaces
				}
			}

			break;
		}
	case OCC_CLIENT_INFO:
		{
			if (markedDown)
			{
				CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_MARKED_DOWN, -1);
				CalEvent e(CAL::EVENT_TYPE_MARKDOWN, "ClientInfo", s, "m_err=client_info() blocked.");
				client_session.set_status(CAL::SYSTEM_FAILURE);
				m_writer->write(OCC_MARKDOWN);
				break;
			}
			//send server Info
			std::string server_info = CalTransaction::GetCurrentPoolInfo();

			m_writer->add(OCC_OK, server_info);
			eor(is_in_transaction() ? EORMessage::IN_TRANSACTION : EORMessage::FREE);
			m_writer->write();

			WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Client info: %s", _line.c_str());
			WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Server Info: %s", server_info.c_str());
			//m_writer->write(OCC_OK); // Make client to proceed.


			// Set the client info only if it's not already set
			if(client_info.empty())
			{
				client_info = _line;
				process_pool_info(client_info);

				unsigned int last_idx = client_info.rfind(CLIENT_NAME_PREFIX);
				if (last_idx != std::string::npos)
				{
					m_client_name.clear(); // Clear previous data if-any, little paranoia.
					m_client_name = client_info.substr(last_idx + CLIENT_NAME_PREFIX.length());
					StringUtil::trim(m_client_name); // Remove any white-spaces
				}
			}

			if (cur_stmt != NULL)
				CalEvent e(CAL::EVENT_TYPE_MESSAGE, "CLIENT_INFO_IN_TXN", "0");			

			break;
		}
	case OCC_BACKTRACE:
		if(backtrace_log_level != -1)
		{
			WRITE_LOG_ENTRY(logfile, backtrace_log_level, "backtrace: %s", _line.c_str());
		}
		// no reply
		break;

	case OCC_SQL_STMT_CACHING:  // PPSCR00377721 session caching
		// unblock client
		m_writer->write(OCC_OK);
		// NOOP
		break;

	case CLIENT_CAL_CORRELATION_ID:
		rc = Worker::handle_command(CLIENT_CAL_CORRELATION_ID, _line);
		OCIAttrSet((dvoid *)authp, OCI_HTYPE_SESSION, (dvoid *) const_cast<char*>(m_corr_id.c_str()), m_corr_id.length(), OCI_ATTR_ACTION, errhp);

		if (cur_stmt != NULL)
			CalEvent e(CAL::EVENT_TYPE_MESSAGE, "CORRID_IN_TXN", "0");			
		else if (!is_in_transaction())
			set_dedicated(false); // this command has no response, so setting this flag here

		break;

	case SERVER_PING_COMMAND :
		rc = Worker::handle_command(_cmd, _line);

		if (cur_stmt != NULL)
			CalEvent e(CAL::EVENT_TYPE_MESSAGE, "PING_IN_TXN", "0");			

		break;

	default:
		rc = Worker::handle_command(_cmd, _line);
		break;
	}
	return rc;
}

/**
 * @brief handle signals
 *
 */
void OCCChild::sigfunc(int _sig)
{
	if ((_sig == SIGALRM) && heartbeat_alarm_set)
	{
		// apparently the OCIServerVersion heartbeat call locked up.  We can't
		// leave this signal handler, because that would put us back into the
		// misbehaving OCIServerVersion code.  So try to log the error from here
		// and then exit.
		WRITE_LOG_ENTRY(logfile, LOG_WARNING, "caught alarm during OCIServerVersion heartbeat -- exiting");
		m_client_session.end_db_txn();
		exit(0);
	}

	if ((_sig == SIGALRM) && !child_shutdown_flag)
	{
		return;
	}

	Worker::sigfunc(_sig);
}

//-----------------------------------------------------------------------------

int OCCChild::connect(const std::string& db_username, const std::string& db_password)
{
	int rc = OCI_SUCCESS;
	// shouldn't already be connected...
	if (envhp != NULL)
		return -1;

	// just to make sure
	attached = false;
	has_session = false;
	
	int oracle_connect_timeout = config->get_int("oracle_connect_timeout", 60 * 25);
	heartbeat_alarm_set = true;
	alarm(oracle_connect_timeout);
	WRITE_LOG_ENTRY(logfile, LOG_VERBOSE,"oracle_connect_timeout %i", oracle_connect_timeout);

	if(db_username.empty()) {
		WRITE_LOG_ENTRY(logfile, LOG_ALERT,"username not found in the config file.");
		CalTransaction::Status s(CAL::TRANS_FATAL, CAL::MOD_OCC, CAL::SYS_ERR_CONFIG, -1);
		CalEvent e(CAL::EVENT_TYPE_FATAL, "Oracle Session", s, "m_err=oracle_username not found in config.");
		client_session.get_session_transaction()->AddDataToRoot("m_err", "ORALCE_USERNAME_NOT_FOUND");
		client_session.get_session_transaction()->AddDataToRoot("m_errtype","CONNECT");
		client_session.get_session_transaction()->SetStatus(CAL::SYSTEM_FAILURE, CalActivity::CAL_SET_ROOT_STATUS);
		return -1;
	}

	if(db_password.empty()) {
		WRITE_LOG_ENTRY(logfile, LOG_ALERT,"password not found in the config file.");
		CalTransaction::Status s(CAL::TRANS_FATAL, CAL::MOD_OCC, CAL::SYS_ERR_CONFIG, -1);
		CalEvent e(CAL::EVENT_TYPE_FATAL, "Oracle Session", s, "m_err=password not found in config.");
		client_session.get_session_transaction()->AddDataToRoot("m_err", "PWD_NOT_FOUND");
		client_session.get_session_transaction()->AddDataToRoot("m_errtype","CONNECT");
		client_session.get_session_transaction()->SetStatus(CAL::SYSTEM_FAILURE, CalActivity::CAL_SET_ROOT_STATUS);
		return -1;
	}

	// before knowing what OCIEnvCreate does if failing, use this to decide if calling disconnect()
	m_oracle_init_called = true;

	CalTransaction cal_trans("CONNECT");
	cal_trans.SetName(m_dbhost_name);
	cal_trans.SetStatus(CAL::TRANS_OK);
	// initialize
	rc = OCIEnvCreate(&envhp, OCI_THREADED, NULL, NULL, NULL, NULL, 0, NULL);
	if (rc != OCI_SUCCESS)
	{
		log_oracle_error(rc, "Failed to create environment");
		return -1;
	}

	//generate an error handle
	rc = OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &errhp, OCI_HTYPE_ERROR, 
			(size_t) 0, (dvoid **) 0);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to get an error handle.");
		return -1;
	}

	rc = OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &errhndl_batch, OCI_HTYPE_ERROR,
			(size_t) 0, (dvoid **) 0);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to get an error batch handle.");
		return -1;
	}

	rc = OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &errhndl_batch2, OCI_HTYPE_ERROR,
			(size_t) 0, (dvoid **) 0);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to get an error batch handle.");
		return -1;
	}

	//server context handle
	rc = OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &srvhp, OCI_HTYPE_SERVER,
			(size_t) 0, (dvoid **) 0);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to get an server context handle.");
		return -1;
	}

	//create a session handle
	rc = OCIHandleAlloc((dvoid *) envhp, (dvoid **)&authp, (ub4) OCI_HTYPE_SESSION,
			(size_t) 0, (dvoid **) 0);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to create a session handle.");
		return -1;
	}

	//service context handle
	rc = OCIHandleAlloc( (dvoid *) envhp, (dvoid **) &svchp, OCI_HTYPE_SVCCTX,
			(size_t) 0, (dvoid **) 0);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to get a service context handle.");
		return -1;
	}

	// allocate Transaction Handle
	rc = OCIHandleAlloc((dvoid *)envhp, (dvoid **)&transhp, OCI_HTYPE_TRANS, (size_t) 0, (dvoid **) 0);
	if (rc != OCI_SUCCESS)
	{
		log_oracle_error(rc, "Failed to get a transaction handle.");
		return -1;
	}

	//attach to the oracle server
	rc = OCIServerAttach( srvhp, errhp, (text *) const_cast<char *> (""), strlen(""), 0);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to attach to the oracle server.");
		cal_trans.SetStatus(CAL::SYSTEM_FAILURE);
		return -1;
	}
	attached = true;

	//set attribute server context in the service context
	rc = OCIAttrSet( (dvoid *) svchp, OCI_HTYPE_SVCCTX, (dvoid *)srvhp, (ub4) 0,
			OCI_ATTR_SERVER, (OCIError *) errhp);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to set the service context attribute.");
		return -1;
	}

	//set the session
	rc = OCIAttrSet((dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX,
			(dvoid *) authp, (ub4) 0,
			(ub4) OCI_ATTR_SESSION, errhp);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to set the session.");
		return -1;
	}

	// associate transaction handle with service context
	rc = OCIAttrSet((dvoid *)svchp, OCI_HTYPE_SVCCTX, transhp, 0, OCI_ATTR_TRANS, errhp);
	if (rc != OCI_SUCCESS)
	{
		log_oracle_error(rc, "Failed to set the transaction handle.");
		return -1;
	}

	if ( oracle_lobprefetch_size > 0) {
		rc = OCIAttrSet ((dvoid *)authp, (ub4) OCI_HTYPE_SESSION, (void *)&oracle_lobprefetch_size,
				0, (ub4) OCI_ATTR_DEFAULT_LOBPREFETCH_SIZE, errhp);
		if (rc!=OCI_SUCCESS) {
			log_oracle_error(rc, "Failed to set OCI_ATTR_DEFAULT_LOBPREFETCH_SIZE");
			rc = OCI_SUCCESS;
		}
	}

	//prepare the username
	rc = OCIAttrSet((dvoid *) authp, (ub4) OCI_HTYPE_SESSION,
			(dvoid *) const_cast<char*>(db_username.c_str()), (ub4) strlen(db_username.c_str()),
			(ub4) OCI_ATTR_USERNAME, errhp);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to set the username.");
		return -1;
	}

	rc = OCIAttrSet((dvoid *) authp, (ub4) OCI_HTYPE_SESSION,
			(dvoid *) const_cast<char*>(db_password.c_str()), (ub4) strlen(db_password.c_str()),
			(ub4) OCI_ATTR_PASSWORD, errhp);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to set the password.");
		return -1;
	}


	//create the user session
	rc = OCISessionBegin(svchp,	 errhp, authp, OCI_CRED_RDBMS, 
			(ub4) OCI_DEFAULT);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to log in the user session.");
		return -1;
	}

	// moving OCI_ATTR_MODULE after OCISessionBegin() for billmelater db
#ifdef OCI_ATTR_MODULE // OCI_ATTR_MODULE is for 10g only

	WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "module info=%S", m_module_info.c_str());
	rc = OCIAttrSet((dvoid *)authp, OCI_HTYPE_SESSION, (dvoid *) const_cast<char*>(m_module_info.c_str()), m_module_info.length(), OCI_ATTR_MODULE, errhp);
	if(rc!=OCI_SUCCESS) {
		log_oracle_error(rc,"Failed to set oracle module.");
	}

#endif

	has_session = true;
	cal_trans.Completed();

	heartbeat_alarm_set = false;
	alarm(0);

	// allocate these here because they contain OCI data which is associated
	// with the environment handle, so if we disconnect and then reconnect,
	// we can't use the same ones
	bind_array = new std::vector<std::shared_ptr<OCCBind> >();
	out_bind_array = new std::vector<std::shared_ptr<OCCBindInOut> >;

	// create the statement cache
	stmt_cache = new StmtCacheEntry*[max_cache_size];
	for (int i = 0; i < max_cache_size; i++)
		stmt_cache[i] = NULL;

	if(oracle_heartbeat_frequency) {
		// we just connected, so start the heartbeat timer from now.
		// set it to go off a random amount early, so a freshly-
		// started and inactive server doesn't send out waves of synchronized
		// heartbeats
		next_oracle_heartbeat_time = time(NULL) + ((rand() >> 5) % oracle_heartbeat_frequency);
	}

	if (enable_cache) {
		next_cache_expire_time = time(NULL) + ((rand() >> 4) % cache_expire_frequency);
	}

	return 0;
}

//-----------------------------------------------------------------------------

int OCCChild::disconnect()
{
	if (!m_oracle_init_called)
		return 0;

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Disconnecting from Oracle ...");

	int rc = -1;

	// clean up our mess
	if (bind_array)
	{
		delete bind_array;
		bind_array = NULL;
	}

	if (out_bind_array)
	{
		delete out_bind_array;
		out_bind_array = NULL;
	}

	if (stmt_cache != NULL)
	{
		for (int i = 0; i < cache_size; i++)
		{
			DO_OCI_HANDLE_FREE(stmt_cache[i]->stmthp, OCI_HTYPE_STMT, LOG_WARNING);
			delete stmt_cache[i];
		}
		delete[] stmt_cache;
		stmt_cache = NULL;
		cache_size = 0;
	}
	free_stmt(&one_stmt);

	// clean up session variables statemend handle
	if( m_session_var_stmthp != NULL )
	{
		if (!(rc = DO_OCI_HANDLE_FREE( m_session_var_stmthp, OCI_HTYPE_STMT, LOG_ALERT)))
		{
			log_oracle_error(rc, "Failed to free statement handle.");
			return -1;
		}
		m_session_var_stmthp = NULL;
	}

	if (has_session)
	{
		// end the session
		rc = OCISessionEnd(svchp, errhp, authp, OCI_DEFAULT);
		if (rc != OCI_SUCCESS)
			log_oracle_error(rc, "failed to OCISessionEnd");
		has_session = false;
	}

	if (attached)
	{
		// detach from server
		rc = OCIServerDetach(srvhp, errhp, OCI_DEFAULT);
		if (rc != OCI_SUCCESS)
			log_oracle_error(rc, "failed to OCIServerDetach");
		attached = false;
	}

	// free a bunch of handles
	DO_OCI_HANDLE_FREE(srvhp, OCI_HTYPE_SERVER, LOG_ALERT);
	DO_OCI_HANDLE_FREE(authp, OCI_HTYPE_SESSION, LOG_ALERT);
	DO_OCI_HANDLE_FREE(transhp, OCI_HTYPE_TRANS, LOG_WARNING);
	DO_OCI_HANDLE_FREE(svchp, OCI_HTYPE_SVCCTX, LOG_ALERT);
	DO_OCI_HANDLE_FREE(errhp, OCI_HTYPE_ERROR, LOG_ALERT);
	DO_OCI_HANDLE_FREE(errhndl_batch, OCI_HTYPE_ERROR, LOG_ALERT);
	DO_OCI_HANDLE_FREE(errhndl_batch2, OCI_HTYPE_ERROR, LOG_ALERT);
	DO_OCI_HANDLE_FREE(envhp, OCI_HTYPE_ENV, LOG_WARNING);

	rc = OCITerminate(OCI_DEFAULT);
	if (rc != OCI_SUCCESS)
	{
		WRITE_LOG_ENTRY(logfile, LOG_WARNING, "failed to OCITerminate, rc = %d", rc);
		CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_ORACLE, rc);
		CalEvent e(CAL::EVENT_TYPE_WARNING, CAL_EVENT_ORACLE, s, "m_err=Failed to OCITerminate");
		client_session.set_status(CAL::SYSTEM_FAILURE);
	}

	return 0;
}

/******************************************************************************
 * if enough time has passed, check to make sure we're still connected to
 * oracle by running OCIServerVersion
 */
void OCCChild::oracle_heartbeat()
{
	// if we're not attached or heartbeats aren't enabled, do nothing
	if(!attached || !oracle_heartbeat_frequency)
		return;

	time_t now = time(NULL);
	// if it hasn't been long enough since the last heartbeat, do nothing
	if(now < next_oracle_heartbeat_time)
		return;

	// OCIServerVersion locks up sometimes apparently.	There's an Oracle TAR
	// open about it, but until that's resolved, we're putting an alarm() around
	// the call and logging when it fails.

	// oracle_heartbeat() is only ever called from post_accept, and the parent
	// Worker alarm is always cleared right after that, so it's okay to
	// overwrite the alarm here.
	heartbeat_alarm_set = true;
	alarm(5);

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Sending heartbeat to Oracle");
	next_oracle_heartbeat_time = now + oracle_heartbeat_frequency;
	// don't actually care about the server version, so ask for the first 1
	// bytes of it.	 Still need a char* we can pass in, though.
	// The old code ask for 0 bytes and that seems to be broken against Oracle 9i.
	// In any case, that optimization is overkill.
	char unused[2];
	int rc = OCIServerVersion(srvhp, errhp, (text*)unused, 1, OCI_HTYPE_SERVER);

	// made it through the OCIServerVersion call... kill the alarm.
	alarm(0);
	heartbeat_alarm_set = false;

	if(rc != OCI_SUCCESS) {
		WRITE_LOG_ENTRY(logfile, LOG_INFO, "heartbeat failed (no longer connected to Oracle)");
		log_oracle_error(rc, "heartbeat failed");
		child_shutdown_flag = 1;
		// Oracle is toast, just exit.  Does it make sense to do any cleanup?
		exit(9);
	}
}

//-----------------------------------------------------------------------------

int OCCChild::set_oci_nonblocking(bool _nonblock, const StmtCacheEntry *_stmt)
{
	int	 rc;

	// first determine the current value of the attribute
	ub1 mode;
	rc = OCIAttrGet(srvhp, OCI_HTYPE_SERVER, &mode, NULL, OCI_ATTR_NONBLOCKING_MODE, errhp);
	if (rc != OCI_SUCCESS)
	{
		if (_stmt)
			sql_error(rc, _stmt);
		else
			log_oracle_error(rc, "failed to get nonblocking mode setting");
		return -1;
	}

	// nothing to do if already set how we want it
	if (_nonblock ? mode : !mode)
		return 0;

	// setting the attribute toggles it always -- no way to specify what we desire
	rc = OCIAttrSet(srvhp, OCI_HTYPE_SERVER, NULL, 0, OCI_ATTR_NONBLOCKING_MODE, errhp);
	if (rc != OCI_SUCCESS)
	{
		if (_stmt)
			sql_error(rc, _stmt);
		else
			log_oracle_error(rc, "failed to change nonblocking mode");
		return -1;
	}

	// all done
	return 0;
}

//-----------------------------------------------------------------------------

int OCCChild::abort_oci_nonblocking(void)
{
	int	 rc;

	// this basically sends an "abort" message to the server
	OCIBreak(svchp, errhp);

	if (oracle_fd >= 0)
	{
		// NOTE: OCI nonblocking calls are really crappy. The OCIReset() call
		// does a busy-poll on the file descriptor using 100% cpu until it
		// gets a response. If the server actually died, this can wind up being
		// really bad.
		fd_set fds;
		FD_ZERO(&fds);
		FD_SET(oracle_fd, &fds);
		struct timeval timeout;
		timeout.tv_sec = 120;
		timeout.tv_usec = 0;
		do
		{
			// be smart and do a select on the socket
			rc = select(oracle_fd + 1, &fds, NULL, NULL, &timeout);
		}
		while ((rc < 0) && (errno == EINTR));

		// on EOF or error on the oracle channel, then we're hung
		// up, so abort the whole thing
		if (rc < 0)
		{
			std::ostringstream msg;
			msg << "m_err=error on select() while aborting statement, errno = " << errno;
			WRITE_LOG_ENTRY(logfile, LOG_WARNING, "%s", msg.str().c_str());
			CalEvent e(CAL::EVENT_TYPE_WARNING, "Heartbeat", CalTransaction::Status(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_INTERNAL, errno), msg.str());
			return -1;
		}
		if (rc == 0)
		{
			WRITE_LOG_ENTRY(logfile, LOG_WARNING, "timeout on select() while aborting statement");
			CalEvent e(CAL::EVENT_TYPE_WARNING, "Heartbeat", CalTransaction::Status(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_INTERNAL, -1), "m_err=timeout on select() while aborting statement");
			return -1;
		}
	}

	// this waits for an "abort okay" response from the server
	OCIReset(svchp, errhp);

	// finally, set oci back to blocking mode
	return set_oci_nonblocking(false);
}

//-----------------------------------------------------------------------------

int OCCChild::find_oracle_fd(void)
{
#ifdef __LINUX__
	int ora_fd = -1;

	// get path to our fd's in /proc
	char path[32];
	if (snprintf(path, sizeof(path), "/proc/%d/fd", getpid()) >= (int) sizeof(path))
		return -1;
	DIR *d = opendir(path);
	if (d == NULL)
		return -1;

	// read the directory
	struct dirent64 *ent;
	while ((ent = readdir64(d)) != NULL)
	{
		// skip dotfiles
		if (ent->d_name[0] == '.')
			continue;

		// build full path to the dir entry
		char fdpath[64];
		if (snprintf(fdpath, sizeof(fdpath), "%s/%s", path, ent->d_name) >= (int) sizeof(fdpath))
		{
			closedir(d);
			return -1;
		}

		// stat it
		struct stat64 fdstat;
		if (stat64(fdpath, &fdstat) < 0)
		{
			closedir(d);
			return -1;
		}

		// only interested in sockets
		if (!S_ISSOCK(fdstat.st_mode))
			continue;

		// convert the name (which should be a string) to the integer file descriptor
		char *endptr = NULL;
		int fd = strtol(ent->d_name, &endptr, 10);
		if ((*endptr != 0) || (fd < 0))
		{
			closedir(d);
			return -1;
		}

		// skip our known connections
		if ((fd == m_data_fd) || (fd == m_ctrl_fd))
			continue;

		// get info about the socket
		struct sockaddr addr;
		socklen_t addrlen = sizeof(addr);
		if (getsockname(fd, &addr, &addrlen) < 0)
		{
			closedir(d);
			return -1;
		}

		// only looking at IPv4 sockets
		if (addr.sa_family != PF_INET)
			continue;

		// semi-hack for avoiding CAL problem... ignore any loopback addresses
		// hopefully connection to caldaemon will always be on the loopback interface
		if (ntohl(((sockaddr_in *) &addr)->sin_addr.s_addr) == INADDR_LOOPBACK)
			continue;

		if (ora_fd >= 0)
		{
			// hmm found more than one. i am confused, so give up...
			closedir(d);
			return -1;
		}

		// okay! it's a real IPv4 socket. this really should be the oracle socket.
		ora_fd = fd;
	}

	// clean up
	closedir(d);

	// return the file descriptor we found
	return ora_fd;
#else
	// dunno how to find it unless on linux...
	return -1;
#endif
}

//-----------------------------------------------------------------------------

int OCCChild::run_oci_func(int _func, const StmtCacheEntry *_stmt, const OCIFuncParams& _params, int *_oci_rc)
{
	int	 rc;

	// if we're supposed to use non-blocking OCI mode, then attempt to set that
	// mode here. pass in the statement handle so we return OCC_ERROR to the client
	// if this fails.
	if (use_nonblock && set_oci_nonblocking(true, _stmt))
		return OCIR_ERROR;

	// build some initial timevals
	struct timeval tv_next_ping, tv_interval;
	gettimeofday(&tv_next_ping, NULL);
	tv_interval.tv_sec = ping_interval;
	tv_interval.tv_usec = 0;
	tv_add(tv_next_ping, tv_interval, tv_next_ping);

	// Note: 
	// to be ultra cautious: hb_sender should be enabled/diabled right before/after blocking 
	// OCIStmtExecute/OCIStmtFetch call
	// 
	while (true)
	{
		switch (_func)
		{
		case OCC_EXECUTE:

			if (hb_sender)
				hb_sender->enable();

			{
				TxnStartTime t(*this);
				// execute the statement
				if (_params.exec_params.iterations <= 1)
					rc = OCIStmtExecute(svchp, _stmt->stmthp, errhp, _params.exec_params.iterations, 0, NULL, NULL, OCI_DEFAULT);
				else
					rc = OCIStmtExecute(svchp, _stmt->stmthp, errhp, _params.exec_params.iterations, 0, NULL, NULL, OCI_BATCH_ERRORS);
			};

			if (hb_sender)
				hb_sender->disable();

			break;
		case OCC_FETCH:

			if (hb_sender)
				hb_sender->enable();
			{
				TxnStartTime t(*this);
				// fetch rows
				rc = OCIStmtFetch(_stmt->stmthp, errhp, _params.fetch_params.rows_this_block, OCI_FETCH_NEXT, OCI_DEFAULT);
			};

			if (hb_sender)
				hb_sender->disable();

			break;
		default:
			{
				// whoops unknown
				std::ostringstream msg;
				msg << "m_err=internal error: unknown OCI function call " << _func;
				WRITE_LOG_ENTRY(logfile, LOG_ALERT, "%s", msg.str().c_str());
				CalTransaction::Status s(CAL::TRANS_ERROR, CAL::MOD_OCC, CAL::SYS_ERR_INTERNAL, OCIR_FATAL);
				CalEvent e(CAL::EVENT_TYPE_ERROR, CAL::MOD_OCC, s, msg.str());
				client_session.set_status(CAL::SYSTEM_FAILURE);
				return OCIR_FATAL;
			}
		}

		if ((rc == OCI_ERROR))
		{
			text errbuf[4];
			sb4 errcode;
			OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
					errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
			if ((errcode == 1003) || (errcode == 3114 /*not connected to Oracle*/)
					|| (errcode == 3113 /*eof Oracle conn*/)
					|| (errcode == 3127 /*no new operation allowed until active operation ends*/)
					|| (errcode == 28 /*session was killed*/)    || (errcode == 1012 /*not logged on*/)
					|| (errcode == 25402 /*transaction must roll back*/) || (errcode == 25405 /*transaction status unknown*/)
					|| (errcode == 25408 /*cannot safely replay call*/)  || (errcode == 25425 /*Connection lost during rollback*/)
					|| (errcode == 24343 /*user defined callback error, bind callback err corrupts the bind array for next query */)
					|| (errcode == 1041 /*internal error. hostdef extension doesn"t exist (Oracle bug)*/))
			{
				sql_error(rc, _stmt);
				child_shutdown_flag = 1;
				return OCIR_FATAL;
			}
		}

		if (rc != OCI_STILL_EXECUTING)
			break;

		// build fd_set with fd's for our connection to oracle and to the client
		fd_set fds;
		FD_ZERO(&fds);
		FD_SET(m_data_fd, &fds);
		FD_SET(oracle_fd, &fds);
		int max_fd = (oracle_fd > m_data_fd) ? oracle_fd : m_data_fd;

		// time out just in time for the next ping
		struct timeval timeout;
		if (ping_interval > 0)
		{
			struct timeval tv_now;
			gettimeofday(&tv_now, NULL);
			if (tv_subtract(tv_next_ping, tv_now, timeout) <= 0)
			{
				// whoops real soon now... wait 0.01 seconds
				timeout.tv_sec = 0;
				timeout.tv_usec = 10000;
			}
		}
		else
		{
			// just wait for a minute
			timeout.tv_sec = 60;
			timeout.tv_usec = 0;
		}

		// wait for activity from oracle or the client
		rc = select(max_fd + 1, &fds, NULL, NULL, &timeout);
		if ((rc < 0) && (errno != EINTR))
		{
			// some error with the select. socket is screwed up? just abort.
			WRITE_LOG_ENTRY(logfile, LOG_WARNING, "error on select(), errno = %d", errno);
			child_shutdown_flag = 1;
			return OCIR_FATAL;
		}
		if ((rc > 0) && (FD_ISSET(m_data_fd, &fds)))
		{
			// most likely the client disconnected and we'd get EOF on the input channel.
			// however, it is possible that the client just sent some data to use. But that
			// is a violation of the occ protocol anyway, so in either case just abort the
			// query. if he did hang up, then the next loop in handle_connection() will
			// get EOF and abort; if he sent data, then the next loop will get whatever he
			// sent and deal with that (which is most likely an SSL "goodbye kiss").
			WRITE_LOG_ENTRY(logfile, LOG_INFO, "client disconnected before query completed");
			if (abort_oci_nonblocking())
			{
				// something went really, really wrong
				child_shutdown_flag = 1;
				return OCIR_FATAL;
			}

			// Note that there may be other commands queued up in Worker that will be
			// processed before we get to whatever new activity just happened on the socket.
			// Since we're aborting the query, make sure those commands don't think we have
			// a valid statement to work with.
			cur_stmt = NULL;

			return OCIR_ERROR;
		}

		// either ((rc > 0) && FD_ISSET(oracle_fd)) or (rc == 0) or ((rc == -1) && (errno == EINTR))...

		if (ping_interval > 0)
		{
			// send a ping periodically to prevent the firewall from dumping the connection
			// if this query takes a really long time.
			struct timeval tv_now, tv_delta;
			gettimeofday(&tv_now, NULL);
			if ((tv_subtract(tv_next_ping, tv_now, tv_delta) <= 0) || ((tv_delta.tv_sec == 0) && (tv_delta.tv_usec <= 10000)))
			{
				std::string dummy;
				m_writer->write(OCC_STILL_EXECUTING, dummy);
				tv_add(tv_now, tv_interval, tv_next_ping);
			}
		}
	}

	// check for errors
	if ((rc != OCI_NO_DATA) && (rc != OCI_SUCCESS))
	{
		// batch collect more detailed info
		ub4 num_errs = 0;
		std::vector<int> row_offset;
		if ((_func == OCC_EXECUTE) && (_params.exec_params.iterations > 1))
		{
			int rc1 = OCIAttrGet( (CONST dvoid *) _stmt->stmthp, OCI_HTYPE_STMT,
					(dvoid *) &num_errs, (ub4 *) 0, OCI_ATTR_NUM_DML_ERRORS, errhp);
			if (rc1 != OCI_SUCCESS) num_errs = 0;
			for (ub4 i=0; i<num_errs; i++)
			{
				ub4 row_off = 0;
				sb4 err_code = 0;
				rc1 = OCIParamGet(errhp, OCI_HTYPE_ERROR, errhndl_batch2, (dvoid**) &errhndl_batch, i);
				if (rc1 != SQL_SUCCESS) break;
				rc1 = OCIAttrGet ((dvoid*)errhndl_batch, (ub4) OCI_HTYPE_ERROR, 
						(dvoid *) &row_off, 0, OCI_ATTR_DML_ROW_OFFSET, errhndl_batch2);
				if (rc1 != SQL_SUCCESS) break;
				rc1 = OCIErrorGet ((dvoid*)errhndl_batch, 1, NULL, &err_code, NULL, 0, OCI_HTYPE_ERROR);
				if (rc1 != SQL_SUCCESS) break;
				row_offset.push_back(row_off);
				row_offset.push_back(err_code);
			}
		}

		if ((rc == OCI_SUCCESS_WITH_INFO) && (num_errs==0))
		{
			// this is actually a success with "warning"
			std::string ora_text;
			// get the oracle error
			int ora_error = get_oracle_error(rc, ora_text);
			// the oracle text includes the error number, so no need to print it separately
			WRITE_LOG_ENTRY(logfile, LOG_WARNING, "OCI call returned OCI_SUCCESS_WITH_INFO: %d: [%s]", ora_error, ora_text.c_str());
			rc = OCI_SUCCESS; // just in case some other place is doing != OCI_SUCCESS
		}
		else
		{																														
			sql_error(rc, _stmt, &row_offset);
			if (use_nonblock && set_oci_nonblocking(false))
			{
				// whoops failed to turn off non-blocking mode. this is bad because
				// the rest of the code doesn't handle non-blocking mode. so force
				// immediate hangup and exit.
				child_shutdown_flag = 1;
				return OCIR_FATAL;
			}
			return OCIR_ERROR;
		}
	}

	// restore OCI to blocking
	// pass in the statement handle so we return OCC_ERROR if this fails
	if (use_nonblock && set_oci_nonblocking(false, _stmt))
	{
		// this is a pretty serious condition... the rest of the code isn't
		// written to handle non-blocking mode, and we failed to turn it off.
		// so we better hang up and start over.
		child_shutdown_flag = 1;
		return OCIR_FATAL;
	}

	if (_oci_rc)
	{
		// return OCI return code to caller
		*_oci_rc = rc;
	}

	// all done
	return OCIR_OK;
}



//-----------------------------------------------------------------------------
// send heartbeat to client, useful for long running queries in OCI blocking mode
int OCCChild::send_heartbeat_ping()
{
	std::string dummy;
	return m_writer->write(OCC_STILL_EXECUTING, dummy);
}


//-----------------------------------------------------------------------------

// asynch large payload insert requires a large socket send buffer size 
// This is especially critical for amq that may have payload of the order
// of several kilobytes 
int OCCChild::resize_oracle_fd_buffer()
{
	if ( oracle_fd <= 0 )
		return -1;

	uint send_buf_size = DEFAULT_SEND_BUF_SIZE; 
	std::string cval;
	// This parameter is not expected to be set in cdb file
	// if ever set in cdb file, it should be set to a large enough value
	if (config->get_value("oracle_send_buffer", cval))
		send_buf_size = StringUtil::to_uint(cval);

	int ret = setsockopt( oracle_fd, SOL_SOCKET, SO_SNDBUF,
			(char *)&send_buf_size, sizeof(send_buf_size) );
	if ( ret != 0 ) 
	{
		WRITE_LOG_ENTRY(logfile, LOG_ALERT, "setsockopt SO_SNDBUF failed %d",errno);
		return -1;
	}

	return 0;
}

//-----------------------------------------------------------------------------
int OCCChild::set_session_variables( void )
{
	if( m_enable_session_variables )
	{		
		set_oracle_client_info( m_client_host_name, m_client_exec_name, m_module_name, m_action_name );
	}
	return 0;
}

//-----------------------------------------------------------------------------
int OCCChild::set_oracle_client_info( const std::string & _host_name, const std::string & _exec_name, const std::string & _module_name, const std::string & _action_name )
{
	std::string query;
	std::vector<std::string> bind_names;
	std::vector<std::string> bind_values;
	query = "BEGIN ";
	query.append( "dbms_application_info.set_module( :command_name, :action_name ); " );
	query.append( "END;" );

	std::string query_buf = query;
	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Executing query: %s", query.c_str() );

	// define bindings
	std::string action_name;
	std::string exec_name;
	std::string bind_name;

	unsigned int last_idx = _exec_name.find_last_of( SLASH );
	exec_name.assign(_exec_name.c_str(), last_idx + 1);
	std::ostringstream os;
	os << _host_name << ":" << exec_name;
	action_name = os.str();

	// name and value for module_name
	bind_names.push_back(std::string(":command_name"));

	bind_values.push_back(_module_name);

	// name and value for action_name (combinationation of host:application)
	bind_names.push_back(std::string(":action_name"));

	bind_values.push_back(action_name);

	return execute_query_with_n_binds( query_buf, bind_names, bind_values );
}

//-----------------------------------------------------------------------------

int OCCChild::set_stored_outlines(void)
{
	std::string setting;

	// grab the setting from config
	if ((!config->get_value("use_stored_outlines", setting)) || setting.empty())
		return 0;

	// our query
	const char *stored_outlines_query = "ALTER SESSION SET USE_STORED_OUTLINES = ";
	std::string query(stored_outlines_query, strlen(stored_outlines_query));
	query.append(setting);

	// and run it
	return execute_query(query);
}

//-----------------------------------------------------------------------------

int OCCChild::execute_query(const std::string& query)
{
	int rc;

	// log it
	//WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "running query %s", query.c_str());

	// prepare a statement handle
	OCIStmt *stmthp = NULL;
	rc = OCIHandleAlloc(
			(dvoid *) envhp, (dvoid **) &stmthp,
			OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
	if (rc != OCI_SUCCESS)
	{
		log_oracle_error(rc,"Failed to prepare a statement handle.");
		return -1;
	}

	rc = OCIStmtPrepare(
			stmthp, errhp, (text *) const_cast<char*>(query.c_str()),
			(ub4) query.length(),
			(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT);
	if (rc != OCI_SUCCESS)
	{
		DO_OCI_HANDLE_FREE(stmthp, OCI_HTYPE_STMT, LOG_WARNING);
		log_oracle_error(rc, "Failed to prepare statement.");
		return -1;
	}

	rc = OCIStmtExecute(svchp, stmthp, errhp, 1, 0, NULL, NULL, OCI_DEFAULT);
	if (rc != OCI_SUCCESS)
	{
		DO_OCI_HANDLE_FREE(stmthp, OCI_HTYPE_STMT, LOG_WARNING);
		log_oracle_error(rc, "Failed to execute statement.");
		return -1;
	}

	if (DO_OCI_HANDLE_FREE(stmthp, OCI_HTYPE_STMT, LOG_ALERT) == false)
	{
		log_oracle_error(rc, "Failed to free statement handle.");
		return -1;
	}

	return 0;
}

//-----------------------------------------------------------------------------

void OCCChild::log_oracle_error(int status, const char * str, LogLevelEnum level /* = LOG_ALERT */)
{
	std::string ora_text;
	const std::string *cal_trans_severity, *cal_error_type;

	int ora_error = get_oracle_error(status, ora_text);
	std::string ora_event_name;
	char tmp[16];
	sprintf(tmp, "ORA-%05d", ora_error);
	ora_event_name = tmp;
	std::ostringstream msg;
	msg << "m_err=Oracle Error " << status << ": " << str << " [" << ora_text << "]";
	WRITE_LOG_ENTRY(logfile, level, "%s", msg.str().c_str());

	if (level == LOG_INFO) {
		cal_trans_severity = &(CAL::TRANS_OK);
		cal_error_type = &(CAL::EVENT_TYPE_MESSAGE);
	} else if (level == LOG_ALERT) {
		cal_trans_severity = &(CAL::TRANS_ERROR);
		cal_error_type = &(CAL::EVENT_TYPE_ERROR);
	} else {
		cal_trans_severity = &(CAL::TRANS_WARNING);
		cal_error_type = &(CAL::EVENT_TYPE_WARNING);
	}

	CalTransaction::Status s(*cal_trans_severity, CAL::MOD_OCC, CAL::SYS_ERR_ORACLE, status);
	CalEvent e(*cal_error_type, ora_event_name, s, msg.str());
	client_session.set_status(CAL::INPUT_FAILURE);
	if ( status == OCI_ERROR ) {
		client_session.get_session_transaction()->AddDataToRoot("m_err", ora_event_name);
		client_session.get_session_transaction()->AddDataToRoot("m_errtype","CONNECT");
		client_session.get_session_transaction()->SetStatus(CAL::SYSTEM_FAILURE, CalActivity::CAL_SET_ROOT_STATUS);
	} else  { // CAL Status 2 will be part of PHASE 2 monitoring effort
		//client_session.set_status(CAL::INPUT_FAILURE,  CalActivity::CAL_SET_ROOT_STATUS);
	}
}

//-----------------------------------------------------------------------------

int OCCChild::get_oracle_error(int rc, std::string& buffer)
{
	text errbuf[512];
	sb4 errcode = 0;
	int x;

	switch (rc)
	{
	case OCI_SUCCESS:
		buffer.clear();
		break;
	case OCI_SUCCESS_WITH_INFO:
		buffer = "OCI_SUCCESS_WITH_INFO: ";
		if (errhp == NULL) break;

		// get the oracle error text
		OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
				errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
		if ((errcode == 1003) || (errcode == 3114 /*not connected to Oracle*/)
				|| (errcode == 28 /*session was killed*/)    || (errcode == 1012 /*not logged on*/)
				|| (errcode == 25402 /*transaction must roll back*/) || (errcode == 25405 /*transaction status unknown*/)
				|| (errcode == 25408 /*cannot safely replay call*/)  || (errcode == 25425 /*Connection lost during rollback*/)
				|| (errcode == 1041 /*internal error. hostdef extension doesn"t exist (Oracle bug)*/)
				|| (errcode == 1012 /*not logged in*/))
		{
			child_shutdown_flag = 1;
		}

		buffer += (char *) errbuf;

		// remove the annoying newline at the end
		x = buffer.length();
		while ((x > 0) && std::isspace(buffer[x - 1]))
			x--;
		buffer.resize(x);
		break;
	case OCI_NEED_DATA:
		buffer = "Error - OCI_NEED_DATA";
		break;
	case OCI_NO_DATA:
		buffer = "Error - OCI_NO_DATA";
		break;
	case OCI_ERROR:
		buffer = "Error - OCI_ERROR"; 
		if (errhp == NULL) break;

		// get the oracle error text
		OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
				errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
		if ((errcode == 1003) || (errcode == 3114 /*not connected to Oracle*/)
				|| (errcode == 28 /*session was killed*/)    || (errcode == 1012 /*not logged on*/)
				|| (errcode == 25402 /*transaction must roll back*/) || (errcode == 25405 /*transaction status unknown*/)
				|| (errcode == 25408 /*cannot safely replay call*/)  || (errcode == 25425 /*Connection lost during rollback*/)
				|| (errcode == 1041 /*internal error. hostdef extension doesn"t exist (Oracle bug)*/)
				|| (errcode == 1012 /*not logged in*/))
		{
			child_shutdown_flag = 1;
		}
		buffer = (char *) errbuf;

		// remove the annoying newline at the end
		x = buffer.length();
		while ((x > 0) && std::isspace(buffer[x - 1]))
			x--;
		buffer.resize(x);
		break;
	case OCI_INVALID_HANDLE:
		buffer = "Error - OCI_INVALID_HANDLE";
		break;
	case OCI_STILL_EXECUTING:
		buffer = "Error - OCI_STILL_EXECUTING";
		break;
	case OCI_CONTINUE:
		buffer = "Error - OCI_CONTINUE";
		break;
	default:
		buffer = "Error - unknown";
		break;
	}

	return errcode;
}

//-----------------------------------------------------------------------------

int OCCChild::rollback(const std::string &xid)
{
	int rc = 0;
	// bool enable_cal = CalClient::is_enabled();

	if (!xid.empty())
	{
		if (set_xid(xid))
		{
			occ_error("failed to set the XID for rollback.");
			return -1;
		}

		m_in_global_txn = m_phase1_done = m_has_real_dml = in_trans = true;
		m_trans_role = PARTICIPANT;
	}

	if (!in_trans)
	{
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "doing ROLLBACK (skipped)");
		m_client_session.end_db_txn();
		return 0;
	}

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "doing ROLLBACK");

	// scoped_ptr<CalEvent> rollback_event;
	// if (enable_cal)
	// {
	//	rollback_event.reset(new CalEvent(CAL_EVENT_ROLLBACK));
	// }

	if (!xid.empty())
	{
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "xid=%s", xid.c_str());
		// if (rollback_event)
		// {
		//	rollback_event->SetName(CAL_NAME_RECOMAN);
		//	rollback_event->AddData(CAL_DATA_XID, xid);
		// }
	}
	// else
	// {
	//	if (rollback_event)
	//	{
	//		rollback_event->SetName((m_in_global_txn && m_has_real_dml) ? CAL_EVENT_DISTRIBUTED : CAL_EVENT_LOCAL);
	//	}
	// }

	rc = OCITransRollback(svchp, errhp, (ub4) OCI_DEFAULT);
	if(rc!=OCI_SUCCESS) {
		int errcode = sql_error(rc, NULL);

		// Did someone completed the transaction heuristically?
		bool heuristically_completed = 
			(!xid.empty()) &&
			( (errcode == ORA_24764_TXN_HAS_BEEN_HEURISTICALLY_COMMITTED) ||
			  (errcode == ORA_24765_TXN_HAS_BEEN_HEURISTICALLY_ROLLED_BACK) );

		if (heuristically_completed)
		{
			// if (rollback_event)
			// {
			//	std::string status_msg;
			//	status_msg.copy_formatted("No Work: ORA-%05d", errcode);
			//	rollback_event->AddData(CAL_DATA_RC,status_msg);
			//	rollback_event->SetStatus(CalTransaction::Status(CAL::TRANS_OK, CAL::MOD_OCC, CAL::SYS_ERR_ORACLE, errcode));
			// }
			trans_forget();
		}
		else
		{
			// if (rollback_event)
			// {
			//	std::string status_msg;
			//	status_msg.copy_formatted("Failed: ORA-%05d", errcode);
			//	rollback_event->AddData(CAL_DATA_RC,status_msg);
			//	rollback_event->SetStatus(CalTransaction::Status(CAL::TRANS_ERROR, CAL::MOD_OCC, CAL::SYS_ERR_ORACLE, errcode));
			// }
			return -1;
		}
	}
	// else
	// {
	//	if (rollback_event)
	//		rollback_event->SetStatus(CAL::TRANS_OK);
	// }

	// rollback ends the transaction
	in_trans = false;
	rc = clear_2pc_state();
	if (rc == 0)
		m_client_session.end_db_txn();

	return rc;
}

//-----------------------------------------------------------------------------

int OCCChild::commit(const std::string &xid)
{
	int rc = 0;
	bool enable_cal = CalClient::is_enabled();

	if (!xid.empty())
	{
		// This is to handle COMMIT of a specific global txn.
		// We automatically thrust this session into a state
		// with a global txn after the prepare phase.
		if (set_xid(xid))
		{
			occ_error("failed to set the XID for commit.");
			return -1;
		}

		m_in_global_txn = m_phase1_done = m_has_real_dml = true;
		m_trans_role = PARTICIPANT;
	}
	else if (!in_trans)
	{
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "doing COMMIT (skipped)");
		m_client_session.end_db_txn();
		return 0;
	}

	// This assumes that m_in_global_txn is set only for participants.
	// For the commit point site and the transaction there is completely loco.
	if (m_in_global_txn && m_has_real_dml && !m_phase1_done && (m_trans_role == PARTICIPANT))
	{
		occ_error("Cannot COMMIT until TransPrepare is done.");
		return -1;
	}

	ub4 flags = (m_in_global_txn && m_has_real_dml) ? OCI_TRANS_TWOPHASE : OCI_DEFAULT;

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "doing COMMIT as %s", (flags == OCI_TRANS_TWOPHASE) ? "2PC" : "DEFAULT");

	std::unique_ptr<CalEvent> commit_event;
	if (enable_cal)
	{
		commit_event.reset(new CalEvent(CAL_EVENT_COMMIT));
	}

	if (!xid.empty())
	{
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "xid=%s", xid.c_str());
		if (commit_event)
		{
			commit_event->SetName(CAL_NAME_RECOMAN);
			commit_event->AddData(CAL_DATA_XID, xid);
		}
	}
	else
	{
		if (commit_event)
			commit_event->SetName((flags == OCI_TRANS_TWOPHASE) ? CAL_EVENT_DISTRIBUTED : CAL_EVENT_LOCAL);
	}

	rc = OCITransCommit(svchp, errhp, flags);
	if(rc!=OCI_SUCCESS) {
		int errcode = sql_error(rc, NULL);

		// Did someone completed the transaction heuristically?
		bool heuristically_completed = 
			(!xid.empty()) &&
			( (errcode == ORA_24764_TXN_HAS_BEEN_HEURISTICALLY_COMMITTED) ||
			  (errcode == ORA_24765_TXN_HAS_BEEN_HEURISTICALLY_ROLLED_BACK) );

		if (heuristically_completed)
		{
			if (commit_event)
			{
				char status_msg[64];
				sprintf(status_msg, "No Work: ORA-%05d", errcode);
				commit_event->AddData(CAL_DATA_RC, status_msg);
				commit_event->SetStatus(CalTransaction::Status(CAL::TRANS_OK, CAL::MOD_OCC, CAL::SYS_ERR_ORACLE, errcode));
			}
			trans_forget();
		}
		else
		{
			if (commit_event)
			{
				char status_msg[64];
				sprintf(status_msg, "Failed: ORA-%05d", errcode);
				commit_event->AddData(CAL_DATA_RC, status_msg);
				commit_event->SetStatus(CalTransaction::Status(CAL::TRANS_ERROR, CAL::MOD_OCC, CAL::SYS_ERR_ORACLE, errcode));
			}
			return -1;
		}
	}
	else
	{
		if (commit_event)
			commit_event->SetStatus(CAL::TRANS_OK);
	}

	// commit ends the transaction
	in_trans = false;
	rc = clear_2pc_state();
	if (rc == 0)
		m_client_session.end_db_txn();

	return rc;
}

//-----------------------------------------------------------------------------

static inline bool do_log_sql_text(int _ora_error_num)
{
	// for some oracle errors, we don't bother logging the sql text after the
	// error, mostly because these errors indicate that the entire database is
	// hosed and we're gonna get the errors for every statement.
	// also some errors (unique constraint, cannot insert NULL) have sufficient
	// info in the error text that the sql text is not necessary
	switch (_ora_error_num)
	{
	case ORA_00001_UNIQUE_CONSTRAINT_VIOLATED:
		//case ORA_00018_MAX_SESSIONS_EXCEEDED:
		//case ORA_00020_MAX_PROCESSES_EXCEEDED:
	case ORA_00028_SESSION_KILLED:
	case ORA_00054_RESOURCE_BUSY_AND_NOWAIT_SPECIFIED:
	case ORA_00055_MAX_DML_LOCKS_EXCEEDED:
	case ORA_01012_NOT_LOGGED_ON:
	case ORA_01033_INITIALIZATION_OR_SHUTDOWN_IN_PROGRESS:
	case ORA_01034_ORACLE_NOT_AVAILABLE:
	case ORA_01400_CANNOT_INSERT_NULL:
	case ORA_03113_END_OF_FILE_ON_COMMUNICATION_CHANNEL:
	case ORA_03114_NOT_CONNECTED_TO_ORACLE:
	case ORA_04031_UNABLE_TO_ALLOCATE_SHARED_MEMORY:
	case ORA_27101_SHARED_MEMORY_REALM_DOES_NOT_EXIST:
		return false;
	}

	// log everything else
	return true;
}

//-----------------------------------------------------------------------------
static long long time_us()
{
    struct timeval now;
    gettimeofday(&now, NULL);
    return 1000000*(long long)now.tv_sec + now.tv_usec;
}

int OCCChild::sql_error(int rc, const StmtCacheEntry *_stmt, const std::vector<int>* row_offset)
{
	std::string ora_text;

	// get the oracle error
	int ora_error = get_oracle_error(rc, ora_text);

	//ignore errors
	std::ostringstream os;
	os << ora_error << " ";
	if (row_offset!=NULL) {
		if (row_offset->size() > 0) {
			os << row_offset->size() << " ";
			for (int i=0; i<row_offset->size(); ++i)
				os << (*row_offset)[i] << " ";
		}
	}
	os << ora_text;

	if (ora_error == 1013) {
		// no EOR for this, because it will be sent by the recover() routine
		m_writer->add(OCC_SQL_ERROR, os.str());
	} else {
		eor(is_in_transaction() ? EORMessage::IN_TRANSACTION : EORMessage::FREE, OCC_SQL_ERROR, os.str());
	}
	m_writer->write();

	// the oracle text includes the error number, so no need to print it separately
	long long beginAppLog = time_us();
	WRITE_LOG_ENTRY(logfile, LOG_WARNING, "Oracle error %d: [%s]", ora_error, ora_text.c_str());
	long long endAppLog = time_us();

	// if the log level is less than debug, that means the statement wasn't logged,
	// so it'll probably be useful to put that in the logs
	if ((_stmt != NULL) && (logfile->get_log_level().level < LOG_DEBUG) && do_log_sql_text(ora_error))
		WRITE_LOG_ENTRY(logfile, LOG_WARNING, "sql text was: %s", _stmt->text.c_str());
	long long endStmtAppLog = time_us();

	char tmp[128];
	sprintf(tmp, "ORA-%05d", ora_error);
	CalEvent c(CAL::EVENT_TYPE_ERROR, tmp, CalTransaction::Status(CAL::TRANS_ERROR, CAL::MOD_OCC, CAL::SYS_ERR_SQL, rc));
	c.AddData("m_err", ora_text);
	sprintf(tmp, "%lli,%lli",endAppLog-beginAppLog, endStmtAppLog-endAppLog);
	c.AddData("appLogUs", tmp);
	c.Completed();
	// we just hit an error, so unset cur_stmt -- that way future operations won't
	// think we have a valid, prepared statement
	cur_stmt = NULL;

	return ora_error;
}

//-----------------------------------------------------------------------------

void OCCChild::free_stmt(StmtCacheEntry *_entry)
{
	// sanity check for NULL entry
	if (_entry == NULL)
		return;

	// zap the stmthp
	DO_OCI_HANDLE_FREE(_entry->stmthp, OCI_HTYPE_STMT, LOG_WARNING);

	// zap the defines
	delete[] _entry->defines;
	_entry->defines = NULL;

	// zap the column list
	delete _entry->columns;
	_entry->columns = NULL;

	// delete it
	// except for the non-caching case, where we just clear it out
	// because it will be used again
	if (_entry == &one_stmt)
		_entry->clear();
	else
		delete _entry;
}

void OCCChild::cache_expire(bool _force_expire)
{
	// nothing to do if not caching
	if (!enable_cache)
		return;

	// what time is it?
	time_t now = time(NULL);

	// if we just recently expired the cache, and we're not forcing expiration
	// the skip the whole process
	if (!_force_expire && (now < next_cache_expire_time))
		return;
	next_cache_expire_time = now + cache_expire_frequency;

	// look for expired statements
	int old_cache_size = cache_size;
	StmtCacheEntry **p = NULL;
	for (int i = 0; i < old_cache_size; i++)
	{
		// get the entry
		StmtCacheEntry *entry = stmt_cache[i];

		// test if expired
		if (now - entry->when >= max_statement_age)
		{
			free_stmt(entry);
			stmt_cache[i] = NULL;
			if (p == NULL)
				p = &stmt_cache[i];
			cache_size--;
			cache_expires++;
		}
		else if (p != NULL)
		{
			// slide it down
			*p++ = stmt_cache[i];
			stmt_cache[i] = NULL;
		}
	}

	if (old_cache_size != cache_size)
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "dumped %d statements", old_cache_size - cache_size);
}

void OCCChild::cache_insert(StmtCacheEntry *_new_entry)
{
	// nothing to do if not caching
	if (!enable_cache)
		return;

	if (cache_size == max_cache_size)
	{
		time_t oldest = 0;
		time_t now = time(NULL);
		int old_index = -1;
		unsigned long long least_exec = 0;
		for (int i = 0; i < cache_size; i++)
		{
			// get this entry
			const StmtCacheEntry *entry = stmt_cache[i];

			// if it's too old, use it
			if (now - entry->when > max_statement_age)
			{
				old_index = i;
				break;
			}

			// find the oldest entry
			if ((old_index < 0) || (entry->when < oldest) || ((entry->when == oldest) && (entry->num_exec < least_exec)))
			{
				old_index = i;
				oldest = entry->when;
				least_exec = entry->num_exec;
			}
		}

		// free it and slide everything down
		free_stmt(stmt_cache[old_index]);
		memmove(stmt_cache + old_index, stmt_cache + old_index + 1, (cache_size - old_index - 1) * sizeof(StmtCacheEntry *));
		cache_size--;
		cache_dumps++;
	}

	// do a binary search to find where to put it
	int lo = 0, hi = cache_size - 1;
	while (lo <= hi)
	{
		int mid = (lo + hi) / 2;
		int cmp = strcmp(stmt_cache[mid]->text.c_str(), _new_entry->text.c_str());
		if (cmp == 0)
		{
			if (stmt_cache[mid]->version == _new_entry->version)
			{
				WRITE_LOG_ENTRY(logfile, LOG_ALERT, "internal error -- attempt to insert duplicate statement");
				return;
			}
			if (stmt_cache[mid]->version < _new_entry->version)
				cmp = -1;
			else
				cmp = 1;
		}
		if (cmp < 0)
			hi = mid - 1;
		else
			lo = mid + 1;
	}

	// now insert at position "lo"
	if (lo < cache_size)
		memmove(stmt_cache + lo + 1, stmt_cache + lo, (cache_size - lo) * sizeof(StmtCacheEntry *));
	stmt_cache[lo] = _new_entry;
	cache_size++;

	// track high-water mark
	if (cache_size > cache_size_peak)
		cache_size_peak = cache_size;
}

StmtCacheEntry* OCCChild::cache_find(const std::string& _query, occ::ApiVersion _version)
{
	StmtCacheEntry* stmt = NULL;
	int lo = 0, hi = cache_size - 1;
	while (lo <= hi)
	{
		int mid = (lo + hi) / 2;
		int cmp = strcmp(stmt_cache[mid]->text.c_str(), _query.c_str());
		if (cmp == 0)
		{
			// see if the statement is using the datetime and version is mismatch, in which case we need extra tests
			// regular, "good citizens" would evaluate to false
			if ((stmt_cache[mid]->has_datetime) && (stmt_cache[mid]->version != _version))
			{
				if (stmt_cache[mid]->version < _version)
					cmp = -1;
				else
					cmp = 1;
			}
			else
			{
				stmt_cache[mid]->version = _version;
				stmt = stmt_cache[mid];
				break;
			}
		}
		if (cmp < 0)
			hi = mid - 1;
		else
			lo = mid + 1;
	}
	return stmt;
}

StmtCacheEntry* OCCChild::get_cur_stmt(void)
{
	// make sure the current statement is valid
	if ((cur_stmt == NULL) || (cur_stmt->stmthp == NULL))
	{
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "statement requested when none was prepared (possible aftermath of an earlier error)");
		return NULL;
	}

	// return it
	return cur_stmt;
}

//-----------------------------------------------------------------------------

int OCCChild::prepare(const std::string& _statement, occ::ApiVersion _version)
{
	int rc;
	//
	// For the 1st release we are limiting CAL in OCC to this single isolated fake transaction 
	// (and only when CAL is on) - in the future we should wrap CAL calls from Worker
	// and use CAL SQL statement caching. Note also that there is no parent transaction here.

	// new fetch
	in_trans = true;
	new_fetch = true;
	cur_results = NULL;
	results_valid = false;

	// clean up newlines in query
	std::string statement(_statement);
	if (enable_query_replace_nl)
	{
		for (int i = 0; i < statement.length(); i++)
		{
			if (statement[i] == '\n')
			{
				statement[i] = ' ';
			}
		}
	}

	// already cached? do a binary search.
	// NOTE: if cache not enabled, this will just short-circuit and find nothing
	cur_stmt = NULL;
	int lo = 0, hi = cache_size - 1;
	while (lo <= hi)
	{
		int mid = (lo + hi) / 2;
		int cmp = strcmp(stmt_cache[mid]->text.c_str(), statement.c_str());
		if (cmp == 0)
		{
			// see if the statement is using the datetime and version is mismatch, in which case we need extra tests
			// regular, "good citizens" would evaluate to false
			if ((stmt_cache[mid]->has_datetime) && (stmt_cache[mid]->version != _version))
			{
				if (stmt_cache[mid]->version < _version)
					cmp = -1;
				else
					cmp = 1;
			}
			else
			{
				stmt_cache[mid]->version = _version;
				cur_stmt = stmt_cache[mid];
				break;
			}
		}
		if (cmp < 0)
			hi = mid - 1;
		else
			lo = mid + 1;
	}

	//do some cleanup first
	bind_array->clear();
	out_bind_array->clear();

	if (cur_stmt != NULL)
	{
		// statement is already prepared! nothing to do.
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "cached v%d statement: %s", cur_stmt->version, statement.c_str());
		cache_hits++;
	}
	else
	{
		StmtCacheEntry *entry;

		if (!enable_cache)
		{
			// use the global statement entry
			// but clear it out first to make sure it's fresh
			free_stmt(&one_stmt);
			entry = &one_stmt;
		}
		else
		{
			// allocate a new entry
			entry = new StmtCacheEntry;
		}
		entry->version = _version;

		// create a statement handle
		rc = OCIHandleAlloc((dvoid *) envhp, (dvoid **) &entry->stmthp, OCI_HTYPE_STMT, (size_t) 0, (dvoid **) NULL);
		if (rc != OCI_SUCCESS)
		{
			sql_error(rc, NULL);
			return -1;
		}

		// save the query text
		entry->text = statement;

		// log it
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "preparing statement: %s", statement.c_str());
		cache_misses++;

		// prepare the new statement
		rc = OCIStmtPrepare(
				entry->stmthp,
				errhp,
				(text *) const_cast<char *> (statement.c_str()),
				(ub4) statement.length(),
				(ub4) OCI_NTV_SYNTAX,
				(ub4) OCI_DEFAULT);
		if (rc != OCI_SUCCESS)
		{
			sql_error(rc, entry);
			free_stmt(entry);
			return -1;
		}

		// determine the statement type (we'll need it later)
		rc = OCIAttrGet(
				(CONST dvoid *) entry->stmthp,
				OCI_HTYPE_STMT,
				(dvoid *) &entry->type,
				(ub4 *) NULL,
				OCI_ATTR_STMT_TYPE,
				errhp);
		if (rc != OCI_SUCCESS)
		{
			sql_error(rc, entry);
			free_stmt(entry);
			return -1;
		}

		//		// Delineate between SELECT and SELECT ... FOR UPDATE
		//		if ((entry->type == SELECT_STMT) &&
		//			statement.contains(" FOR UPDATE"))
		//		{
		//			entry->type == SELECT_FOR_UPDATE_STMT;
		//		}

		if ((entry->type == UNKNOWN_STMT) && (strncasecmp(statement.c_str(), "CALL ", 5) == 0))
		{
			entry->type = CALL_STMT;  // Need to get iteration count set.
		}

		// remember the current statement
		cache_insert(entry);
		cur_stmt = entry;
	}

	// in any event, update some stats for the statement
	cur_stmt->when = time(NULL);
	++cur_stmt->num_exec;

	// expire the cache
	cache_expire(false);

#if 0
	// WE ARE NOT DOING THIS BECAUSE THIS INCREASES THE NUMBER
	// OF ROUNDTRIPS BETWEEN OCCCLIENT AND SERVER
	// Return the Statement type back to the client
	return m_writer->write(OCC_VALUE, std::string().fmt_uint(cur_stmt->type));
#endif

	return 0;
}

//-----------------------------------------------------------------------------

/**
 * @brief prepare a special statement.
 *
 * NOTE: The special statement CANNOT BE a DML
 * @param _statement_id These special queries are identified by an ID in the config file.
 */
int OCCChild::prepare_special(uint _statement_id)
{
	// Get a singleton results object from the cache
	OCCCachedResults * results = OCCCachedResults::get_cache_entry(_statement_id, config, logfile);

	// This shouldn't happen. If it does, this is bad, because we don't have the query text!
	if (results == NULL)
	{
		std::ostringstream error;
		error << "Can't initialize cache entry for special query " << _statement_id;
		occ_error(error.str().c_str());
		return -1;
	}

	// Is the cache valid?	
	if (!results->valid())
	{
		// Prepare the query as usual (this call will reset cur_results and results_valid)
		int rc = prepare(results->get_query(), occ::V1);
		if (rc)
			return rc;

		// if the cache is disabled for this query, stop here, and the query will behave normally
		if (!results->enabled())
			return 0;

		// sanity check - verify that the query is a SELECT
		StmtCacheEntry *stmt = get_cur_stmt();
		if (stmt == NULL)
			return -1; 

		if ((stmt->type != SELECT_STMT) &&
				(stmt->type != SELECT_FOR_UPDATE_STMT))
		{
			occ_error("preparing statement (cached): not a SELECT, bailing");
			return -1;
		}

		// dump the results
		results->expire();
		results_valid = false;

		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "preparing statement (cached): results will be cached");
	}
	else
	{
		results_valid = true;

		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "preparing statement (cached): %s", results->get_query().c_str());
	}

	// This is for future execute/fetch
	cur_results = results;

	return 0;
}

//-----------------------------------------------------------------------------

void OCCChild::occ_error(const char * str)
{
	if (CalClient::is_enabled())
	{
		CalTransaction::Status s(CAL::TRANS_ERROR, CAL::MOD_OCC, CAL::SYS_ERR_INTERNAL, -1);
		CalEvent ce(CAL::EVENT_TYPE_ERROR, CAL::MOD_OCC, s);
		ce.AddData("m_err", str);
		client_session.set_status(CAL::INPUT_FAILURE);
	}
	//ignore errors?
	std::string buff_str(str, strlen(str));
	eor(is_in_transaction() ? EORMessage::IN_TRANSACTION : EORMessage::FREE, OCC_ERROR, buff_str);
	m_writer->write();
	WRITE_LOG_ENTRY(logfile, LOG_WARNING, "%s", str);

	// we just hit an error so unset cur_stmt -- that way future operations won't
	// think we have a valid, prepared statement
	cur_stmt = NULL;
}

/**
 * This is the callback function for feeding OCIBindDynamic data for
 * a regular (in-bound) placeholder (bind) variable.
 *
 * @param ictxp The context of the call. This is set to a pointer to the OCCBind instance in OCIBindDynamic. [IN/OUT]
 * @param bindp Pointer to the Bind handle. [IN/OUT]
 * @param iter The iteration counter for statement exection (0-based). [IN]
 * @param index The piecewise data chunk index (0-based). [IN]
 * @param bufpp A handle to the data chunk. [OUT]
 * @param alenp Pointer to a unsigned int for telling Oracle the length of the data chunk. [OUT]
 * @param piecep Pointer for returning which piecewise mode we are in. We ALWAYS use OCI_ONE_PIECE. [OUT]
 * @param indpp Handle to the null indicator storage. [IN/OUT]
 * @return OCI_CONTINUE - OK, -1 - Some failure happened.
 */
sb4 OCCChild::ph_cb_in(dvoid *ictxp,
		OCIBind *bindp,
		ub4 iter,
		ub4 index,
		dvoid **bufpp,
		ub4 *alenp,
		ub1 *piecep,
		dvoid **indpp)
{
	// Retrieve context
	OCCBind *binder = (OCCBind *)ictxp;

	if (binder->is_inout())
	{
		// We are dealing with the IN value for an OUT var
		*bufpp = NULL;
		*alenp = 0;
		binder->null_indicators[0] = -1;
	}
	else if (binder->lob)
	{
		*bufpp = binder->lob;
		*alenp = 0;
		binder->null_indicators[0] = 0;
	}
	else if (!(binder->value.empty()))
	{
		*bufpp = (dvoid *)(const_cast<char *>(binder->value.c_str()));
		*alenp = (binder->value.length() + 1);
		binder->null_indicators[0] = 0;
	}
	else
	{
		*bufpp = (dvoid *)(const_cast<char *>(binder->value.c_str()));
		*alenp = 0;
		binder->null_indicators[0] = -1;
	}
	*indpp = binder->null_indicators;
	*piecep = OCI_ONE_PIECE;	// Do not support piecewise DML

	if (index > 0 || iter > 0)
	{
		// Cannot handle piecewise DML
		occ_error("Piecewise DML is not supported.");
		return -1;
	}

	return OCI_CONTINUE;
}

/**
 * This is the callback function for OCIBindDynamic to allocate storage for data 
 * returned for an out-bound placeholder (bind) variable.
 *
 * NOTE: It looks like there's no way to accertain the returned data size before or during this callback
 * function. For this implementation, we are only interested in supporting returning the sequence value
 * used in an INSERT so we can merge the selecting from the sequence and the row insertion into one
 * statement. Therefore, we are fixing the buffer size to MAX_OUT_BIND_VAR_SIZE.
 *
 * @param ictxp The context of the call. This is set to a pointer to the OCCBindInOut instance in OCIBindDynamic. [IN/OUT]
 * @param bindp Pointer to the Bind handle. [IN/OUT]
 * @param iter The iteration counter for statement exection (0-based). [IN]
 * @param index The row index of data chunk index (0-based). [IN]
 * @param bufpp A handle to the allocated data buffer. [OUT]
 * @param alenp A handle to a unsigned int for Oracle to store the length of the returned data. [OUT]
 * @param piecep Pointer for returning which piecewise mode we are in. We ALWAYS use OCI_ONE_PIECE. [OUT]
 * @param indpp Handle to the null indicator storage. [OUT]
 * @param rcodepp Handle for Oracle to return result code. [OUT]
 * @return OCI_CONTINUE - OK, -1 - Some failure happened.
 */
sb4 OCCChild::ph_cb_out(dvoid *octxp,
		OCIBind *bindp,
		ub4 iter,
		ub4 index,
		dvoid **bufpp,
		ub4 **alenpp,
		ub1 *piecep,
		dvoid **indpp,
		ub2 **rcodepp)
{
	int rc = 0;
	// Retrieve context
	OCCBindInOut *binder = (OCCBindInOut *)octxp;

	if (iter > 0)
	{
		occ_error("OCC does not support multiple iteration.");
		return -1;
	}

	if (index == 0)
	{
		if ((binder->rows > 0) && (binder->buffer != NULL))
			binder->cleanup();

		// First row returned
		rc = OCIAttrGet((CONST dvoid *) binder->bind,
				OCI_HTYPE_BIND,
				(dvoid *) &binder->rows,
				(ub4 *) NULL,
				OCI_ATTR_ROWS_RETURNED, 
				binder->errhp);
		if (rc != OCI_SUCCESS)
		{
			sql_error(rc, NULL);
			return -1;
		}

		// Allocate buffer for storage (if necessary)
		// Cannot use data_buf because we don't know what's the starting offset 
		// in data_buf
		// unsigned int size = binder->rows * (binder->maxlen + 1/* account for terminating NUL */);

		// here binder->rows doesn't seem to give us row count info
		unsigned int _rows = binder->rows + 1;
		binder->buffer = new char *[_rows];  // Syntax Change for Porting to RHEL4.0
		binder->lengths = new ub4[_rows];
		binder->rcs = new ub2[_rows];
		binder->indicators = new sb2[_rows];
		binder->indicators[index] = -1; // leave uninitialized resulting in random data later
	}
	else if (index >= MAX_DYNAMIC_BIND_ROWS)
	{
		occ_error("RETURNING too many rows");
		return -1;
	}

	binder->buffer[index] = new char[binder->maxlen + 1];
	*bufpp = binder->buffer[index];
	binder->lengths[index] = binder->maxlen;
	*alenpp = &(binder->lengths[index]);
	*piecep = OCI_ONE_PIECE;
	*indpp = &(binder->indicators[index]);
	*rcodepp = &(binder->rcs[index]);

	return OCI_CONTINUE;
}


//-----------------------------------------------------------------------------

/**
 * @brief Handle the actual OCIBind* calls for both bind() and bind_out().
 *
 * @param binder Reference to the OCCBind/OCCBindInOut object
 * @param at_exec Whether we are operating in OCI_DATA_AT_EXEC mode. This should be true for bind-out()
 * @return 0 - Success, -1 - Failure
 */
int OCCChild::internal_bind(OCCBind &binder, bool at_exec)
{
	int rc = 0;
	StmtCacheEntry *stmt = get_cur_stmt();
	if (stmt == NULL)
		return -1;	// Should never happen

	// this is to guard against a bug in OCIBindByName where if the bind name is
	// longer than 30 chars (plus the colon), it segfaults rather than returning
	// a meaningful error
	const int MAX_BIND_NAME_LEN = 31;  // 30 + 1 for the colon
	if (binder.name.length() > MAX_BIND_NAME_LEN)
	{
		// whoops bind name is too long
		WRITE_LOG_ENTRY(logfile, LOG_WARNING, "bind name exceeds maximum length: '%s'", binder.name.c_str());

		// report error to the client
		std::ostringstream os;
		os << "bind name '" << binder.name << "' exceeds maximum length";
		m_writer->write(OCC_ERROR, os.str());

		// wipe out cur_stmt so future commands can't operate on this bad statement handle
		cur_stmt = NULL;
		return -1;
	}

	switch (binder.type)
	{
	case OCC_TYPE_BLOB:
	case OCC_TYPE_CLOB:

		if (binder.array_row_num>1) {
			occ_error("OCC_TYPE_BLOB/OCC_TYPE_CLOB do not support array bind");
			return -1;
		}
		rc = OCIDescriptorAlloc((dvoid*)envhp, (dvoid**)&binder.lob, OCI_DTYPE_LOB, 0, 0);
		if(rc!=OCI_SUCCESS) {
			sql_error(rc, stmt);
			return -1;
		}
		rc = OCIBindByName(
				stmt->stmthp,
				(OCIBind **) &binder.bind,
				errhp,
				(text *) const_cast<char *>(binder.name.c_str()),
				binder.name.length(),
				&binder.lob,
				-1,
				(binder.type == OCC_TYPE_BLOB) ? SQLT_BLOB : SQLT_CLOB,
				(dvoid *) binder.null_indicators,
				(ub2 *) NULL,
				(ub2 *) NULL,
				(ub4) 0,
				(ub4 *) NULL,
				(ub4 )(at_exec ? OCI_DATA_AT_EXEC : OCI_DEFAULT));
		if(rc!=OCI_SUCCESS) {
			sql_error(rc, stmt);
			return -1;
		}
		break;

	case OCC_TYPE_BLOB_SINGLE_ROUND:
	case OCC_TYPE_CLOB_SINGLE_ROUND:
	case OCC_TYPE_RAW:
		rc = OCIBindByName(
				stmt->stmthp,
				(OCIBind **) &binder.bind,
				errhp,
				(text *) const_cast<char *>(binder.name.c_str()),
				binder.name.length(),
				(dvoid *)(binder.is_inout() ? NULL : const_cast<char *>(binder.value.c_str())),
				((binder.is_inout()) ? static_cast<OCCBindInOut &>(binder).maxlen : binder.array_max_data_size),
				(binder.type == OCC_TYPE_CLOB_SINGLE_ROUND) ? SQLT_CHR : SQLT_BIN,
				(dvoid *) binder.null_indicators,
				(binder.array_row_num<=1)? (ub2 *) NULL : binder.bind_data_size, 
				(ub2 *) NULL,
				(ub4) 0,
				(ub4 *) NULL,
				(ub4)(at_exec ? OCI_DATA_AT_EXEC : OCI_DEFAULT));
		if(rc!=OCI_SUCCESS) {
			sql_error(rc, stmt);
			return -1;
		}
		break;

	case OCC_TYPE_TIMESTAMP:
	case OCC_TYPE_TIMESTAMP_TZ:
		{
			rc = OCIArrayDescriptorAlloc((dvoid*)envhp, (dvoid**)binder.date_time, (binder.type == OCC_TYPE_TIMESTAMP) ? OCI_DTYPE_TIMESTAMP : OCI_DTYPE_TIMESTAMP_TZ, binder.array_row_num, 0, 0);
			if(rc!=OCI_SUCCESS) {
				sql_error(rc, stmt);
				return -1;
			}
#define DATE_TIME_FORMAT	"DD-MM-YYYY HH24:MI:SS.FF3"
#define DATE_TIME_FORMAT_TZ	"DD-MM-YYYY HH24:MI:SS.FF3 TZR"

			for (unsigned int i=0; i<binder.array_row_num; ++i)
			{
				if (binder.bind_data_size[i] > 0) {
					OCIDateTimeFromText ( (dvoid*)envhp,
							errhp,
							(OraText*)const_cast<char *>(binder.value.c_str() + i*(binder.array_max_data_size+1)),
							(binder.array_row_num<=1)? binder.array_max_data_size : binder.bind_data_size[i],
							(binder.type == OCC_TYPE_TIMESTAMP) ? (OraText*)DATE_TIME_FORMAT : (OraText*)DATE_TIME_FORMAT_TZ,
							(binder.type == OCC_TYPE_TIMESTAMP) ? (sizeof(DATE_TIME_FORMAT) - 1) : (sizeof(DATE_TIME_FORMAT_TZ) - 1),
							NULL,
							0,
							binder.date_time[i]
							);
					if(rc!=OCI_SUCCESS) {
						sql_error(rc, stmt);
						return -1;
					}
					binder.null_indicators[i] = 0; // not NULL
				} else {
					binder.null_indicators[i] = -1; // -1 means NULL
				}
			}

			rc = OCIBindByName(
					stmt->stmthp,
					(OCIBind **) &binder.bind,
					errhp,
					(text *) const_cast<char *>(binder.name.c_str()),
					binder.name.length(),
					&binder.date_time[0],
					-1,
					(binder.type == OCC_TYPE_TIMESTAMP) ? SQLT_TIMESTAMP : SQLT_TIMESTAMP_TZ,
					(dvoid *) binder.null_indicators,
					(ub2 *) NULL,
					(ub2 *) NULL,
					(ub4) 0,
					(ub4 *) NULL,
					(ub4 )(at_exec ? OCI_DATA_AT_EXEC : OCI_DEFAULT));
			if(rc!=OCI_SUCCESS) {
				sql_error(rc, stmt);
				return -1;
			}
			break;
		}

	case OCC_TYPE_STRING:
		{
			// only do for bind in and when appending, append with a leading ','
			if(!binder.is_inout()) 
			{
				int data_len = m_bind_data.length() == 0 ? binder.value.length() : m_bind_data.length() + binder.value.length() + 1;
				if (m_bind_data.length() > 0 && m_bind_data.length() < MAX_VSESSION_BIND_DATA) // oci won't set attr if data more than max
					m_bind_data.append(",");

				if (data_len <= MAX_VSESSION_BIND_DATA)
				{
					m_bind_data.append(binder.value);
				}
				else
				{
					if (m_bind_data.length() == 0)
						m_bind_data.append(" ");  // inject a space if buffer is empty therefore next bind data will has the leading comma.
				}
			}
		}
	default:
		rc = OCIBindByName(
				stmt->stmthp,
				(OCIBind **) &binder.bind,
				errhp,
				(text *) const_cast<char *>(binder.name.c_str()),
				binder.name.length(),
				(dvoid *)(binder.is_inout() ? NULL : const_cast<char *>(binder.value.c_str())),
				((binder.is_inout()) ? static_cast<OCCBindInOut &>(binder).maxlen : binder.array_max_data_size + ((stmt->version == occ::V1) ? 1 : 0)),
				(stmt->version == occ::V1) ? SQLT_STR : ((binder.is_inout()) ? SQLT_STR : SQLT_AFC),
				(dvoid *) binder.null_indicators,
				(binder.array_row_num<=1)? (ub2 *) NULL : binder.bind_data_size,
				(ub2 *) NULL,
				(ub4) 0,
				(ub4 *) NULL,
				(ub4)(at_exec ? OCI_DATA_AT_EXEC : OCI_DEFAULT));
		if(rc!=OCI_SUCCESS) {
			sql_error(rc, stmt);
			return -1;
		}
		break;
	}

	if (binder.array_row_num>1)
	{
		rc = OCIBindArrayOfStruct(
				(OCIBind *) binder.bind,
				errhp,
				(binder.type == OCC_TYPE_TIMESTAMP || binder.type == OCC_TYPE_TIMESTAMP_TZ) ? sizeof(OCIDateTime*) : binder.array_max_data_size+1,
				(binder.type == OCC_TYPE_TIMESTAMP || binder.type == OCC_TYPE_TIMESTAMP_TZ) ? sizeof (ub2) : 0,
				sizeof (ub2),
				0);
		if (rc!=OCI_SUCCESS) {
			sql_error(rc, stmt);
			return -1;
		}
	}

	if (at_exec)
	{
		rc = OCIBindDynamic(
				binder.bind,
				errhp,
				(dvoid *) &binder,
				OCCChild::placeholder_cb_in,
				(dvoid *) &binder,
				OCCChild::placeholder_cb_out);
		if (rc != OCI_SUCCESS)
		{
			sql_error(rc, stmt);
			return -1;
		}
	}

	return 0;
}

int OCCChild::bind(const std::string& name, const std::string& values, ub2* value_size,
		unsigned int value_max_size, unsigned int num, DataType type)
{

	// might need to commit/rollback. just to be safe.
	in_trans = true;

	// bind not supported with server-cached queries
	if (cur_results)
	{
		occ_error("binding: not supported with special queries");
		return -1;
	}

	// get current statement
	StmtCacheEntry *stmt = get_cur_stmt();
	if (stmt == NULL)
		return -1;


	// On your head be it if the the data gets mangled because you tried to log binary data
	if (logfile->get_log_level().level >= LOG_DEBUG) 
	{
		// turn CAL logging OFF temporarily; don't want to log bind vars to CAL
		bool old_enable_cal = logfile->get_enable_cal();
		logfile->set_enable_cal(false);
		if (values.length() > 1024)
		{
			// limit value to 1024 characters between brackets using printf truncation syntax
			WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "binding [%s]:[%.1021s...]", name.c_str(), StringUtil::hex_escape(values).c_str());
		}
		else
		{
			WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "binding [%s]:[%s]", name.c_str(), StringUtil::hex_escape(values).c_str());
		}
		// restore CAL logging
		logfile->set_enable_cal(old_enable_cal);
	}

	//add one to the bind array
	OCCBind *binder = new OCCBind();
	binder->name = name;
	binder->value = values;
	binder->type = type;
	binder->array_row_num = num;
	binder->array_max_data_size = value_max_size;
	for (unsigned int i=0; i<num; ++i) {
		binder->bind_data_size[i]= value_size[i];
		if ((stmt->version == occ::V1) && (type!=OCC_TYPE_BLOB_SINGLE_ROUND) &&
				(type!=OCC_TYPE_CLOB_SINGLE_ROUND) && (type!=OCC_TYPE_RAW) &&
				(type!=OCC_TYPE_TIMESTAMP) && (type!=OCC_TYPE_TIMESTAMP_TZ))
			binder->bind_data_size[i] = binder->bind_data_size[i] + 1;
	}
	bind_array->push_back(std::shared_ptr<OCCBind>(binder));

	return internal_bind(*binder, false/*at_exec*/);
}

/**
 * New OCC command to support binding to out-bound placeholders as in RETURNING ... INTO ... clause.
 * Current support is limited to returning only 1 row of data, and with data not exceeding MAX_OUT_BIND_VAR_SIZE.
 * The prototypical usage is for inserts like this:
 *   INSERT INTO tbl (col1, col2, ...) VALUES (tbl_seq.NEXTVAL, :in2, ...) RETURNING col1 INTO :out1
 *
 * @param name Outbound placeholder name (including the ':') [IN]
 * @param type Data type, this is currently ignored and we only support SQLT_STR. [IN]
 * @return 0 - Success, -1 - Failure.
 */
int OCCChild::bind_out(const std::string &name, DataType type)
{
	// might need to commit/rollback. just to be safe.
	in_trans = true;

	// bind not supported with server-cached queries
	if (cur_results)
	{
		occ_error("binding out: not supported with special queries");
		return -1;
	}

	// get current statement
	StmtCacheEntry *stmt = get_cur_stmt();
	if (stmt == NULL)
		return -1;

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "binding out [%s]", name.c_str());

	//add one to the bind array
	OCCBindInOut *binder = new OCCBindInOut();
	binder->name = name;
	binder->value.clear(); // already empty, but just pointing it out.
	binder->null_indicators[0] = -1; // initialize the IN value to NULL
	binder->type = type;
	binder->errhp = this->errhp;
	binder->pos = out_bind_array->size();
	binder->maxlen = config->get_int("max_out_bind_var_size", MAX_OUT_BIND_VAR_SIZE);

	// For in-out bind var, it's tied to the Statement Cache
	out_bind_array->push_back(std::shared_ptr<OCCBindInOut>(binder));

	return internal_bind(*binder, true/*at_exec*/);
}

/**
 * Send the outbound placeholder data back to the client.
 *
 * @param stmt Pointer to the current StmtCacheEntry. [IN]
 */
int OCCChild::return_out_bind_vars(StmtCacheEntry *stmt)
{
	if (!out_bind_array)
	{
		occ_error("out_bind_array not initialized.");
		return -1;
	}

	unsigned int out_bind_cnt = out_bind_array->size();

	if (out_bind_cnt > 0)
	{
		std::ostringstream stream;
		NetstringWriter nw(&stream);

		// There are OUT bind vars
		column_output output;
		std::string name, row_cnt;
		std::string value;
		OCCBindInOut *first_out_col = out_bind_array->at(0).get();

		// PL/SQL block bind variables will not have had their 'rows' values set.
		// For DML with RETURNING ... INTO ... clauses, this is set in ph_cb_out(); that is not
		// called for PL/SQL blocks.  Perhaps it should be called in execute?
		//
		if ((stmt->type == BEGIN_STMT) || (stmt->type == DECLARE_STMT))
		{
			for( unsigned i = 0 ; i < out_bind_array->size() ; ++i )
			{
				OCCBindInOut* out_col = out_bind_array->at(i).get();
				out_col->rows = 1 ;
			}
		}
		unsigned int num_rows = first_out_col->rows;

		// Support returning of 1 row for now.
		if (num_rows > MAX_DYNAMIC_BIND_ROWS)
		{
			WRITE_LOG_ENTRY(logfile, LOG_INFO, "OUT bind var returning more than 1 row. Not supported.");
			num_rows = MAX_DYNAMIC_BIND_ROWS;
		}

		// Send the row count to client.
		StringUtil::fmt_ulong(row_cnt, num_rows);
		nw.add(OCC_VALUE, row_cnt);

		for (unsigned int row = 0; row < num_rows; ++row)
		{
			for (unsigned int i = 0; i < out_bind_cnt; ++i)
			{
				OCCBindInOut *binder = out_bind_array->at(i).get();
				if (binder->get_column(&output, row))
				{
					occ_error("Failed OCCBindInOut::get_column()");
					return -1;
				}

				// Value
				if (output.indicator == -1)
				{
					nw.add(OCC_VALUE, null_value);
					WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "out bind [%s]:NULL", binder->name.c_str());
				}
				else
				{
					// NOTE: output.column_size is not the number of bytes in this value; 
					// it's the maximum length this value could have.  This code is meant
					// to truncate at the first NUL.
					value.resize(output.column_size);
					char *dest = (char*)(value.c_str());
					if (stmt->version == occ::V1) {
						// Meant to truncate at first NUL
						strcpy(dest, output.data);
					} else {
						memcpy(dest, output.data, output.column_size);
					}
					nw.add(OCC_VALUE, value);
					WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "out bind [%s]:[%s]", binder->name.c_str(), value.c_str());
				}
			} // for row
		} // for i
		nw.write();
		eor(is_in_transaction() ? EORMessage::IN_TRANSACTION : EORMessage::FREE, stream.str());

		if (m_writer->write() < 0)
			return -1;
	}

	return 0;
}

#define CHECK_ERR_BREAK() { \
	if (rc != OCI_SUCCESS)\
	{\
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Error retrieving column info"); \
		delete stmt->columns;\
		stmt->columns = NULL;\
		break;\
	}\
}
//-----------------------------------------------------------------------------

int OCCChild::execute(int& _cmd_rc)
{
	OCCBind *binder;
	uint iterations;
	uint lob_size;
	uint i;
	int  rc;

	// normally we want to return zero out of handle_command()
	_cmd_rc = 0;

	// executing query.. will need to commit/rollback when asked
	in_trans = true;

	// is it a server-cached query with valid results?
	if (cur_results && results_valid)
	{
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "execute (cached): skipping");

		std::string out_buf;
		//return the number of columns
		StringUtil::fmt_ulong(out_buf, cur_results->get_num_columns());
		if (m_writer->add(OCC_VALUE, out_buf) < 0)
			return -1;

		// return the number of rows
		StringUtil::fmt_ulong(out_buf, cur_results->get_num_rows());
		if (m_writer->add(OCC_VALUE, out_buf) < 0)
			return -1;

		if (m_writer->write() < 0)
			return -1;

		return 0;
	}

	m_last_exec_rc = OCIR_ERROR;

	bool preExistingTransaction = is_in_transaction();

	// get current statement
	StmtCacheEntry *stmt = get_cur_stmt();
	if (stmt == NULL)
	{
		// there is no need to log the error to the log file, get_cur_stmt() already did that
		eor(is_in_transaction() ? EORMessage::IN_TRANSACTION : EORMessage::FREE, OCC_ERROR);
		m_writer->write();
		return -1;
	}

	bool is_dml = ((stmt->type == UPDATE_STMT) || (stmt->type == INSERT_STMT) || (stmt->type == DELETE_STMT) || (stmt->type == MERGE_STMT));
	// Do not allow certain types of Statements if we are
	// in a Global txn
	bool bad_stmt = (m_in_global_txn && (
				(stmt->type == CREATE_STMT) ||
				(stmt->type == DROP_STMT) ||
				(stmt->type == ALTER_STMT) ||
				(stmt->type == CALL_STMT)));
	if (bad_stmt)
	{
		WRITE_LOG_ENTRY(logfile, LOG_ALERT, "Cannot execute CREATE/DROP/ALTER in the middle of a global transaction.");
		CalTransaction::Status s(CAL::TRANS_ERROR, CAL::MOD_OCC, CAL::SYS_ERR_INTERNAL, -1);
		CalEvent e(CAL::EVENT_TYPE_ERROR, CAL::MOD_OCC, s, "m_err=Cannot execute CREATE/DROP/ALTER in the middle of a global transaction.");
		client_session.set_status(CAL::INPUT_FAILURE);
		eor(is_in_transaction() ? EORMessage::IN_TRANSACTION : EORMessage::FREE, OCC_ERROR);
		m_writer->write();
		return -1;
	}

	//only do one iteration
	//note:  an enhancement for the future will be to allow the client to specify multiple iterations
	if ((stmt->type == SELECT_STMT) || (stmt->type == SELECT_FOR_UPDATE_STMT))
	{
		iterations = 0;
	}
	else
	{
		iterations = 1;
		if ((bind_array->size()>0) && (bind_array->at(0).get()->array_row_num > 1))
		{
			iterations = bind_array->at(0).get()->array_row_num;
			for (unsigned int j = 1; j < bind_array->size(); ++j) {
				if (iterations != bind_array->at(j).get()->array_row_num) {
					occ_error("Array bind length is different among columns.");
					return -1;
				}
			}
		}
	}

	// execute the statement
	OCIFuncParams params;
	params.exec_params.iterations = iterations;
	rc = run_oci_func(OCC_EXECUTE, stmt, params);
	if (rc != OCIR_OK)
	{
		// whoops it failed
		if (rc == OCIR_FATAL)
			_cmd_rc = -1;
		return -1;
	}

	//check if we need to send up BLOB data as part of a bind
	for (i = 0; i < bind_array->size(); i++)
	{
		binder = bind_array->at(i).get();
		if(binder->lob) {
			//write the data
			lob_size = binder->value.length();
			if (lob_size == 0) {
				rc = OCILobTrim(svchp, errhp, binder->lob, 0);
			} else {
				rc = OCILobWrite(svchp, errhp, binder->lob, (ub4*)&lob_size, 1,
						(dvoid*) const_cast<char*>(binder->value.c_str()), binder->value.length(),
						OCI_ONE_PIECE, 0, 0, 0, SQLCS_IMPLICIT);
			}
			if(rc!=OCI_SUCCESS) {
				sql_error(rc, stmt);
				return -1;
			}
		}
	}

	unsigned int rows = 0;

	if ((stmt->type == SELECT_STMT) || (stmt->type == SELECT_FOR_UPDATE_STMT))
	{
		// we'll write the row count to the log later...
		rc = OCIAttrGet(
				(CONST dvoid *) stmt->stmthp,
				OCI_HTYPE_STMT,
				(dvoid *) &rows, 
				(ub4 *) 0,
				OCI_ATTR_ROW_COUNT,
				errhp);
		if (rc != OCI_SUCCESS)
			rows = 0;

		if (stmt->num_cols == 0)
		{
			// get number of columns
			rc = OCIAttrGet(
					(CONST dvoid *) stmt->stmthp,
					(ub4) OCI_HTYPE_STMT,
					(dvoid *) &stmt->num_cols,
					(ub4 *) NULL,
					(ub4) OCI_ATTR_PARAM_COUNT,
					errhp);
			if (rc != OCI_SUCCESS)
			{
				sql_error(rc, stmt);
				return -1;
			}

			if (stmt->columns == NULL)
			{
				// We pre-set the length to avoid lots of appends to extend the TArray;
				// each element will be filled out inside the loop

				stmt->columns = new std::vector<ColumnInfo>(stmt->num_cols);
			}

			for (ub4 i = 0; i < stmt->num_cols; ++i)
			{
				OCIParam *paramdp = NULL;

				rc = OCIParamGet((CONST dvoid *)stmt->stmthp,
						(ub4) OCI_HTYPE_STMT, 
						errhp, 
						(void **)&paramdp, 
						(i + 1));
				CHECK_ERR_BREAK();

				ub4 name_len = 0;
				char *name = NULL;

				rc = OCIAttrGet((CONST dvoid *) paramdp,
						(ub4) OCI_DTYPE_PARAM,
						(dvoid *) &name,
						(ub4 *) &name_len,
						(ub4) OCI_ATTR_NAME,
						errhp);
				CHECK_ERR_BREAK();

				// note that 'name' is not guaranteed to be null-terminated by
				// oracle, and must always be used with 'name_len'.

				(*stmt->columns)[i].name.assign(name, name_len);

				rc = OCIAttrGet((CONST dvoid *) paramdp,
						(ub4) OCI_DTYPE_PARAM,
						(dvoid *) &((*stmt->columns)[i].type),
						(ub4 *) 0,
						(ub4) OCI_ATTR_DATA_TYPE,
						errhp);
				CHECK_ERR_BREAK();

				ub4 char_semantics;
				rc = OCIAttrGet((CONST dvoid *) paramdp,
						(ub4) OCI_DTYPE_PARAM,
						(dvoid *) &char_semantics,
						(ub4 *) 0,
						(ub4) OCI_ATTR_CHAR_USED,
						errhp);
				CHECK_ERR_BREAK();

				if (char_semantics)
				{
					// retrieve column width in characters
					rc = OCIAttrGet((CONST dvoid *) paramdp,
							(ub4) OCI_DTYPE_PARAM,
							(dvoid *) &((*stmt->columns)[i].width),
							(ub4 *) 0,
							(ub4) OCI_ATTR_CHAR_SIZE,
							errhp);
					CHECK_ERR_BREAK();
				}
				else
				{
					// retrieve column width in bytes
					rc = OCIAttrGet((CONST dvoid *) paramdp,
							(ub4) OCI_DTYPE_PARAM,
							(dvoid *) &((*stmt->columns)[i].width),
							(ub4 *) 0,
							(ub4) OCI_ATTR_DATA_SIZE,
							errhp);
					CHECK_ERR_BREAK();
				}

				rc = OCIAttrGet((CONST dvoid *) paramdp,
						(ub4) OCI_DTYPE_PARAM,
						(dvoid *) &((*stmt->columns)[i].precision),
						(ub4 *) 0,
						(ub4) OCI_ATTR_PRECISION,
						errhp);
				CHECK_ERR_BREAK();

				rc = OCIAttrGet((CONST dvoid *) paramdp,
						(ub4) OCI_DTYPE_PARAM,
						(dvoid *) &((*stmt->columns)[i].scale),
						(ub4 *) 0,
						(ub4) OCI_ATTR_SCALE,
						errhp);
				CHECK_ERR_BREAK();

				WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "column name[%d] is %s, type is %d, width is %d, precision is %d, scale is %d", i, (*stmt->columns)[i].name.c_str(), (*stmt->columns)[i].type, (*stmt->columns)[i].width, (*stmt->columns)[i].precision, (*stmt->columns)[i].scale);
			} // for i
		} // if num_cols == 0
	}
	else if (is_dml)
	{
		// get how many rows we updated/inserted/deleted
		rc = OCIAttrGet(
				(CONST dvoid *) stmt->stmthp,
				OCI_HTYPE_STMT,
				(dvoid *) &rows, 
				(ub4 *) 0,
				OCI_ATTR_ROW_COUNT,
				errhp);

		if (rc == OCI_SUCCESS)
		{
			// log the info
			const char *noun;
			const char *verb = "";
			noun = (rows == 1) ? "row" : "rows";
			if (stmt->type == UPDATE_STMT)
				verb = "updated";
			else if (stmt->type == INSERT_STMT)
				verb = "created";
			else if (stmt->type == DELETE_STMT)
				verb = "deleted";
			else if (stmt->type == CALL_STMT)
				verb = "called";
			else if (stmt->type == MERGE_STMT)
				verb = "merged";
			WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "%u %s %s", rows, noun, verb);

			if (!m_has_real_dml)
				m_has_real_dml = (rows > 0);
		}
		else
		{
			// Kind of nasty.
			m_has_real_dml = true;
		}
	}
	else
	{
		// log that the statement was executed successfully
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "execution completed");
	}

	std::string out_buf;

	if ((is_dml || stmt->type == BEGIN_STMT || stmt->type == DECLARE_STMT || stmt->type == ROLLBACK_STMT || stmt->type == COMMIT_STMT) && (out_bind_array->size() == 0)) {
		std::ostringstream stream;
		NetstringWriter nw(&stream);
		StringUtil::fmt_ulong(out_buf, stmt->num_cols);
		nw.add(OCC_VALUE, out_buf);
		StringUtil::fmt_ulong(out_buf, rows);
		nw.add(OCC_VALUE, out_buf);
		nw.write();

		eor(is_in_transaction() ? EORMessage::IN_TRANSACTION : EORMessage::FREE, stream.str());
	} else {
		//return the number of columns that will be returned
		StringUtil::fmt_ulong(out_buf, stmt->num_cols);
		if (m_writer->add(OCC_VALUE, out_buf) < 0)
		{
			return -1;
		}

		// And return the number of rows affected as well
		StringUtil::fmt_ulong(out_buf, rows);
		if (m_writer->add(OCC_VALUE, out_buf) < 0)
		{
			return -1;
		}
	}
	// flush
	if (m_writer->write() < 0)
		return -1;

	if (cur_results)
	{
		// if we are caching, remember how many columns there are
		cur_results->set_num_columns(stmt->num_cols);
		cur_results->set_num_rows(rows);
	}

	// log if oracle started transaction spuriously 
	if (!preExistingTransaction && is_in_transaction()) {
		if (stmt->type == SELECT_STMT) {
			CalEvent e("ORA_ONLY_TXN", m_query_hash, CAL::TRANS_OK);
		}
	}


	if (is_dml || stmt->type == BEGIN_STMT || stmt->type == DECLARE_STMT)
	{
		m_last_exec_rc = return_out_bind_vars(stmt);
		return m_last_exec_rc;
	}

	m_last_exec_rc = OCIR_OK;
	return 0;
}

//-----------------------------------------------------------------------------

int OCCChild::row_count()
{
	int rc;
	unsigned int rows;
	std::string row_count;

	// get current statement
	StmtCacheEntry *stmt = get_cur_stmt();
	if (stmt == NULL)
		return -1;

	rc = OCIAttrGet(
			(CONST dvoid *) stmt->stmthp,
			OCI_HTYPE_STMT,
			(dvoid *) &rows, 
			(ub4 *) 0,
			OCI_ATTR_ROW_COUNT,
			errhp);
	if(rc!=OCI_SUCCESS) {
		sql_error(rc, stmt);
		return -1;
	}

	//return the number of rows
	StringUtil::fmt_ulong(row_count, rows);
	//ignore error?
	m_writer->write(OCC_VALUE, row_count);
	return 0;
}

//-----------------------------------------------------------------------------

int OCCChild::col_count()
{
	std::string str;

	// get current statement
	StmtCacheEntry *stmt = get_cur_stmt();
	if (stmt == NULL)
		return -1;

	//return the number of columns
	StringUtil::fmt_ulong(str, stmt->num_cols);

	//ignore error?
	m_writer->write(OCC_VALUE, str);
	return 0;
}

/**
 * Returns the column headers in a SELECT query back to
 * the client.
 * The list will be sent in the same order as in the
 * query itself.
 */
int OCCChild::col_names(ub4 _num_cols, std::vector<ColumnInfo>* _cols)
{
	if (_cols == NULL)
	{
		occ_error("Column names were lost before requested.");
		return -1;
	}

	unsigned int col_cnt = (unsigned int)_cols->size();
	if (col_cnt != _num_cols)
	{
		occ_error("Column count mismatch.");
		return -1;
	}

	return Util::out_col_names(m_writer, _cols);
}

/**
 * Returns the column headers in a SELECT query back to
 * the client.
 * The list will be sent in the same order as in the
 * query itself.
 */
int OCCChild::col_info(ub4 _num_cols, std::vector<ColumnInfo>* _cols)
{
	if (_cols == NULL)
	{
		occ_error("Column names were lost before requested.");
		return -1;
	}

	unsigned int col_cnt = (unsigned int)_cols->size();
	if (col_cnt != _num_cols)
	{
		occ_error("Column count mismatch.");
		return -1;
	}

	return Util::out_col_info(m_writer, _cols);
}

//-----------------------------------------------------------------------------

unsigned long long OCCChild::fetch(const std::string& count)
{
	int rc, no_more_data;
	int nrows;
	unsigned int rows_this_block=0, rows_fetched=0, start_row=0;
	unsigned int col;
	unsigned int lob_size, lob_size_read, buffer_size;
	column_output output;
	std::string value;
	unsigned long long value_buf_len = 0;
	// doing OCIStmtFetch here, dunno if we really need to set the "in transaction"
	// flag, but just to be safe
	in_trans = true;

	// do we have valid cached results?
	if (cur_results && results_valid)
	{
		// block fetching not supported with special queries
		if (StringUtil::to_uint(count) != 0)
		{
			occ_error("fetch (cached): block fetching not supported with special queries");
			return -1;
		}

		const std::vector<std::string> & results = cur_results->get_results();

		uint cached_columns = cur_results->get_num_columns();
		uint cached_rows = results.size() / cached_columns;
		int index = 0;

		for (uint i = 0; i < cached_rows; i++)
		{
			for (uint j = 0; j < cached_columns; j++)
			{
				m_writer->add(OCC_VALUE, results[index]);
				value_buf_len += results[index].length();
				index++;
			}			
		}

		if (index != results.size())
			occ_error("fetch (cached): unaligned data in the cached results array");

		eor(is_in_transaction() ? EORMessage::IN_TRANSACTION : EORMessage::FREE, OCC_NO_MORE_DATA);

		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "fetch (cached): fetched %u rows", cached_rows);

		if (m_writer->write() < 0)
			return -1;

		return value_buf_len;
	}

	// get current statement
	StmtCacheEntry *stmt = get_cur_stmt();
	if (stmt == NULL)
	{
		// send back error only for rc == OK. if last rc is not OK, there is no need to send
		if (m_last_exec_rc == OCIR_OK)
			occ_error("fetch requested but no statement exists");
		else {
			eor(is_in_transaction() ? EORMessage::IN_TRANSACTION : EORMessage::FREE);
			m_writer->write();
		}
		return -1;
	}

	nrows = StringUtil::to_uint(count);
	if(nrows==0) {
		//fetch all rows
		nrows = 2147483647;
	}

	// initialize the define array
	if (initialize_define_array(stmt->version == occ::V2))
		return -1;

	if (new_fetch)
	{
		current_row = 0;
		max_rows = ((unsigned) nrows < max_fetch_block_size) ? nrows : max_fetch_block_size;
		new_fetch = false;
	}

	no_more_data = 0;

	bool block_fetched = false;
	for(;nrows>0 && !no_more_data;nrows-=max_fetch_block_size) {

		// NOTE: Write block fetched in previous iteration here
		// instead of waiting till the very end.
		// This is so that we don't accumulate a ton of data 
		// in te m_writer's buffer
		if (block_fetched)
			if (m_writer->write() < 0)
				return -1;

		// fetch a block of rows
		rows_this_block = ((unsigned)nrows<max_fetch_block_size)?nrows:max_fetch_block_size;
		block_fetched = true;

		if(rows_this_block > max_rows) {
			occ_error("Fetch block size > initial fetch block size, insufficient buffers available");
			return -1;
		}

		if(clear_indicators())
			return -1;

		// fetch the rows
		OCIFuncParams params;
		params.fetch_params.rows_this_block = rows_this_block;
		rc = run_oci_func(OCC_FETCH, stmt, params);
		if (rc != OCIR_OK)
			return -1;

		rc = OCIAttrGet(
				(CONST dvoid*) stmt->stmthp,
				OCI_HTYPE_STMT,
				(dvoid *) &rows_fetched, 
				(ub4 *) NULL,
				OCI_ATTR_ROW_COUNT,
				errhp);
		if(rc!=OCI_SUCCESS) {
			sql_error(rc, stmt);
			return -1;
		}

		if(rows_fetched - current_row < rows_this_block)
			no_more_data = 1;

		// send this block of rows (or as many as were fetched)
		for(start_row = current_row;current_row<rows_fetched;current_row++) {
			for (col = 0; col < stmt->num_cols; col++)
			{
				OCCDefine& define = stmt->defines[col];
				if (define.get_column(current_row - start_row, &output))
				{
					occ_error("Failed to OCCDefine::get_column");
					return -1;
				}

				// The column value to be sent to the client
				value.clear();

				//check for the indicator
				if(output.indicator==-1) {
					//it's a null value
					value = null_value;
				} else {
					//check the type
					if(output.type == SQLT_STR || output.type == SQLT_LNG)
					{
						if ((output.type == SQLT_STR) && (stmt->version == occ::V2)) {
							value.assign(output.data, output.str_size);
						} else {
							// NOTE: output.column_size is not the number of bytes in this value;
							// it's the maximum length this value could have.  This code is meant
							// to truncate at the first NUL.
							int data_size = strlen(output.data);
							value.assign(output.data, data_size);
						}
					} else if(output.type == SQLT_BIN){
						ub4 raw_size = OCIRawSize ( envhp, (OCIRaw *)output.data);
						value.assign(output.data+4, raw_size);
					} else if ((output.type == SQLT_TIMESTAMP) || (output.type == SQLT_TIMESTAMP_TZ)) {
						// #define DATE_TIME_FORMAT	"DD-MM-YYYY HH24:MI:SS.FF3"
						// #define DATE_TIME_FORMAT_TZ	"DD-MM-YYYY HH24:MI:SS.FF3 TZR"
#define DATE_TIME_BUFF_SIZE 30
						char tmp_buff[DATE_TIME_BUFF_SIZE];
						ub4 tmp_buff_size = DATE_TIME_BUFF_SIZE;
						rc = OCIDateTimeToText(envhp, errhp, output.datetime,
								(output.type == SQLT_TIMESTAMP) ? (OraText*)DATE_TIME_FORMAT : (OraText*)DATE_TIME_FORMAT_TZ,
								(output.type == SQLT_TIMESTAMP) ? (sizeof(DATE_TIME_FORMAT) - 1) : (sizeof(DATE_TIME_FORMAT_TZ) - 1),
								3/*frac secs*/, NULL, 0, &tmp_buff_size, (OraText*)tmp_buff);
						if(rc!=OCI_SUCCESS) {
							sql_error(rc, stmt);
							return -1;
						}
						value.assign(tmp_buff, tmp_buff_size);
					} else {
						// type==SQLT_BLOB || type==SQLT_CLOB
						// figure out the size
						oraub8	lob_size_ub8;
						rc = OCILobGetLength2(svchp, errhp, output.lob, &lob_size_ub8);
						lob_size = (unsigned int) lob_size_ub8;

						if(rc!=OCI_SUCCESS) {
							sql_error(rc, stmt);
							return -1;
						}

						if (lob_size == 0) {
							value.clear();
						} else if (output.type == SQLT_CLOB) {

							// make sure we have enough space
							oraub8	byte_amt, char_amt;
							byte_amt = char_amt = lob_size;
							// according to the oracle documentation the maximum possible 
							// length of a UTF-8 CLOB is 3 bytes (AL32UTF8, 4 bytes) per character.  So we multiply 
							// https://docs.oracle.com/cd/B28359_01/server.111/b28298/ch6unicode.htm
							buffer_size = (lob_size * 4) + 1;
							// since oracle will not null terminate the CLOB data fill the buffer 
							// with NULLs ('\0')
							value.resize(0);
							value.resize(buffer_size);
							rc = OCILobRead2(svchp, errhp, output.lob, &byte_amt, &char_amt, 1,
									(void*)(value.c_str()), buffer_size, OCI_ONE_PIECE, 0, 0, 0, SQLCS_IMPLICIT);
							lob_size_read = (unsigned int) char_amt;

							if(rc!=OCI_SUCCESS) {
								value.resize(0);
								sql_error(rc, stmt);
								WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "got failure during OCILobRead, lob_size = %u, lob_read_size = %u", lob_size, lob_size_read);
								return -1;
							}
							value.resize(lob_size_read);
						} else { // SQLT_BLOB
							// make sure we have enough space
							oraub8	byte_amt, char_amt;
							byte_amt = char_amt = lob_size;
							value.resize(lob_size);
							rc = OCILobRead2(svchp, errhp, output.lob, &byte_amt, &char_amt, 1,
									(void*)(value.c_str()), lob_size, OCI_ONE_PIECE, 0, 0, 0, SQLCS_IMPLICIT);
							lob_size_read = (unsigned int) byte_amt;

							if(rc!=OCI_SUCCESS) {
								value.resize(0);
								sql_error(rc, stmt);
								WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "got failure during OCILobRead, lob_size = %u, lob_read_size = %u", lob_size, lob_size_read);
								return -1;
							}
							value.resize(lob_size_read);
						}
					}
				}

				// Write the value out
				m_writer->add(OCC_VALUE, value);
				
				value_buf_len += value.length();

				if (cur_results)
				{
					// We are caching results of a special query.
					cur_results->add_result(value);
				}
			}
		}
	}

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "fetched %u rows", rows_fetched);

	//send the termination
	if(no_more_data)
	{
		// mark the results cache valid
		if (cur_results)
		{
			cur_results->validate();
		}

		eor(is_in_transaction() ? EORMessage::IN_TRANSACTION : EORMessage::FREE, OCC_NO_MORE_DATA);

		cur_stmt = NULL;
	}
	else
	{
		eor(is_in_transaction() ? EORMessage::IN_CURSOR_IN_TRANSACTION : EORMessage::IN_CURSOR_NOT_IN_TRANSACTION, OCC_OK);
	}

	// Now, write the final block read
	rc = m_writer->write();

	if (rc < 0)
		return -1;

	return value_buf_len;
}

//-----------------------------------------------------------------------------

int OCCChild::initialize_define_array(bool use_datetime)
{
	uint i;
	int size, total_size;
	ub2 sqlt;

	// get current statement
	StmtCacheEntry *stmt = get_cur_stmt();
	if (stmt == NULL)
		return -1;

	// if we already prepared the define array, we're done
	if (stmt->defines != NULL)
		return 0;

	// from the statement handle, we can figure out how many "defines" we need to call
	if ((stmt->type != SELECT_STMT) && (stmt->type != SELECT_FOR_UPDATE_STMT))
	{
		occ_error("client attempt to fetch results on a non-select statement");
		return -1;
	}
	if (!stmt->num_cols)
	{
		occ_error("select statement with zero columns");
		return -1;
	}

	// allocate the define array
	stmt->defines = new OCCDefine[stmt->num_cols];

	//add them to the array
	total_size = 0;
	for (i = 0; i < stmt->num_cols; i++)
	{
		OCCDefine& define = stmt->defines[i];

		if (get_column_size(&size, &sqlt, i + 1, use_datetime))
			return -1;

		// maybe use the pre-allocated buffers
		char *col_buf = NULL;
		if (size > 0)
		{
			int this_buf_size = (size + 1) * max_fetch_block_size;
			if (total_size + this_buf_size <= COL_DATA_BUF_SIZE)
			{
				col_buf = data_buf + total_size;
				total_size += this_buf_size + (3 - ((this_buf_size + 3) & 3)); // align to 4-byte boundary
			}
		}
		sb2 *indicator_buf = NULL;
		if (i < NUM_INDICATOR_BUF)
			indicator_buf = indicator_bufs[i];
		ub2 *str_size_buf = NULL;
		if (i < NUM_STR_SIZE_BUF)
			str_size_buf = str_size_bufs[i];

		// set up the define
		// NOTE: have to use max_fetch_block_size here, not max_rows because we
		// might re-use this statement later with a larger max_rows (but in any
		// event, it will not be bigger than max_fetch_block_size)
		if (define.init(max_fetch_block_size, size + 1, sqlt, envhp, col_buf, indicator_buf, str_size_buf))
		{
			if (define.get_oci_rc())
				sql_error(define.get_oci_rc(), stmt);
			else
				occ_error("Failed to OCCDefine::init (internal, non-OCI error)");
			return -1;
		}

		// define it
		int retcode_define_by_pos;
		retcode_define_by_pos = define.define_by_pos(stmt->stmthp, errhp, i + 1, oracle_lobprefetch_size);

		if (retcode_define_by_pos)
		{
			if (define.get_oci_rc())
				sql_error(define.get_oci_rc(), stmt);
			else
				occ_error("Failed to OCCDefine::define_by_pos (internal, non-OCI error)");
			if (retcode_define_by_pos != -2) return -1;
		}
	}

	return 0;
}

//-----------------------------------------------------------------------------

int OCCChild::clear_indicators()
{
	uint i;

	// get current statement
	StmtCacheEntry *stmt = get_cur_stmt();
	if (stmt == NULL)
		return -1;

	for (i = 0; i < stmt->num_cols; i++)
	{
		OCCDefine& define = stmt->defines[i];
		if (define.clear_indicators())
		{
			if (define.get_oci_rc())
				sql_error(define.get_oci_rc(), stmt);
			else
				occ_error("Failed to OCCDefine::clear_indicators (internal, non-OCI error)");
			return -1;
		}
	}

	return 0;
}


bool OCCChild::real_oci_handle_free(dvoid *&hndlp, ub4 type, const char *res, LogLevelEnum level)
{
	int rc;

	if (hndlp == NULL) return true;

	rc = OCIHandleFree(hndlp, type);
	hndlp = NULL;
	if (rc != OCI_SUCCESS) {
		std::ostringstream os;
		os << "failed to OCIHandleFree(";
		if (res)
			os << res;
		else 
			os << "(null)";
		os <<")";
		log_oracle_error(rc, os.str().c_str(), level);
		return false;
	}
	return true;
}


int OCCChild::get_column_size(int *size, ub2 *type, ub4 pos, bool use_datetime)
{
	int rc = 0;
	StmtCacheEntry *stmt = get_cur_stmt();

	if (stmt == NULL)
		return -1;

	OCIParam *param = NULL;

	//figure out how large this potentially can be
	rc = OCIParamGet(stmt->stmthp, OCI_HTYPE_STMT, errhp, (void **) &param, (ub4) pos);
	if(rc!=OCI_SUCCESS) {
		sql_error(rc, stmt);
		return -1;
	}
	if (param == NULL)
	{
		occ_error("Failed to get column parameters.");
		return -1;
	}

	//check which type it is
	ub2 oci_type = 0;
	rc = OCIAttrGet((CONST dvoid *)param,OCI_DTYPE_PARAM,&oci_type,0,OCI_ATTR_DATA_TYPE,errhp);
	if(rc!=OCI_SUCCESS) {
		sql_error(rc, stmt);
		return -1;
	}

	// convert the type from OCI_TYPECODE to SQLT
	switch(oci_type) {
	case OCI_TYPECODE_BLOB:
		*type = SQLT_BLOB;
		break;
	case OCI_TYPECODE_CLOB:
		*type = SQLT_CLOB;
		break;
	case SQLT_LNG :
		*type = SQLT_LNG;
		break;
	case SQLT_BIN :
		*type = SQLT_BIN;
		break;
	case SQLT_DAT :
	case SQLT_TIMESTAMP :
		if (use_datetime)
			*type = SQLT_TIMESTAMP;
		else {
			*type = SQLT_STR;
			CalEvent e(CAL_EVENT_DATETIME, "Timestamp", CAL::TRANS_OK);
		}
		stmt->has_datetime = true;
		break;
	case SQLT_TIMESTAMP_TZ :
		if (use_datetime)
			*type = SQLT_TIMESTAMP_TZ;
		else {
			*type = SQLT_STR;
			CalEvent e(CAL_EVENT_DATETIME, "Timestamp_tz", CAL::TRANS_OK);
		}
		stmt->has_datetime = true;
		break;
	default:
		*type = SQLT_STR;
		break;
	}

	// Get the column size
	*size = 0;
	if (*type==SQLT_STR) {
		if (oci_type == SQLT_NUM) {
			// OCIAttrGet(... OCI_ATTR_DISP_SIZE ) would return 40, however this is incorrect. if number is large, more than 40 digits,
			// fetch will either truncate data (if the result set is smaller than fetch bulk size) or return "ORA-01406: fetch column value is truncated"
			// Oracle proposed work-around for now is to use the maximum possible size
			*size = 133;
		} else {
			rc = OCIAttrGet((dvoid *)param,OCI_DTYPE_PARAM,size,0,OCI_ATTR_DISP_SIZE,errhp);

			if(rc!=OCI_SUCCESS) {
				sql_error(rc, stmt);
				return -1;
			}
		}
	} else if (*type==SQLT_BIN) {
		rc = OCIAttrGet((dvoid *)param,OCI_DTYPE_PARAM,size,0,OCI_ATTR_DATA_SIZE,errhp);

		//oci uses first three bytes to store the length of this data.
		*size+=4;

		if(rc!=OCI_SUCCESS) {
			sql_error(rc, stmt);
			return -1;
		}
	} else if (*type == SQLT_LNG) {
		*size = 8192;
	} else {
		*size = -1;
	}

	return 0;
}

/**
 * @brief This method starts an Oracle Global Transaction
 * on the current connection with the given XID (Global Transaction ID)
 * by calling OCITransStart()
 *
 * The XID must have this format:
 *   formatID.gtrid.bqual
 * where
 *   formatID is the format ID in unsigned int
 *   gtrid is the Transaction ID for the 'local' connection (up to char[64])
 *   bqual is the Branch Qualifier (up to char[64])
 *
 * @param xid The global transaction ID provided by the client.
 * @param timeout The timeout value to assume. If this is 0, the default will be assumed.
 * @param role What role the database assumes in the 2PC transaction.
 */
int OCCChild::trans_start(const std::string &xid, unsigned int timeout, TransRole role, TransType type)
{
	std::string xid_copy(xid);
	int rc = 0;

	// If we are already in a 2PC transaction,
	// Do nothing
	if (m_in_global_txn)
	{
		// If it is the same transaction
		if (xid != m_curr_xid)
		{
			std::ostringstream os;
			os << "Already in a global transaction (" << m_curr_xid << "). Cannot start transaction " << xid;
			occ_error(os.str().c_str());
			return -1;
		}

		WRITE_LOG_ENTRY(logfile, LOG_WARNING, "Trying to start the same global transaction (%s) again. (Skipped).", xid.c_str());
		return 0;
	}

	if (set_xid(xid))
	{
		// set_xid() will clear the 2pc state
		return -1;
	}

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Starting transaction %s (timeout=%u).", xid.c_str(), (timeout ? timeout : m_default_trans_timeout));

	std::unique_ptr<CalEvent> trans_start_event;
	trans_start_event.reset(new CalEvent(CAL_EVENT_TRANS_START));
	trans_start_event->SetName(CAL_EVENT_DISTRIBUTED);
	trans_start_event->AddData(CAL_DATA_XID, xid);

	rc = OCITransStart(svchp, errhp, (timeout ? timeout : m_default_trans_timeout), OCI_TRANS_NEW | (trans_type_to_oci(type) & OCI_TRANS_TYPEMASK));
	if (rc != OCI_SUCCESS)
	{
		std::ostringstream os;
		os << "Failed to start transaction " << xid;
		log_oracle_error(rc, os.str().c_str());
		clear_2pc_state();
		m_writer->write(OCC_ERROR, os.str());
		if (trans_start_event)
		{
			CalTransaction::Status s(CAL::TRANS_ERROR, CAL::MOD_OCC, CAL::SYS_ERR_ORACLE, rc);
			trans_start_event->SetStatus(s);
		}
		return -1;
	}
	else
	{
		if (trans_start_event)
			trans_start_event->SetStatus(CAL::TRANS_OK);
	}

	// All good, we are now in 2PC mode.
	m_in_global_txn = true;
	m_trans_role = role;

	return 0;
}

int OCCChild::trans_prepare(const std::string &_line)
{
	if (!m_in_global_txn)
	{
		// If we are not in global txn, we don't need to call TransPrepare. Do nothing.
		occ_error("Calling TransPrepare without TransStart. (Skipped).");
		return 0;
	}
	else if (m_phase1_done)
	{
		// If we have already done TransPrepare, we do nothing.
		occ_error("TransPrepare has already be done. (Skipped).");
		return 0;
	}
	else if (m_trans_role == POINT_SITE)
	{
		// This database is the commit point site, so we don't need to call TransPrepare. Do nothing.
		WRITE_LOG_ENTRY(logfile, LOG_INFO, "Calling TransPrepare on the commit point site. (Skipped).");
		m_writer->write(OCC_OK);
		return 0;
	}
	else if (!m_has_real_dml)
	{
		// There is no real DML (INSERT/UPDATE/DELETE) in this session
		WRITE_LOG_ENTRY(logfile, LOG_INFO, "No real DML's in this session. No need for TransPrepare. (Skipped).");
		m_writer->write(OCC_OK);
		m_phase1_done = true;
		return 0;
	}

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Preparing global transaction %s.", m_curr_xid.c_str());

	std::unique_ptr<CalEvent> trans_prepare_event;
	trans_prepare_event.reset(new CalEvent("TRANSPREPARE"));
	trans_prepare_event->SetName(CAL_EVENT_DISTRIBUTED);
	trans_prepare_event->AddData(CAL_DATA_XID, m_curr_xid);

	int rc = OCITransPrepare(svchp, errhp, OCI_DEFAULT);
	if (rc == OCI_SUCCESS_WITH_INFO)
	{
		WRITE_LOG_ENTRY(logfile, LOG_INFO, "TransPrepare has no effect.");
		// No real DML here.
		m_has_real_dml = false;
		if (trans_prepare_event)
			trans_prepare_event->SetStatus(CAL_STATUS_SUCCESS_WITH_INFO);
	}
	else if (rc != OCI_SUCCESS)
	{
		log_oracle_error(rc, "TransPrepare call failed.");
		if (trans_prepare_event)
		{
			CalTransaction::Status s(CAL::TRANS_ERROR, CAL::MOD_OCC, CAL::SYS_ERR_ORACLE, rc);
			trans_prepare_event->SetStatus(s);
		}
		return -1;
	}
	else
	{
		if (trans_prepare_event)
			trans_prepare_event->SetStatus(CAL::TRANS_OK);
	}

	m_phase1_done = true;
	m_writer->write(OCC_OK);
	return 0;
}

int OCCChild::clear_2pc_state(void)
{
	// If we've been in a Global transaction (OCITransStart called)
	// We have to cycle the transaction handle or we would not be
	// able to execute local transaction in this OCI session.
	if (m_in_global_txn && (transhp != NULL))
	{
		// Free the existing transaction handle
		int rc;
		if (DO_OCI_HANDLE_FREE(transhp, OCI_HTYPE_TRANS, LOG_WARNING) == false) {
			return -1;
		}

		// Allocate a new transaction handle
		rc = OCIHandleAlloc((dvoid *)envhp, (dvoid **)&transhp, OCI_HTYPE_TRANS, (size_t) 0, (dvoid **) 0);
		if (rc != OCI_SUCCESS)
		{
			log_oracle_error(rc, "Failed to get a transaction handle.");
			return -1;
		}

		// associate transaction handle with service context
		rc = OCIAttrSet((dvoid *)svchp, OCI_HTYPE_SVCCTX, transhp, 0, OCI_ATTR_TRANS, errhp);
		if (rc != OCI_SUCCESS)
		{
			log_oracle_error(rc, "Failed to set the transaction handle.");
			return -1;
		}
	}

	// Clear flags.
	m_has_real_dml = false;
	m_in_global_txn = false;
	m_phase1_done = false;
	m_curr_xid.clear();
	m_trans_role = POINT_SITE;

	return 0;
}

int OCCChild::set_xid(const std::string &xid)
{
	// Break up the XID
	XID ora_xid;
	std::string format_id, gtrid, bqual, xid_copy(xid);

	if (!StringUtil::tokenize(xid_copy, format_id, ':'))
	{
		std::ostringstream err_msg;
		err_msg << "Invalid XID: " << xid;
		occ_error(err_msg.str().c_str());
		return -1;
	}
	ora_xid.formatID = StringUtil::to_uint(format_id);

	if (!StringUtil::tokenize(xid_copy, gtrid, ':'))
	{
		std::ostringstream err_msg;
		err_msg << "Invalid XID: " << xid;
		occ_error(err_msg.str().c_str());
		return -1;
	}
	ora_xid.gtrid_length = gtrid.length();

	if (!StringUtil::tokenize(xid_copy, bqual, ':'))
	{
		std::ostringstream err_msg;
		err_msg << "Invalid XID: " << xid;
		occ_error(err_msg.str().c_str());
		return -1;
	}
	snprintf(ora_xid.data, sizeof(ora_xid.data), "%s%s", gtrid.c_str(), bqual.c_str());
	ora_xid.bqual_length = bqual.length();
	m_curr_xid = xid;

	int rc = OCIAttrSet((dvoid *)transhp, OCI_HTYPE_TRANS, &ora_xid, sizeof(XID), OCI_ATTR_XID, errhp);
	if (rc != OCI_SUCCESS)
	{
		log_oracle_error(rc, "Failed to set the XID.");
		clear_2pc_state();
		return -1;
	}

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "xid.data=%s", ora_xid.data);

	return 0;
}

int OCCChild::trans_forget()
{
	int rc = OCITransForget(svchp, errhp, OCI_DEFAULT);

	if (rc != OCI_SUCCESS)
	{
		log_oracle_error(rc, "Failed to forget global transaction.");
		return -1;
	}

	return 0;
}

/** read all files in the markdown_directory and insert structures to the current_markdown vector.  
 *  Of course, in general there will not be any files in the directory.  If there are any files, 
 *  the names indicate the type of markdown contained within.
 */
void OCCChild::build_markdowns()
{
	if (markdown_directory.empty())
		return; // no directory, can not be any markdowns.

	current_markdowns->clear(); // start with no markdowns.
	const char *path = markdown_directory.c_str();
	size_t n = 256;
	char *mybuf = new char[256]; // a big buffer for reading from stream
	char **lineptr = &mybuf;

	DIR *d = opendir(path);
	if (d == NULL) {
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "Markdown path %s does not exist", path);
		//CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_CONFIG, -1);

		delete[] mybuf;
		return; // can't read the directory, cannot be any markdowns.
	}
	mklist.load_control_files(path, logfile);
	struct dirent64 *ent;
	while ((ent = readdir64(d)) != NULL)
	{
		// skip dotfiles
		if (ent->d_name[0] == '.')
			continue;
		std::string filename = ent->d_name;
		// build full path to the dir entry
		char fdpath[256];
		if (snprintf(fdpath, sizeof(fdpath), "%s/%s", path, ent->d_name) >= (int) sizeof(fdpath))
		{
			closedir(d);
			WRITE_LOG_ENTRY(logfile, LOG_WARNING, "The markdown path is too long");
			CalTransaction::Status s(CAL::TRANS_WARNING, CAL::MOD_OCC, CAL::SYS_ERR_CONFIG, -1);
			CalEvent e(CAL::EVENT_TYPE_WARNING, CAL::MOD_OCC, s, "m_err=Markdown path is too long.");

			delete[] mybuf;
			return;
		}
		MarkdownEnum type = MARKDOWN_NONE; // the type is contained in the filename.
		StringUtil::to_lower_case(filename);
		if (strncmp(filename.c_str(), TABLE_PREFIX, sizeof(TABLE_PREFIX) - 1) == 0) {
			type = MARKDOWN_TABLE;
		} else if (strncmp(filename.c_str(), SQL_PREFIX, sizeof(SQL_PREFIX) - 1) == 0) {
			type = MARKDOWN_SQL;
		} else if (strncmp(filename.c_str(), URL_PREFIX, sizeof(URL_PREFIX) - 1) == 0) {
			type = MARKDOWN_URL;
		} else if (strncmp(filename.c_str(), HOST_PREFIX, sizeof(HOST_PREFIX) - 1) == 0) {
			type = MARKDOWN_HOST;
		} else if (strncmp(filename.c_str(), TRANS_PREFIX, sizeof(TRANS_PREFIX) - 1) == 0) {
			type = MARKDOWN_TRANS;
		} else if (strncmp(filename.c_str(), COMMIT_PREFIX, sizeof(COMMIT_PREFIX) - 1) == 0) {
			type = MARKDOWN_COMMIT;
		}
		if (type==MARKDOWN_NONE && 
				mklist.isEmpty()) {
			if (strncmp(filename.c_str(), "postinstall", sizeof("postinstall") - 1) == 0) // we expect postinstall.sh to be in the directory.
				WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Markdown file %s is of unknown type", ent->d_name);

			continue; // try the next file.
		}
		FILE *Fp = fopen(fdpath, "r");
		ssize_t st = getline (lineptr, &n, Fp); // puts the text into **lineptr. Should be null terminated
		if (st==-1) {
			// a bad file, with bad content.
			fclose(Fp); // cant use it, close it. BUG 60513
			continue; // try the next file.
		}

		MarkdownStruct mds;

		mds.type = type;

		mybuf = *lineptr; // there is a very small chance our ptr was realloced.
		mybuf[st-1] = 0;
		mds.detail = mybuf; // converts from char* into std::string.
		// see if there is an extra line in the file specifying the host
		st = getline (lineptr, &n, Fp);
		if (st>0) {
			mybuf = *lineptr; // the pointer may have changed (unlikely, but let's be careful)
			mybuf[st-1] = 0;
			mds.hostForDetail = mybuf;
		} else {
			// markdown applies to all hosts.
			mds.hostForDetail.clear(); // all hosts.
		}
		current_markdowns->push_back(mds);
		fclose(Fp);
	}
	delete[] mybuf;
	closedir(d);
}

/** This method is called with a TYPE and a string that should be associated.
 *  For HOST, there is no string we simply check if we are on the marked-down host.
 *  Returns 0 for no markdown, -1 for markdown found.  */
int OCCChild::check_markdowns(MarkdownEnum type, const std::string &_src) {
	bool fixedStr = false; // set true when we remove newlines, extra spaces, etc.

	// This method used a lot of std::string-specific methods (case-insensitive
	// search, etc.) but is almost never actually called unless ops needs
	// to mark down a table or DB.  We'll convert the query (a std::string)
	// to a std::string only if needed, so we don't have to rewrite this methods
	// entirely to use std::string methods.
	std::string src = _src;

	if (current_markdowns->empty() ) 
	{   // No markdown filters.
		return 0;
	}

	for (int i=0; i<current_markdowns->size(); ++i) {
		// Since current_markdowns will generally be empty, let's not
		// even convert the query to std::string unless we need to

		const MarkdownStruct &curMD = (*current_markdowns)[i];

		if (type!=curMD.type)
			continue; // not interested in this type of markdown.
		//  see if the hostForDetail is not empty, if so, make sure this is the host.
		if (curMD.hostForDetail != "" && StringUtil::compare_ignore_case(host_name, curMD.hostForDetail)!=0) {
			// wrong host for this.
			// WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Not processing markdown because wrong host");
			continue;
		}

		switch (curMD.type) {
		case MARKDOWN_TABLE:
			// for tables, branch depending on this statement.
			// it will be either UPDATE <table>, INSERT INTO, or some kind of SELECT statement.
			// first, just look for the table name.
			{
				//LFU: Most of sqls now starts with comment with the following format. So the "src" without comment
				std::string srcNoCmt;
				unsigned int srcStartLoc = src.find("*/"); // find end of sql comment
				if ((srcStartLoc != std::string::npos) && (srcStartLoc + 2 < src.length()))
				{
					srcNoCmt.assign(src.c_str(), srcStartLoc+2, src.length() - srcStartLoc - 2);
					src = srcNoCmt;
				}

				unsigned int tableNameLoc = StringUtil::index_of_ignore_case(src, curMD.detail);
				if (tableNameLoc==std::string::npos)
					continue; // the name is nowhere in the string.

				if (StringUtil::starts_with_ignore_case(src, "update")) {
					// this is an update statement.  get the index of the SET, make sure the name is before that.
					unsigned int setLoc = StringUtil::index_of_ignore_case(src, " set");
					if ((setLoc == std::string::npos) || (setLoc > tableNameLoc)) {
						return -1; // in the proper location for a table name, flag markdown.
					}
				} else if (StringUtil::starts_with_ignore_case(src, "insert")) {
					// make sure table name is before the (
					unsigned int parenLoc = src.find('(');
					if ((parenLoc == std::string::npos) || (parenLoc>tableNameLoc)) {
						return -1;
					}
				} else {
					// it must be a SELECT statement.  look for the first FROM keyword, make sure the table name is after that from
					// this is NOT SQL parsing...  just a pretty good sign it is accessing the marked down table.
					unsigned int fromLoc = StringUtil::index_of_ignore_case(src, "from ");
					tableNameLoc = StringUtil::index_of_ignore_case(src.c_str() + fromLoc, curMD.detail);
					if (tableNameLoc!=src.length()) {
						return -1; // the table name appeared after "FROM", so let's guess its involved.
					}
				}
			}
			break;
		case MARKDOWN_SQL:
			// For SQL, look for an exact match INSIDE the SQL statement.  
			if (!fixedStr) {
				// we mangle the src string to be a nice straightforward SQL statement.
				fixedStr = true;
				std::replace(src.begin(), src.end(), '\n', ' ');
				StringUtil::replace_str(src, "  ", " ");

			}
			if (StringUtil::index_of_ignore_case(src, curMD.detail) != std::string::npos) {
				return -1; // this is the offending SQL. Don't run it.
			}

			break;
		case MARKDOWN_HOST:
			/// see if we are handling the "marked down" host.

			if ( (StringUtil::compare_ignore_case(host_name, curMD.detail)==0) || (StringUtil::compare_ignore_case(mark_host_name, curMD.detail)==0) ) {
				// our host is marked down!
				return -1;
			}
			break;
		case MARKDOWN_COMMIT:
			// For commit, look to see if we are handling the "marked down" module.
			StringUtil::trim(m_module_info);
			if (StringUtil::compare_ignore_case(m_module_info, curMD.detail)==0) {
				return -1; // This module is marked down, so do not allow commits.
			}
			break;
		case MARKDOWN_TRANS:
			// Do not run Transaction_start on anything.  If we want to constrain to a specific host it should be
			// the detail part, which is checked at top of method
			WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Aborting a transstart because of markdown");
			return -1; // marked down!
		default:
		case MARKDOWN_URL:
			break;

		}
	}
	return 0;
}

bool OCCChild::cal_log_command(int _cmd)
{
	switch (_cmd)
	{
	case OCC_BIND_NAME:
	case OCC_BIND_OUT_NAME:
	case OCC_ROWS:
	case OCC_COLS:
	case OCC_FETCH:
	case OCC_PREPARE_SPECIAL:
	case OCC_EXECUTE:
	case OCC_BACKTRACE:
	case OCC_CLIENT_INFO:
	case OCC_PREPARE:
	case OCC_PREPARE_V2:
	case OCC_COMMIT:
		return false;
	case OCC_ROLLBACK:
	case OCC_TRANS_PREPARE:
	case OCC_TRANS_START:
		return true;
	default:
		// fall through to the generic server behavior
		return Worker::cal_log_command(_cmd);
	}
}

std::string OCCChild::get_command_name(int _cmd)
{
	std::string name = Util::get_command_name(_cmd);
	if (name.empty())
		return Worker::get_command_name(_cmd);
	return name;
}


// discover database character set by pinging database
// while we are discovering database character set, client character set
// is set to US7ASCII which is also Oracle default
// expected db character set is WE8ISO8859P1 or UTF8
int OCCChild::get_db_charset(std::string& _charset)
{
	int rc;

	// don't even try it if we didn't construct successfully
	if (!constructor_success)
		return -1;

	// prepare a statement handle
	OCIStmt *stmthp = NULL;
	rc = OCIHandleAlloc((dvoid *) envhp, (dvoid **) &stmthp, OCI_HTYPE_STMT, (size_t) 0, NULL);
	if (rc != OCI_SUCCESS)
	{
		log_oracle_error(rc,"Failed to prepare a statement handle.");
		return -1;
	}

	CalTransaction cal_trans("ORACLE");
	cal_trans.SetName("profile");
	
	const char sql[] = "SELECT value, sys_context('USERENV','INSTANCE') AS my_inst, \
						sys_context('USERENV', 'DB_UNIQUE_NAME') AS db_uname, \
						sys_context('USERENV', 'SID') AS sid \
						FROM nls_database_parameters WHERE parameter = 'NLS_CHARACTERSET'";
	rc = OCIStmtPrepare(stmthp, errhp, (text *) const_cast<char*>(sql), strlen(sql), OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (rc != OCI_SUCCESS)
	{
		DO_OCI_HANDLE_FREE(stmthp, OCI_HTYPE_STMT, LOG_WARNING);
		log_oracle_error(rc, "Failed to prepare statement.");
		return -1;
	}

	// Define the output variable
	char value[128];
	OCIDefine *defnp[4] = {NULL};
	rc = OCIDefineByPos(
			stmthp, &defnp[0], errhp,
			1, (dvoid *) value, sizeof(value), SQLT_STR,
			NULL, NULL, NULL, OCI_DEFAULT);
	if (rc != OCI_SUCCESS)
	{
		DO_OCI_HANDLE_FREE(stmthp, OCI_HTYPE_STMT, LOG_WARNING);
		log_oracle_error(rc, "Failed to define output parameter [value].");
		return -1;
	}

	int local_id[2] = {0};
	rc = OCIDefineByPos(
			stmthp, &defnp[1], errhp,
			2, (dvoid *) local_id, sizeof(int), SQLT_INT,
			NULL, NULL, NULL, OCI_DEFAULT);

	if (rc != OCI_SUCCESS)
	{
		DO_OCI_HANDLE_FREE(stmthp, OCI_HTYPE_STMT, LOG_WARNING);
		log_oracle_error(rc, "Failed to define output parameter [local_id].");
		return -1;
	}
	
	char db_uname[128];
	rc = OCIDefineByPos(
			stmthp, &defnp[2], errhp,
			3, (dvoid *) db_uname, sizeof(db_uname), SQLT_STR,
			NULL, NULL, NULL, OCI_DEFAULT);
	if (rc != OCI_SUCCESS)
	{
		DO_OCI_HANDLE_FREE(stmthp, OCI_HTYPE_STMT, LOG_WARNING);
		log_oracle_error(rc, "Failed to define output parameter [db_uname].");
		return -1;
	}

	int sid = 0;
	rc = OCIDefineByPos(
			stmthp, &defnp[3], errhp,
			4, (dvoid *) &sid, sizeof(int), SQLT_INT,
			NULL, NULL, NULL, OCI_DEFAULT);

	if (rc != OCI_SUCCESS)
	{
		DO_OCI_HANDLE_FREE(stmthp, OCI_HTYPE_STMT, LOG_WARNING);
		log_oracle_error(rc, "Failed to define output parameter [sid].");
		return -1;
	}

	// Execute
	rc = OCIStmtExecute(svchp, stmthp, errhp, 1, 0, NULL, NULL, OCI_DEFAULT);
	if ((rc != OCI_NO_DATA) && (rc != OCI_SUCCESS))
	{
		DO_OCI_HANDLE_FREE(stmthp, OCI_HTYPE_STMT, LOG_WARNING);
		log_oracle_error(rc, "Failed to execute statement.");
		return -1;
	}

	// fill in the result
	_charset = value;
	m_connected_id = local_id[0];
	m_db_uname = db_uname;
	m_sid = sid;
	WRITE_LOG_ENTRY(logfile, LOG_INFO, "rac id: %d|db_unique_name: %s|sid: %d",
					m_connected_id, m_db_uname.c_str(), m_sid);


	// clean up
	if (!DO_OCI_HANDLE_FREE(stmthp, OCI_HTYPE_STMT, LOG_ALERT))
	{
		log_oracle_error(rc, "Failed to free statement handle.");
		return -1;
	}
	
	cal_trans.Completed(CAL::TRANS_OK);
	
	std::string tmp;
	StringUtil::fmt_int(tmp, m_connected_id);
	CalEvent e_rac_id("RAC_ID_START", tmp, CAL::TRANS_OK);
	CalEvent e_uname("DB_UNAME_START", m_db_uname, CAL::TRANS_OK);

	return 0;
}

int OCCChild::execute_query_with_n_binds( const std::string & _sql, const std::vector<std::string> &_names, const std::vector<std::string> & _values )
{
	int rc = 0;

	// do prepare
	if( m_session_var_stmthp == NULL )
	{
		rc = OCIHandleAlloc((dvoid *) envhp, (dvoid **) &m_session_var_stmthp, OCI_HTYPE_STMT, (size_t) 0, NULL);
		if (rc != OCI_SUCCESS)
		{
			log_oracle_error(rc,"Failed to prepare a statement handle.");
			return -1;
		}

		rc = OCIStmtPrepare(m_session_var_stmthp, errhp, (text *) const_cast<char*>(_sql.c_str()), (ub4) _sql.length(), OCI_NTV_SYNTAX, OCI_DEFAULT);
		if (rc != OCI_SUCCESS)
		{
			DO_OCI_HANDLE_FREE(m_session_var_stmthp, OCI_HTYPE_STMT, LOG_WARNING);
			log_oracle_error(rc, "Failed to prepare statement.");
			m_session_var_stmthp = NULL;
			return -1;
		}
	}

	//  do all bindings
	sb2 null_indicator = 0;
	OCIBind * bind_ptr = NULL;

	for ( unsigned int i = 0; i < _names.size(); i++ )
	{
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Binding: [%s]:%s", _names[i].c_str(), _values[i].c_str() );

		rc = OCIBindByName(
				m_session_var_stmthp,
				(OCIBind **) &bind_ptr,
				errhp,
				(text *) const_cast<char *>(_names[i].c_str()),
				_names[i].length(),
				const_cast<char *>(_values[i].c_str()),
				_values[i].length() + 1,
				SQLT_STR,
				(dvoid *) &null_indicator,
				(ub2 *) NULL,
				(ub2 *) NULL,
				(ub4) 0,
				(ub4 *) NULL,
				(ub4)OCI_DEFAULT);

		if (rc != OCI_SUCCESS)
		{
			DO_OCI_HANDLE_FREE(m_session_var_stmthp, OCI_HTYPE_STMT, LOG_WARNING);
			log_oracle_error(rc, "Failed to prepare statement.");
			m_session_var_stmthp = NULL;
			return -1;
		}

	}

	// Execute
	rc = OCIStmtExecute(svchp, m_session_var_stmthp, errhp, 1, 0, NULL, NULL, OCI_DEFAULT);
	if ((rc != OCI_NO_DATA) && (rc != OCI_SUCCESS))
	{
		DO_OCI_HANDLE_FREE(m_session_var_stmthp, OCI_HTYPE_STMT, LOG_WARNING);
		log_oracle_error(rc, "Failed to execute statement.");
		m_session_var_stmthp = NULL;
		return -1;
	}

	return 0;
}

// get DB release/version information
int OCCChild::get_db_information()
{
	// get oracle version
	char db_info[512];
	int rc = OCIServerVersion(srvhp, errhp, (OraText *)db_info, (ub4)sizeof(db_info), OCI_HTYPE_SERVER);
	if (rc != OCI_SUCCESS)
	{
		log_oracle_error(rc, "Failed to obtain the oracle version");
		return rc;
	}

	std::string tmp(db_info);
	unsigned int last_idx;

	last_idx = tmp.rfind( SERVER_DB_PREFIX );
	if (last_idx != std::string::npos)
	{
		m_db_version.clear();
		m_db_version.assign(tmp, last_idx + SERVER_DB_PREFIX.length(), std::string::npos);
		unsigned int space_idx = m_db_version.find( SPACE );
		if (space_idx != std::string::npos) {
			m_db_version.resize( space_idx );
		}
	}

	last_idx = tmp.rfind( SERVER_RELEASE_PREFIX );
	if (last_idx != tmp.length())
	{
		m_db_release.clear();
		m_db_release.assign(tmp, last_idx + SERVER_RELEASE_PREFIX.length(), std::string::npos);
		unsigned int space_idx = m_db_release.find( SPACE );
		if (space_idx != std::string::npos) { 
			m_db_release.resize( space_idx );
		}
	}

	return 0;
}
void OCCChild::process_pool_info(std::string& _client_info)
{
	std::string pool_name; 
	unsigned int start_idx = _client_info.rfind(POOLNAME_PREFIX);

	//FIXME: Once improper CAL instrumentation in CLIENT_INFO is rectified
	// delimiter needs to be changed to "&", since, data-value pairs in CAL messages are separated by '&'.
	if (start_idx != std::string::npos)
	{
		pool_name.assign(_client_info, start_idx + POOLNAME_PREFIX.length(), std::string::npos);
		unsigned int comma_idx = pool_name.find(COMMA);
		if (comma_idx != std::string::npos) {
			pool_name.resize(comma_idx);
		}
	}
	else
	{
		pool_name.assign("UNKNOWN");
	}

	//Start CLIENT_INFO transactions and log CLIENT_INFO event
	CalEvent e(CAL::EVENT_TYPE_CLIENT_INFO, pool_name, CAL::TRANS_OK);

	if (CalClient::is_poolstack_enabled())
	{
		start_idx = _client_info.rfind(POOLSTACK_PREFIX);
		if (start_idx != std::string::npos)
		{
			std::string parent_pool_stack = _client_info.substr(start_idx);
			unsigned int comma_idx = parent_pool_stack.find(COMMA);
			if (comma_idx != std::string::npos) {
				parent_pool_stack.resize(comma_idx);
			}
			StringUtil::replace_str(parent_pool_stack, POOLSTACK_PREFIX, "");
			//defaulting op name to CLIENT_INFO
			CalTransaction::SetParentStack(parent_pool_stack, std::string("CLIENT_INFO"));
		}

	}
	e.AddPoolStack();
}

int OCCChild::break_oci_call()
{
	CalTransaction cal_trans(CAL::EVENT_TYPE_ERROR);
	cal_trans.SetName("break_oci_call");
	int rc = OCIBreak(svchp, errhp);
	cal_trans.SetStatus((rc == OCI_SUCCESS)? CAL::TRANS_OK : CAL::TRANS_ERROR);
	if (rc != OCI_SUCCESS)
	{
		WRITE_LOG_ENTRY(logfile, LOG_WARNING, "OCIBreak failed");
		return -1;    
	}
	return 0;
}

/**
 *  @Brief   check if OCC is still servicing client's request and oracle has transaction in progress
 *
 *  @Return  true if in transaction or service, otherwise false.
 */
bool OCCChild::is_in_transaction()
{
	boolean txnInProgress = true;

	int rc = OCIAttrGet(authp, OCI_HTYPE_SESSION,
			&txnInProgress, (ub4 *)0,
			OCI_ATTR_TRANSACTION_IN_PROGRESS,
			errhp);
	if (rc == -1)
	{
		log_oracle_error(rc, "Failed to check if it is in transaction!");
		txnInProgress = true; // this is probably not necessary, but can't be 100% (it depends on oracle)
	}
	else
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "is_in_transaction(): result=%d", txnInProgress);

	return txnInProgress;
}

void OCCChild::check_OCI_SUCCESS_WITH_INFO(int& _rc, const char* _message, LogLevelEnum _log_level)
{
	if (_rc == OCI_SUCCESS_WITH_INFO)
	{
		// this is actually a success with "warning"
		std::string ora_text;
		// get the oracle error
		int ora_error = get_oracle_error(_rc, ora_text);
		char ora_event_name[64];
		sprintf(ora_event_name, "ORA-%05d", ora_error);

		std::ostringstream msg;
		// the oracle text includes the error number, so no need to print it separately
		msg << "m_err=OCI_SUCCESS_WITH_INFO for " << _message << " [" << ora_text << "]";
		WRITE_LOG_ENTRY(logfile, _log_level, "%s", msg.str().c_str());
		CalEvent e(CAL::EVENT_TYPE_WARNING, ora_event_name, CAL::TRANS_OK, msg.str());

		_rc = OCI_SUCCESS; // just in case some other place is doing != OCI_SUCCESS
	}
}

void OCCChild::set_orig_query_hash(const std::string& _query) {
	StringUtil::fmt_ulong(m_orig_query_hash, Util::sql_CAL_hash(_query.c_str()));
}

uint OCCChild::compute_scuttle_id(unsigned long long _shardkey_val)
{
	uint scuttle_id = 0;
	switch(m_sharding_algo)
	{
		case MOD_ONLY:
			scuttle_id = _shardkey_val % m_max_scuttle_buckets;
			break;
		case HASH_MOD:
			scuttle_id = HashUtil::MurmurHash3(_shardkey_val) % m_max_scuttle_buckets;
			break;
		default:
			break;
	}
	return scuttle_id;
}
