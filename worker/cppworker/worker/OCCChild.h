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
#ifndef _OCCCHILD_H_
#define _OCCCHILD_H_

#include <oci.h>
#include <time.h>
#include <cal/CalClientSession.h>
#include "OCCStatementType.h"
#include "Markdown.h"
#include "Worker.h"
#include "SQLRewriter.h"
#include "ColumnInfo.h"
#include "OCCBind.h"
#include "OCCConfig.h"

class LogWriterBase;
class OCCBind;
class OCCDefine;
class OCCCachedResults;
class HBSender;
class RStatusHandler;
class StandByScnHandler;
struct RACNodeStatus;

//-----------------------------------------------------------------------------

#define OCC_CONFIG_FILENAME "occ.cdb"

//-----------------------------------------------------------------------------

struct StmtCacheEntry
{
	StmtCacheEntry() { clear(); }
	// destruction cleanup is done in OCCChild::free_stmt()

	void clear(void) { text.clear(); stmthp = NULL; type = occ::UNKNOWN_STMT; num_cols = 0; when = 0; num_exec = 0; defines = NULL; columns = NULL; version = occ::V1; has_datetime = false; }

	std::string    text;       // query text
	OCIStmt*  stmthp;     // statement handle
	occ::StatementType       type;       // the type of statement (largely the same as OCI_STMT_xxx)
	ub4       num_cols;   // number of columns selected
	time_t    when;       // last used
	unsigned long long    num_exec;   // execution count

	OCCDefine *defines;       //!< Used to handle OCIDefine's in extracting returned results from a SELECT

	std::vector<ColumnInfo> *columns;  //!< Returns the SELECT column list

	occ::ApiVersion version; // API client version
	bool has_datetime; // if is uses a DATE, TIMESTAMP, TIMESTAMP with TZ
};

//-----------------------------------------------------------------------------

struct OCIExecParams
{
	uint  iterations;
};

struct OCIFetchParams
{
	uint  rows_this_block;
};

union OCIFuncParams
{
	OCIExecParams  exec_params;
	OCIFetchParams fetch_params;
};

enum
{
	OCIR_OK     = 0,
	OCIR_ERROR  = -1,
	OCIR_FATAL  = -2,
};

//-----------------------------------------------------------------------------
// Here is the markdown related info.  
#define HOST_PREFIX "host"
#define TABLE_PREFIX "table"
#define SQL_PREFIX "sql"
#define URL_PREFIX "url"
#define TRANS_PREFIX "trans"
#define COMMIT_PREFIX "commit"

// not really a class, but I need to store these things in a vector.
struct MarkdownStruct
{
	MarkdownEnum type;
	std::string detail; // will either be a host name, a sql string, a table name, etc.
	std::string hostForDetail; // We can specify a host for table & sql markdowns here
	// or it can be empty, in which case it applies to all hosts.
};



class OCCChild : public Worker
{
	friend class ManualQueriesOCCChild;
private:
	// This is used by client tracking by CAL
	struct ClientSession
	{
		//CalClientSession m_db_txn;
		std::string m_query;
		//unsigned int m_db_txn_cnt;

	        ClientSession();
		~ClientSession();
		void start_db_txn();
		void end_db_txn();
	};

	struct TxnStartTime
	{
		TxnStartTime(OCCChild&);
		~TxnStartTime();
		OCCChild &m_child;
	};

	// ALL member vars
	std::string m_username;

	// Markdown structures; built in post_accept so that each connection gets the latest info.
	std::vector<MarkdownStruct> *current_markdowns;
	std::string host_name; // filled in at connection time with the environment variable "TWO_TASK"
	std::string mark_host_name; // filled in at connection time with the environment variable "MARK_HOST_NAME"
	std::string markdown_directory; // from config, the directory where markdown files are stored.
        MarkdownList mklist;  

	// what the default value for a NULL column should be represented as
	// this is kept as a copy because an individual session can change it and it
	// needs to be reset by the next connection
	std::string null_value;

	// Variables used by OCI
	OCIEnv* envhp;     //environment handle
	OCIServer* srvhp;  //server context handle
	OCIError* errhp;   //error handle
	OCIError* errhndl_batch; // error handle for individual batch
	OCIError* errhndl_batch2; // error handle for individual batch
	OCISvcCtx* svchp;  //service context handle
	OCISession* authp; //session handle
	OCITrans* transhp;  //!< Transaction Handle
	bool  attached;    // are we attached to the server?
	bool  has_session; // did we successfully begin the session?
	bool  m_oracle_init_called; // flag indicates connect() has been called or not.

	// use non-blocking execute?
	bool  use_nonblock;
	int   oracle_fd;
	long  ping_interval;

	// oracle heartbeat
	int              oracle_heartbeat_frequency;
	time_t           next_oracle_heartbeat_time;
	volatile bool    heartbeat_alarm_set; 

	// config to allow fetching config from db
	bool             enable_whitelist_test;
	// PPSCR00548601
	// client heartbeat, This goes out only if there is a long running SQL pending
	// This is used in sync/blocking mode to send out heartbeat on a separate thread
	// This is to work around an Oracle bug that happens with 8client/10server in async query mode
	// This mode is enabled through a cdb switch (enable_heartbeat_fix) and is meaningful only in 
	// blocking/sync SQL mode
	// cdb switch to be enabled only for occ pools using 8i client/10h server combination
	bool enable_hb_fix;  // on-off switch based on cdb
	HBSender* hb_sender; // handle to HBSender

	// PPSCR00377721 
	// enable_cache turns on global OCC process-wide sql stmt caching

	// our statment cache
	bool             enable_cache;
	StmtCacheEntry** stmt_cache;
	StmtCacheEntry*  cur_stmt;
	StmtCacheEntry   one_stmt;
	int              max_cache_size;
	int              max_statement_age;
	int              cache_size;
	int              cache_size_peak;
	ulong            cache_hits, cache_misses, cache_expires, cache_dumps;
	int              cache_expire_frequency;
	time_t           next_cache_expire_time;

	// cleans up statement whitespace before logging
	bool             enable_query_replace_nl;

	// our results cache
	OCCCachedResults* cur_results;
	bool              results_valid;

	// this is an area to put the define data
	char *           data_buf;
	sb2 **           indicator_bufs;
	ub2 **           str_size_bufs;

	// an array of OCIBind pointers for IN placeholders
	std::vector<std::shared_ptr<OCCBind> > *bind_array;
	std::vector<std::shared_ptr<OCCBindInOut> >  *out_bind_array;	//!< Used to handle OUT bind vars

	bool new_fetch;
	bool in_trans;
	// max number of rows that can be fetched at once for this statement
	unsigned int max_rows;
	// max rows that the server will allow to be fetched at once
	unsigned int max_fetch_block_size;
	// current row in the fetch
	unsigned int current_row;

	// what log level to use for backtraces (-1 == disabled)
	LogLevelEnum backtrace_log_level;

	// Are we in 2PC transaction?
	bool m_has_real_dml;
	bool m_in_global_txn;
	bool m_phase1_done;
	occ::TransRole m_trans_role;
	std::string m_curr_xid;
	unsigned int m_default_trans_timeout;
	std::string m_module_info;
	std::string m_dbhost_name;
	LogWriterBase *m_2pc_log;
	ClientSession m_client_session;

	// used by handle_command - are members to prevent multiple ctor/dtor
	// std::string client_info; // #PPSCR00797704 Moving to Worker class in Worker.h
	std::string command_info;
	std::string m_module_name;
	std::string m_action_name;
	std::string m_client_name;
	std::string m_log_format;
	std::string xid;
	std::string m_client_host_name;
	std::string m_client_exec_name;
	bool m_enable_session_variables;
	bool m_enable_session_flow;
	std::string m_db_version;
	std::string m_db_release;
	OCIStmt * m_session_var_stmthp;
	// oracle_lobprefetch_size turns on LOB prefetch size --- during BLOB/CLOB fetch, if BLOB/CLOB size is less than or equal to oracle_lobprefetch_size,
	// 														  then the data is returned in the same round trip as BLOB/CLOB locator.
	// Thus, this feature would save one round trip occured in OCILobRead2().
	// Note this feature should only be enabled between >=11.1g DB client and >=11.1g DB server
	ub4 oracle_lobprefetch_size;

	int m_last_exec_rc;
	std::string m_bind_data;
	std::string m_scuttle_id;
	int  m_restart_window;

	bool m_enable_sharding;
	std::string m_shard_key_name;
	std::string m_scuttle_attr_name;
	ShardingAlgo m_sharding_algo;
	int m_max_scuttle_buckets;
	SQLRewriter m_rewriter;
	bool m_sql_rewritten;
	bool m_enable_sql_rewrite;
	bool m_shard_key_value_type_string;
	std::string m_orig_query_hash;
	int bits_to_match; // Sampled Bind Hash logging. Sampling ratio (1:pow(2,bits_to_match)). Default 1 (Sampling ratio 1:2)
	unsigned long long int bit_mask; // Compute based on bits_to_match

public:
	// need to pass in a server socket which is already bound to the correct port
	// the child will accept on the socket
	OCCChild(const InitParams& _params);
	virtual ~OCCChild();

	// Oracle calls
	
	// connects to the database and does some initialization
	int connect(const std::string& db_username, const std::string& db_password);
	// disconnect is a stub at the moment
	int disconnect();

	// roll back the current transaction
	int rollback(const std::string &xid);
	// commit the current transaction
	int commit(const std::string &xid);
	// prepare a statement
	int prepare(const std::string& statement, occ::ApiVersion _version);
	// prepare a special statement
	int prepare_special(uint _statement_id);

	// bind a variable
	int bind(const std::string& name, const std::string& values, ub2* value_size, unsigned int value_max_size, unsigned int num, occ::DataType type);
	int bind_out(const std::string &name, occ::DataType type);

	// execute a prepared statement
	int execute(int& _cmd_rc);

	// fetch a block of rows, can be called multiple times
	unsigned long long fetch(const std::string& count);

	// return the number of rows processed so far
	int row_count();

	// return the number of columns in the select list
	int col_count();

	//! return the column headers in the select list
	int col_names(ub4 _num_cols, std::vector<ColumnInfo>* _cols);

	//! return the column types in the select list
	int col_info(ub4 _num_cols, std::vector<ColumnInfo>* _cols);

	int trans_start(const std::string &xid, unsigned int timeout, occ::TransRole role, occ::TransType type);

	int trans_prepare(const std::string &_line);

	// discover character set
	int get_db_charset(std::string& _charset);

	// send heartbeat to client, useful for long running queries in OCI blocking mode
	int send_heartbeat_ping();

	// execute a single query (*not* a select)
	int execute_query(const std::string& query);

	// get the DB version/release information
	int get_db_information();

	//break long oci call to prevent hang on long query
	int break_oci_call();
protected:
	
	// do idle processing: check cache expiration and send heartbeat
	virtual void on_idle(void);

	// processes one connection
	virtual int prepare_connection();
	virtual void cleanup_connection();
	virtual int handle_command(const int _cmd, std::string &_line);

	// handle signals
	virtual void sigfunc(int _sig);

    // PPSCR00797704: Modified Worker::get_client_info to
    // have similar definition. So, no need of overriding.
	// Override from Worker to return pertinent info
	// virtual std::string get_client_info() const { return client_info; }

	// Override from Worker to only CAL log certain commands
	virtual bool cal_log_command(int _cmd);

	// Override from Worker to name the commands
	virtual std::string get_command_name(int _cmd);

	// Moved following methods to protected for OCC PTB
	StmtCacheEntry* get_cur_stmt(void);

	int run_oci_func(int _func, const StmtCacheEntry *_stmt, const OCIFuncParams& _params, int *_oci_rc = NULL);

        // execute query with N bind variables
        int execute_query_with_n_binds( const std::string & _sql, const std::vector<std::string> &_names, const std::vector<std::string> & _values );

	// clears out the null indicators in the define array
	int clear_indicators();

	// sets up the define array with max capacity of num_rows
	int initialize_define_array(bool use_datetime);

	// sends a sql error back to the client
	int sql_error(int rc, const StmtCacheEntry *_stmt, const std::vector<int>* row_offset=NULL);

	// Adding following protected getters for OCC PTB
	const std::vector<std::shared_ptr<OCCBind> >* get_bind_array();
	const OCIError* get_errhp();
	const OCISvcCtx* get_svchp();
	
	std::string m_shardcfg_postfix;

private:

	int internal_update_maint_shm(RACNodeStatus);

	//returns pool name stack info from data string
	void process_pool_info(std::string& _client_info);

	// New markdown-related methods.
	void build_markdowns(); // reads files from markdown_directory
	int check_markdowns(MarkdownEnum type, const std::string &_src); // returns 0 if no problem, -1 if marked down.

	// set session variables for occ client
	int set_session_variables( void );

	// unset session variables for occ client
	int unset_session_variables( void );

	// execute session variables query
	int set_oracle_client_info( const std::string & _host_name, const std::string & _exec_name, const std::string & _module_name, const std::string & _action_name );

	// enable the cost-based optimizer
	int set_stored_outlines(void);

	// this will write an oracle error to the log file
	// str is included in the error information
	void log_oracle_error(
		int status, 
		const char* str, 
		LogLevelEnum level = LOG_ALERT);

	// an internal error with the OCC
	void occ_error(const char *str);

	// returns a string representation of an oracle error
	// stores into buffer (overwrites)
	int get_oracle_error(int rc, std::string& buffer);

	// use the #define DO_OCI_HANDLE_FREE()
	// it'll only be in the .cpp since this is private
	// frees oci resources and logs messages
	// if the hndlp is NULL, then do nothing and return true
	// returns false if error was detected
	bool real_oci_handle_free(dvoid *&hndlp, ub4 type, const char *name, LogLevelEnum level);

	// set or clear non-blocking mode
	int set_oci_nonblocking(bool _nonblock, const StmtCacheEntry *_stmt = NULL);
	int abort_oci_nonblocking(void);
	int find_oracle_fd(void);

	// resize socket send buffer
	int resize_oracle_fd_buffer();

	//dump temporary sql statement session cache
	void dump_session_cache();

	// statement cache functions
	void            free_stmt(StmtCacheEntry *_entry);
	void            cache_expire(bool _force_expire);
	void            cache_insert(StmtCacheEntry *_entry);
	StmtCacheEntry *            cache_find(const std::string& _query, occ::ApiVersion _version);

	// make sure we're still connected to oracle, if enough time has passed
	// since the last check
	void oracle_heartbeat();

	// utility
	int get_column_size(int *size, ub2 *type, ub4 pos, bool use_datetime);

	// placeholder binding
	//! common handler for OCI bind
	int internal_bind(OCCBind &binder, bool at_exec);
	//! Callback function for OCIBindDynamic to get input data
	sb4 ph_cb_in(dvoid *ictxp,
                 OCIBind *bindp,
                 ub4 iter,
                 ub4 index,
                 dvoid **bufpp,
                 ub4 *alenp,
                 ub1 *piecep,
                 dvoid **indpp);
	static sb4 placeholder_cb_in(dvoid *ictxp,
                                 OCIBind *bindp,
                                 ub4 iter,
                                 ub4 index,
                                 dvoid **bufpp,
                                 ub4 *alenp,
                                 ub1 *piecep,
                                 dvoid **indpp) { 
		if (the_child == NULL) return -1; 
		return static_cast<OCCChild *>(the_child)->ph_cb_in(ictxp, bindp, iter, index, bufpp, alenp, piecep, indpp);
	}
	//! Callback function for OCIBindDynamic to store output data
	sb4 ph_cb_out(dvoid *octxp,
                  OCIBind *bindp,
                  ub4 iter,
                  ub4 index,
                  dvoid **bufpp,
                  ub4 **alenpp,
                  ub1 *piecep,
                  dvoid **indpp,
                  ub2 **rcodepp);
	static sb4 placeholder_cb_out(dvoid *octxp,
                                  OCIBind *bindp,
                                  ub4 iter,
                                  ub4 index,
                                  dvoid **bufpp,
                                  ub4 **alenpp,
                                  ub1 *piecep,
                                  dvoid **indpp,
                                  ub2 **rcodepp) {
		if (the_child == NULL) return -1;
		return static_cast<OCCChild *>(the_child)->ph_cb_out(octxp, bindp, iter, index, bufpp, alenpp, piecep, indpp, rcodepp);
	}
	//! Callback function for failover
	sb4 cb_failover(void *svchp, void *envhp, void *fo_ctx, ub4 fo_type, ub4 fo_event);
	static sb4 c_cb_failover(void *svchp, void *envhp, void *fo_ctx, ub4 fo_type, ub4 fo_event) {
		return static_cast<OCCChild *>(fo_ctx)->cb_failover(svchp, envhp, fo_ctx, fo_type, fo_event);
	}
	int return_out_bind_vars(StmtCacheEntry *stmt);
	//!< Reset 2PC transaction state.
	int clear_2pc_state(void);
	//!< Associate an XID with the transaction handle
	int set_xid(const std::string &xid);
	//!< Deletes the status of a pending global transaction from Oracle's pending transaction table
	int trans_forget(void);
	// check with oracle if we're in transaction
	virtual bool is_in_transaction();
	virtual uint compute_scuttle_id(unsigned long long _shardkey_val);
	virtual uint compute_scuttle_id(std::string _shardkey_str_val);

	// handle OCI_SUCCESS_WITH_INFO
	void check_OCI_SUCCESS_WITH_INFO(int& _rc, const char* _message, LogLevelEnum _log_level);

	void set_orig_query_hash(const std::string& _query);

};

#endif
