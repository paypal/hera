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
#ifndef __CALCLIENT_H
#define __CALCLIENT_H

#include <unistd.h>
#include <vector>
#include <string>

/**
 * This file contains the CalClient implementation class. CalClient users need to call the 
 * static CalClient::init() method to initialize CAL, before logging CAL messages
 */

//  forward declarations
class Config;
class CalConfig;
class CalHandler;
class CalTransaction;
class CalHandler;

class CalClient
{
 public:
	/**
	 * Method to initialize CalClient. This method should be invoked only once during startup of process.
	 * Calling this method multiple times is a no-op.
	 * After forking/daemonizing a process, this method should be called again to avoid reusingthe parent process initialization state.If init() is not called after fork/daemon, then the child process will share the connection to daemon with parent and hence there would be an issue in data quality of child process. 
	 * @param _config Pointer to a valid config object containing cal_client.cdb. Null config pointer will
	 *		  will result in CalClient not being initialized. 
	 *		  Ownership of _config object is passed on to CalClient. Please do not delete this object.
	 * @param _poolname Name of the pool for which CAL logging is done. _poolname overrides the poolname present
	 *		    present in cal_client.cdb
	 * @param _read_version_cdb Boolean to indicate whether version.cdb file is available in the current
	 *			    folder to be loaded for getting the build and product number.
	 */ 
	static bool init(Config *_config, const char* _poolname="", bool _read_version_cdb=true);

	/**
	 * Method to check if CalClient is initialized or not
	 * @return true is CalClient::init() has been called and CalClient is initialized, returns false otherwise
 	 */
	static bool is_initialized(); 

	/**
	 * Method to check if CAL logging is enabled or not
	 * @return true if CAL logging is enabled, else returns false
	 */
	static bool is_enabled();

	/** 
	 * Method to get the poolname that has been initialized 
	 * @return poolname initialized with CalClient
	 */
	static std::string get_poolname();

	/**
	 * Method to check if poolstack is enabled or not
	 * @return true if PoolStack is enabled, false otherwise
 	 */
	static bool is_poolstack_enabled();

	/**
	 * Method to set affix label which will be used by CalConfig
	 * @return void 
 	 */
	static void set_label_affix(const std::string label_affix);

 private:	
	/**
	 * CalClient singleton private constructor
	 */
	CalClient();
	
	/**
	 * No default copy construction allowed
	 */
	CalClient(const CalClient& _other);

	/**
	 * CalClient singleton instance getter method
	 * @return a valid initialized or non-initialized CalClient instance
	 */	 
	static CalClient* get_instance();

	/**
	 * Internal method to disable CAL logging. This function is registered to be invoked when the process exits, 
	 * via atexit(), to disable CAL logging when the process is exiting. Applications can also disable 
	 * CAL by calling CalLog::set_exit_flag(), when they are exiting.
	 */
	static void atexit_callback();  

	/**
	 * Internal method to initialize CalClient singleton object. 
	 * Once initialized, calling this method again is a NO-OP.
	 */
	void initialize(Config *_config, const char* _poolname, bool _read_version_cdb); 

	/**
	 * method for resetting CalClient object in the child process
	 */
	void reset(Config *_config, const char* _poolname, bool _read_version_cdb);

	/**
	 * No default assignment allowed
	 */
	void operator=(const CalClient& _other);


	/**
	 * Getter method for checking if this CalClient instance is initialized or not
	 */
	bool get_is_already_initialized() const
	{
		return m_is_already_initialized;
	}
	/**
	 * Getter method for obtaining CalConfig pointer instance
	 */
	CalConfig* get_config_instance() const 
	{
		return m_config;
	}

	/**
	 * Getter method for obtaining CalHandler pointer instance
	 */
	CalHandler* get_handler() const
	{
		return m_handler;
	}

	/**
	 * Getter method for obtaining current CalTransaction pointer
	 */
	CalTransaction* get_current_transaction() const
	{
		return m_current_transaction;
	}

	/**
	 * Setter method for current CalTransaction pointer
	 */
	void set_current_transaction(CalTransaction *_current_txn)
	{
		m_current_transaction = _current_txn;
	}

	/**
	 * Getter method for obtaining root CalTransaction pointer
	 */
	CalTransaction* get_root_transaction() const
	{
		return m_root_transaction;
	}

	/**
	 * Setter method for setting root CalTransaction pointer
	 */
	void set_root_transaction(CalTransaction *_root_txn)
	{
		m_root_transaction = _root_txn;
	}
	/**
	 * Getter method for obtaining pending flag
	 */
	bool get_pending_flag() const
	{
		return m_pending_flag;
	}

	/**
	 * Setter method for pending flag
	 */
	void set_pending_flag(bool _pending_flag)
	{
		m_pending_flag = _pending_flag;
	}

	/**
	 * Getter method for obtaining pending message buffer
	 */
	std::vector<std::string>* get_pending_message_buffer()
	{
		return &m_pending_message_buffer;
	}

	/**
	 * Getter method for obtaining session id
	 */
	std::string get_session_id() const
	{
		return m_session_id;
	}

	/**
	 * Setter method for setting session id
	 */
	void set_session_id(std::string _session_id)
	{
		m_session_id=_session_id;
	}

	/**
         * Flag indicating whether this CalClient instance is initialized or not
	 */
	bool	 	m_is_already_initialized;

	/**
	 * Pointer to CalConfig object, which stores all CalClient config information
	 */
	CalConfig*      m_config;

	/**
 	 * Pointer to CalHandler object, which sends the CAL data
	 */
	CalHandler*     m_handler;


	/**
	 * Pointer to current transaction in scope
	 */
	CalTransaction*    m_current_transaction;

	/**
	 * Pointer to the root transaction
	 */ 
	CalTransaction* m_root_transaction;

	/**
 	 * Boolean indicating if CAL_PENDING flag is set on root txn
	 */
	bool            m_pending_flag;

	/**
	 * In transit message buffer for CAL messages
	 */
	std::vector<std::string>  m_pending_message_buffer;

	/**
	 * Will be used by CalConfig to create label
	 */
	static std::string m_label_affix;

	std::string		m_session_id;

	static pid_t s_pid;

	// friends
	friend class CalLog;
	friend class CalActivity;
	friend class CalTransaction;
	friend class CalUtility;
	friend class CalSocketHandler;
	friend class CalClientBasicTester;
};

#endif
