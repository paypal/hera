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
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <sstream>
#include <memory>

#include "log/LogFactory.h"
#include "config/CDBConfig.h"
#include "config/Config.h"
#include "config/MultiConfig.h"
#include "config/OPSConfig.h"
#include "worker/OCCChild.h"
#include "worker/WorkerApp.h"
#include "worker/WorkerFactory.h"

// environment vars
#define ENV_MODULE "HERA_NAME"
#define ENV_LOG_PREFIX "logger.LOG_PREFIX"
#define ENV_CAL_CLIENT_SESSION "CAL_CLIENT_SESSION"
#define ENV_DB_HOSTNAME "DB_HOSTNAME"
#define ENV_OPSCFG_FILE "OPS_CFG_FILE"
#define ENV_MUX_START_TIME_SEC "MUX_START_TIME_SEC"
#define ENV_MUX_START_TIME_USEC "MUX_START_TIME_USEC"
#define ENV_USERNAME "username"
#define ENV_PASSWORD "password"

const char *g_log_path;
const char *g_cfg_path;

WorkerApp* WorkerApp::instance = 0;
WorkerApp::WorkerApp(const WorkerFactory& _factory):
		pin(0),
		logger(0)
{
	std::string cfg_filename = g_cfg_path;
	cfg_filename += "/";
	cfg_filename += _factory.get_config_name();
	config = std::unique_ptr<CDBConfig>(new CDBConfig(cfg_filename));

	std::string cval;
	if (config->get_value("child_failure_delay", cval))
		failure_delay = StringUtil::to_uint(cval);
	else
		failure_delay = DEFAULT_FAILURE_DELAY;
	init_logs();
}

WorkerApp::~WorkerApp()
{
}

void WorkerApp::AtExit()
{
	delete instance;
	instance = 0;
	LogFactory::get(DEFAULT_LOGGER_NAME)->write_entry(LOG_VERBOSE, "AtExit()");
}

int WorkerApp::execute(const WorkerFactory& _factory)
{
	// TODO: see how to do it in mux
	// for some reason syscall.ForkExec in workerclient.go leaves more than 5 descriptors open
	int fd = 5;
	int fdlimit = sysconf(_SC_OPEN_MAX);

	// use one sys call [poll] to find fd's to close
	struct pollfd *fds = (struct pollfd*)malloc(sizeof(struct pollfd) * (fdlimit-fd));
	for (int i=fd; i<fdlimit; i++) {
		 fds[i-fd].fd = i;
		 fds[i-fd].events = 0;
		 fds[i-fd].revents = 0; // look for POLLNVAL == 0
	}
	poll(fds, fdlimit-fd, 0);
	int strayCnt = 0;
	int strayFd = -1;
	for (int i=fd; i<fdlimit; i++) {
		if (0 == fds[i-fd].revents & POLLNVAL) {
			close(i);
			strayFd = i; // logs aren't initialized, save value
			strayCnt++;
		}
	}
	free(fds);




	if (instance)
		return -1;
	try
	{
		instance = new WorkerApp(_factory); // initializes logs
		if (strayFd != -1) {
			LogFactory::get(DEFAULT_LOGGER_NAME)->write_entry(LOG_WARNING, "Stray fd %d, total %d closed, earlier at WorkerApp::execute start", strayFd, strayCnt);
		}
	}
	catch (const PPException& ex)
	{
		fprintf(stderr, "Exception: %s", ex.get_string().c_str());
		sleep(DEFAULT_FAILURE_DELAY);
		return -1;
	}
	// it would be nice to disconnect from Oracle properly, however if at_exit handler is called while
	// in an OCI call it could cause a deadlock
	//atexit(WorkerApp::AtExit);
	try
	{
		instance->initialize(_factory);
		int ret = instance->main();
		delete instance;
		return ret;
	}
	catch (const PPException& ex)
	{
		int delay = instance->failure_delay * ( 500000 + (rand()*1000000LL)/RAND_MAX );
		LogFactory::get(DEFAULT_LOGGER_NAME)->write_entry(LOG_ALERT, "Sleep %d us after Exception: %s", delay, ex.get_string().c_str());
		// most likely DB is down... retry after some time
		usleep(delay);
		return -1;
	}
}

int WorkerApp::main()
{
	failure_delay = 1;
	worker->main();
	return 0;
}

void WorkerApp::initialize(const WorkerFactory& _factory)
{
	InitParams _params;
	_params.config_filename = _factory.get_config_name();
	_params.module = _factory.get_server_name();
	_params.server_name = _factory.get_server_name();
	_params.mux_start_time_sec = getenv(ENV_MUX_START_TIME_SEC);
	_params.mux_start_time_usec = getenv(ENV_MUX_START_TIME_USEC);
	_params.client_session = getenv(ENV_CAL_CLIENT_SESSION);
	_params.db_hostname = getenv(ENV_DB_HOSTNAME);
	_params.module = getenv(ENV_MODULE);
	const char *username = getenv(ENV_USERNAME);
	if (username == NULL) { // std::string seg faults on null
		username="userNotConfigured";
		logger->write_entry(LOG_ALERT,"user not configured envvar %s", ENV_USERNAME);
	}
	_params.db_username = username;
	const char *password = getenv(ENV_PASSWORD);
	if (password == NULL) {
		password="passwordNotConfigured";
		logger->write_entry(LOG_ALERT,"password not configured envvar %s", ENV_PASSWORD);
	}
	_params.db_password = password;
	worker = _factory.create(_params);
}

void WorkerApp::init_logs()
{
	uint   log_level;
	std::string cval;

	// get the log level
	if (config->get_value("log_level", cval))
		log_level = StringUtil::to_uint(cval);
	else
		log_level = LOG_DEBUG;


	// open file for output
	if (!config->get_value("log_file", cval))
		cval = "/dev/stderr";
	if (cval[0] != '/') {
		cval = std::string(g_log_path) + "/" + cval;
	}

	// set up logfile
	Logger* _logger = LogFactory::get<Logger>(DEFAULT_LOGGER_NAME, true/*create*/);
	_logger->set_stream(cval);
	_logger->set_log_level(static_cast<LogLevelEnum>(log_level));
	
	const char *log_prefix = getenv(ENV_LOG_PREFIX);
	if (log_prefix == NULL) {
		log_prefix = "bad env logger.LOG_PREFIX";
	}
	std::string worker_type = log_prefix;

	std::stringstream ss;
	ss << "%U %p %t [" << worker_type << "]: %s\n";

	_logger->set_format(ss.str().c_str());

	logger = _logger;
	logger->write_entry(LOG_INFO,"starting");

	// This config will contain all CAL configuration information.
	MultiConfig* cal_cfg = new MultiConfig;
	// Add CAL-specific configuration
	CDBConfig *cal_client_cfg = new CDBConfig(std::string(g_cfg_path)+"/cal_client.cdb");
	cal_cfg->add_config(cal_client_cfg);
	// Add version information
	CDBConfig *version_cfg = new CDBConfig(std::string(g_cfg_path)+"/version.cdb");
	cal_cfg->add_config(version_cfg);

	CalClient::init (cal_cfg);
	logger->set_enable_cal(true);

	// note: use OPSConfig after cal is inited
	const char *opscfgFile = getenv(ENV_OPSCFG_FILE);
	if (opscfgFile == NULL) {
		opscfgFile = "opscfgFileNotCfg";
		logger->write_entry(LOG_ALERT,"opscfg not configured %s",ENV_OPSCFG_FILE);
	}
	OPSConfig& opscfg = OPSConfig::create_instance(opscfgFile);
	if (opscfg.get_value("log_level", cval)) {
		log_level = StringUtil::to_uint(cval);
	}
	logger->set_log_level(static_cast<LogLevelEnum>(log_level));
}
