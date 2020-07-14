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
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <string.h>

#include "HBSender.h"
#include "OCCMuxCommands.h"
#include "worker/OCCChild.h"

#include "log/LogFactory.h"
#include "utility/encoding/NetstringReader.h"
#include "utility/FileUtil.h"

void* hb_worker(void* _arg);

HBSender* HBSender::the_hbsender = NULL;

HBSender::HBSender(OCCChild* _occ_child, int _timeout, pid_t _ppid, int _ctrl_fd) {
	m_occ_child = _occ_child;
	m_timeout = _timeout;
	m_is_enabled = false;
	m_rq_id = 0;
	m_ppid = _ppid;
	m_ctrl_fd = _ctrl_fd;
	m_reader = new NetstringReader(FileUtil::istream_from_fd(m_ctrl_fd));

	// not required really, since it is meaningful only if m_is_enabled is turned on
	// initialize it to a large value anywayl
	m_next_hb_time = time(NULL) + 8640000;

	logfile = LogFactory::get(DEFAULT_LOGGER_NAME); 

	the_hbsender = this;
}

HBSender::~HBSender() {
}


void HBSender::start() {
	pthread_t tid;

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "starting HBSender %d", m_ppid);
	int ret = pthread_create(&tid, NULL, hb_worker, (void*) this);

	ASSERT(!ret);
}

void HBSender::enable() {
	synchronize(this);
	m_is_enabled = true;
	m_next_hb_time = time(NULL) + m_timeout;
}

void HBSender::disable() {
	synchronize(this);
	m_is_enabled = false;
	m_next_hb_time = time(NULL) + 8640000;
}


int HBSender::get_snooze_time() {

	synchronize(this);
	return m_is_enabled ? ( m_next_hb_time - time(NULL) ) : m_timeout;
}


void HBSender::send_heartbeat_ping() {

	int now = time(NULL);

	synchronize(this);

	// must check we are still good 
	if (!m_is_enabled || ( now < m_next_hb_time) )
		return;

	// still enabled... OCC is still blocked on Oracle
	// Can not be disabled until we sent the heartbeat

	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, " %d long running query waiting on oracle. Sending client heartbeat", m_ppid);
	if (m_occ_child->send_heartbeat_ping() == -1 ) {
		// could not write heartbeat to client, shouldn't happen unless there is a bug in occmux
		// and we are hung on OCIStmtExecute or OCIStmtFetch
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, " %d could not send heartbeat to client while waiting on Oracle.", m_ppid);
		exit(0);
	}

	m_next_hb_time = now + m_timeout;
}


void HBSender::run() {
	while(1) {

		int snooze_time = get_snooze_time();

		if (snooze_time > 0 ) {
			if (wait_for_ctrl(snooze_time)) {
				if (!handle_ctrl())
					// we did not act because HBS is inactive, so the main thread will handle it.
					// we sleep here so that the HBS thread does not "spin" until the main thread handle
					// the ctrl message
					usleep(100000);
			}
			continue;
		}

		// time to send heartbeat
		send_heartbeat_ping();
	}
}

bool HBSender::wait_for_ctrl(int wait_time)
{
	struct timeval timeout;
	timeout.tv_sec = wait_time;
	timeout.tv_usec = 0;

	fd_set fdset;
	FD_ZERO(&fdset);
	FD_SET(m_ctrl_fd, &fdset);
	return (1 == select(m_ctrl_fd + 1, &fdset, 0, 0, &timeout));
}

bool HBSender::handle_ctrl()
{
	synchronize(this);
	std::string payload;
	int code = m_reader->read(&payload);
	if (code != CMD_INTERRUPT_MSG) {
			WRITE_LOG_ENTRY(logfile, LOG_ALERT, "Invalid control command: %d. Exiting", code);

			// we're in really bad state
			_exit(0);
	}
	if (payload.length() != 5) {
			WRITE_LOG_ENTRY(logfile, LOG_ALERT, "Invalid control command payload length: %d. Exiting", payload.length());

			// we're in really bad state
			_exit(0);
	}
	const char* data = payload.c_str();
	uint32_t rq_id = (uint32_t(uint8_t(data[1])) << 24) + (uint32_t(uint8_t(data[2])) << 16) + (uint32_t(uint8_t(data[3])) << 8) + uint8_t(data[4]);
	uint32_t my_rq_id = m_rq_id.load();
	WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Mux asks to abort existing work. mux_rq_ID = %u, wk_rq_ID = %u", rq_id, my_rq_id);
	if (rq_id != my_rq_id) {
		WRITE_LOG_ENTRY(logfile, LOG_WARNING, "Race interrupting SQL, mux_rq_ID is %u and wk_rq_ID is %u.", rq_id, my_rq_id);
		return false;
	}
	// capture the req_id which can be used during recover() so as to not recover different req_id due to race condition
	m_occ_child->set_id_to_abort(rq_id);
	uint16_t flags = data[0];
	if (m_is_enabled) {
		WRITE_LOG_ENTRY(logfile, LOG_DEBUG, "Breaking the OCI call !!!");
		if ( m_occ_child->break_oci_call() != 0 ) {
			WRITE_LOG_ENTRY(logfile, LOG_ALERT, " %d Error breaking out of OCI call. Exiting", m_ppid);

			// we're in really bad state
			_exit(0);
		}
		m_is_enabled = false;
		m_next_hb_time = time(NULL) + 8640000;
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "HBS disabled");
		m_occ_child->trigger_recovery(flags);
		return true;
	} else {
		WRITE_LOG_ENTRY(logfile, LOG_VERBOSE, "HBS disabled already, trigger recovery");
		m_occ_child->trigger_recovery(flags);
	}
	return false;
}

// entry point for hearbeat thread
void* hb_worker(void* _arg) {

	// mask all signals
	sigset_t   signal_mask;
	sigemptyset (&signal_mask);
	sigaddset(&signal_mask, SIGINT);
	sigaddset(&signal_mask, SIGTERM);
	sigaddset(&signal_mask, SIGALRM);
	sigaddset(&signal_mask, SIGUSR1);
	sigaddset(&signal_mask, SIGPIPE);
	int rc = pthread_sigmask (SIG_BLOCK, &signal_mask, NULL);
	ASSERT(!rc);

	HBSender* hb_sender = (HBSender*) _arg;
	hb_sender->run();

	return NULL;
}
