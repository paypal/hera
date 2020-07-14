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
#ifndef OCCPROXY_EORMESSAGE_H
#define OCCPROXY_EORMESSAGE_H

#include <stdint.h>

class LogWriterBase;

class EORMessage
{
public:
	enum Status {
		FREE = 0,
		IN_TRANSACTION = 1,
		IN_CURSOR_NOT_IN_TRANSACTION = 2, /* not in transaction but not free because the cursor is open for ex */
		IN_CURSOR_IN_TRANSACTION = 3, /* not in transaction but not free because the cursor is open for ex */
		MORE_INCOMING_REQUESTS = 4, /* worker would be free, but it is not because there are more requests on the incomming buffer because
		 	 	 	 	 	 	 	 	they were pipelined by the client */
		BUSY_OTHER = 5, /* not used yet */
		RESTART = 6
	};
public:
	EORMessage();

	EORMessage(Status _status, const uint32_t _reqid, const std::string& _payload);

	void compose(std::string& _buffer);
	bool parse(const std::string& _buffer);

	void dump(LogWriterBase& out);
	void dump(std::string& out);

	Status get_status() const { return status; }
	const std::string& get_payload() { return payload; }
	uint16_t get_rq_id() const { return req_id; }

private:
	Status status;
	uint32_t req_id;
	std::string payload;
};

#endif //OCCPROXY_EORMESSAGE_H
