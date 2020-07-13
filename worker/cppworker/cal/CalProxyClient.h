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
#ifndef __CALPROXYCLIENT_H
#define __CAPROXYLCLIENT_H

#include <stdint.h>
#include <netinet/in.h>
#include <string>

#define CAL_MESSAGE_BUFFER_SIZE 4096

class CalConnectionHandler
{
	public:
		CalConnectionHandler (std::string host, unsigned short port);
		void send_cal_msg(std::string label, std::string message);
	private:
		sockaddr_in destination;
};

class CalLabel
{
	public:
		CalLabel (const std::string& _environment, const std::string& _pool_name, const std::string& _version, const std::string& _build)
			:environment(_environment), pool_name(_pool_name), version(_version), build(_build)
		{
		}
		std::string frame_label ();
	private:
		std::string environment;
		std::string pool_name;
		std::string version;
		std::string build;

};
class CalBaseMessage
{
	public:
		CalBaseMessage (const char _cal_class, const std::string _type, const std::string _name, const std::string _status, const std::string _payload);
		virtual ~CalBaseMessage() { }
		virtual std::string frame_msg_body () = 0;
	protected:
		char cal_class;
		char time_stamp[12];
		std::string type;
		std::string name;
		std::string status;
		std::string payload;
};
class CalEventMessage:public CalBaseMessage
{
	public:
		CalEventMessage (const std::string _type, const std::string _name, const std::string _status, const std::string _payload);
		std::string frame_msg_body ();
};
class CalAtomicTransactionMessage:public CalBaseMessage
{
	public:
		CalAtomicTransactionMessage (const std::string _type, const std::string _name, const std::string _status, const std::string _payload, const double _duration);
		std::string frame_msg_body ();
	protected:
		std::string duration;
};
class CalMessageRaw
{
	public:
		CalMessageRaw();
		unsigned int get_length () const;
		void construct_cal_message(const std::string& _buffer);
	private:
		struct Header
		{
			uint32_t	m_thread_id; //  unused
			uint32_t	m_connect_time; //  unused
			uint32_t	m_msg_len;
		};
		Header m_header;
		/* Maximum PayLoad size of the CalMessage supported
		   is CAL_MESSAGE_BUFFER_SIZE(4096 bytes ) */
		char m_body[CAL_MESSAGE_BUFFER_SIZE];
};
// A simple cal client which will help in log messages for other applications. Intended primarily for server applications to log messages for its mobile clients.
// A sample usage would be to create a CalProxyClient object and invoke log method.
// 		CalProxyClient proxy_client("ppmobile", "107.0","1234567");
// 		proxy_client.log(CalProxyClient::ATOMIC_TRANSACTION, "URL", "Payment", "-1", "m_country=US&m_currency&err_code=1234", 1.5, "decd5d174464d");
// This willl put this CAL message in ppmobile pool
// NOTE: This client doesn't work on cal_client.cdb at all. It is enabled and connects to 127.0.0.1:1118. 
//       Current version makes a connection for every log message and it wouldnt perform well for too many number of messages.
class CalProxyClient
{
	public:
		CalProxyClient(const std::string pool_name, std::string version, std::string build);
		enum cal_class
		{
			ATOMIC_TRANSACTION,
			EVENT
		} ;
		void log(cal_class _cal_class, const std::string type, const std::string name, const std::string status, const std::string payload, const double duration=0.0, const std::string _corr_id="");
	private:
		CalLabel label;	
		CalConnectionHandler connection;	
};

#endif
