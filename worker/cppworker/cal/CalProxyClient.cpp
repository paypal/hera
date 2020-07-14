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
#include <unistd.h>
#include <netdb.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <string>
#include <sstream>
#include "CalProxyClient.h"
#include "log/LogFactory.h"
#include "CalTime.h"

const char *kConstCALEndOfLine     = "\r\n";
const char *kConstCALTab           = "\t";
const char *DEFAULT_ENVIRONMENT  = "PayPal";
const char *DEFAULT_IP           = "127.0.0.1";
unsigned short DEFAULT_PORT      = 1118;
const char ATOMIC_TRANSACTION    = 'A';

CalConnectionHandler::CalConnectionHandler(std::string host, unsigned short port)
{
	destination.sin_family = AF_INET;
	destination.sin_port	= htons (port);

	if (inet_aton (host.c_str(), &(destination.sin_addr)))
	{
		//Do we really this ?
		struct hostent * he = gethostbyname (host.c_str()); 
		if (he != NULL)
			memcpy (&(destination.sin_addr.s_addr), he->h_addr, he->h_length);
	}
}
void CalConnectionHandler::send_cal_msg(std::string label, std::string message)
{
	int dest_socket = ::socket (PF_INET, SOCK_STREAM, 0);
	if (dest_socket < 0)
	{
		LogFactory::get()->write_entry(LOG_WARNING, "%d %s" , errno, " CalProxyClient socket creation failed");
		return;
	}

	int flags = fcntl(dest_socket, F_GETFL, 0);
	if (flags == -1)
	{
		LogFactory::get()->write_entry(LOG_WARNING, "%d %s" , errno, " CalProxyClient fcntl failed");
		return;
	}
	fcntl(dest_socket, F_SETFL, flags | O_NONBLOCK);

	if (::connect (dest_socket, (struct sockaddr *) &destination, sizeof (destination)) < 0)
	{
		//Assuming connection would be completed before we get to write.
		if (errno != EINPROGRESS)
		{
			LogFactory::get()->write_entry(LOG_WARNING, "%d %s" , errno, " CalProxyClient connect failed");
			return;
		}
	}

 	CalMessageRaw cal_message;
	cal_message.construct_cal_message(label); 
	ssize_t size = 0;
	if ((size=::send (dest_socket, &cal_message, cal_message.get_length(), MSG_NOSIGNAL)) < 0)
	{
		LogFactory::get()->write_entry(LOG_WARNING, "%d %s" , errno, " CalProxyClient send label failed");
		return;
	}

	cal_message.construct_cal_message(message); 
	if ((size=::send (dest_socket, &cal_message, cal_message.get_length(), MSG_NOSIGNAL)) < 0)
	{
		LogFactory::get()->write_entry(LOG_WARNING, "%d %s" , errno, " CalProxyClient send message failed");
		return;
	}

	if (::close (dest_socket) < 0)
	{
		LogFactory::get()->write_entry(LOG_WARNING, "%d %s" , errno, " CalProxyClient close failed");
		return;
	}

	return;
}

CalBaseMessage::CalBaseMessage (const char _cal_class, const std::string _type, const std::string _name, const std::string _status, const std::string _payload)
	:cal_class(_cal_class), type(_type), name(_name), status(_status), payload(_payload)
{
	CalTimeOfDay::Now (time_stamp);
}
CalEventMessage::CalEventMessage (const std::string _type, const std::string _name, const std::string _status, const std::string _payload)
	:CalBaseMessage('E', _type, _name, _status, _payload) 
{
}
CalAtomicTransactionMessage::CalAtomicTransactionMessage (const std::string _type, const std::string _name, const std::string _status, const std::string _payload, const double _duration)
	:CalBaseMessage('A', _type, _name, _status, _payload)
{
	char durationString[256];
	CalMicrosecondTimer::PrivFormatDuration (durationString, _duration);
	duration = durationString;
}
std::string CalEventMessage::frame_msg_body()
{
	std::ostringstream os;
	os << cal_class << time_stamp << kConstCALTab << type << kConstCALTab << name << kConstCALTab << status << kConstCALTab 
		<< payload << kConstCALEndOfLine;
	return os.str();
}

std::string CalAtomicTransactionMessage::frame_msg_body()
{
	std::ostringstream os;
	os << cal_class << time_stamp << kConstCALTab << type << kConstCALTab << name << kConstCALTab << status << kConstCALTab 
		<< duration << kConstCALTab << payload << kConstCALEndOfLine;
	return os.str();
}
std::string CalLabel::frame_label ()
{
	char host[40];
	if (::gethostname (host, sizeof(host)) != 0)
		LogFactory::get()->write_entry(LOG_WARNING, " %d %s ", errno, " CalProxyClient gethostname failed");
	host[sizeof(host)-1] = '\0';

	time_t t = time(NULL);        
	struct tm st;
	localtime_r(&t, &st);
	std::ostringstream os;
	os << "SQLLog for " << pool_name << ":" << host << "\r\n";
	os << "Environment: " << environment << "\r\n";
	os << "Label: " << pool_name << "-" << version << "-" << build << "\r\n";
	char buff[256];
	sprintf(buff, "Start: %02d-%02d-%04d %02d:%02d:%02d\r\n", st.tm_mday, st.tm_mon+1, st.tm_year+1900,
		st.tm_hour, st.tm_min, st.tm_sec);
	os << buff << "\r\n";

	return os.str();
}
CalMessageRaw::CalMessageRaw()
{
	/* This doesnt look good. We are creating one object of CalClientMessage and expect it to be reused for both label and message.
	Only reasoning seems to be that we want to keep the time of connection same*/
	m_header.m_connect_time = htonl (time(NULL));
}

void CalMessageRaw::construct_cal_message(const std::string& _buffer)
{
	m_header.m_thread_id = 0;
	/* *  This gets called for every single CAL message, so it
	 *  needs a little optimization.  There may not be a
	 *  trailing NUL on the end of the string, but it
	 *  wouldn't get copied anyway.  We get the string
	 *  length, ensure that it ends with at least one \r\n
	 *  (although it could end up ending with two), and
	 *  then use memcpy (fast!) to ship it out.
	 */
	memset(m_body,0,sizeof(m_body));

	int len = _buffer.length();
	if (len > CAL_MESSAGE_BUFFER_SIZE) 
	{
		len = CAL_MESSAGE_BUFFER_SIZE;
	}
	
	memcpy(m_body, _buffer.c_str(), len);
	m_body[CAL_MESSAGE_BUFFER_SIZE-2] = '\r';
	m_body[CAL_MESSAGE_BUFFER_SIZE-1] = '\n';
	m_header.m_msg_len = htonl (len);
}

unsigned int CalMessageRaw::get_length () const
{
	return ntohl(m_header.m_msg_len)+sizeof(Header);
}
CalProxyClient::CalProxyClient(const std::string pool_name, std::string version, std::string build)
	:label(DEFAULT_ENVIRONMENT, pool_name, version, build), connection(DEFAULT_IP, DEFAULT_PORT)
{
}
void CalProxyClient::log(cal_class _cal_class, const std::string _type, const std::string _name, const std::string _status, const std::string _payload, const double _duration, const std::string _corr_id)
{
	std::ostringstream os;
	
	CalBaseMessage* message = NULL;
	switch (_cal_class)	
	{
		case ATOMIC_TRANSACTION:

			//We are adding corr_id as this is the only transaction and hence the root transaction. If we ever start creating other transactions, this could be a non root transaction and corr_id shouldnt be added everytime. 		
			os << "corr_id_=" << _corr_id << "&" << _payload;
			message = new CalAtomicTransactionMessage(_type, _name, _status, os.str(), _duration);
			break;

		case EVENT:
			message = new CalEventMessage(_type, _name, _status, _payload);
			break;
	}

	std::string cal_msg_body=message->frame_msg_body();
	std::string label_content = label.frame_label();
	connection.send_cal_msg(label_content, cal_msg_body);
	delete message;	
}
