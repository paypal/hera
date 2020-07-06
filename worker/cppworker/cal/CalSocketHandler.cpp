
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>
#include <string.h>

#include "CalSocketHandler.h"
#include "CalRingBuffer.h"
#include "CalLog.h"
#include "CalConfig.h"
#include "CalClient.h"

CalSocketHandler::CalSocketHandler (CalConfig* _config, CalLog* _logger)
  :CalHandler(_logger),
  m_msg(_config->get_label()),
  m_label((const char*)&m_msg,m_msg.get_length()),
  m_client_socket(_config,_logger),
  m_ringbuffer(_config->get_ring_buffer_size(),_logger),
  m_root_txn_lossy_flag (false)
{
	add_label_to_ring_buffer ();
}

CalSocketHandler::~CalSocketHandler ()
{
}

CalSocketHandler::CalClientMessage::CalClientMessage(const std::string & _label)
{
	set_connection_time_in_message();
	construct_cal_message(_label);
}

void CalSocketHandler::CalClientMessage::construct_cal_message(const std::string& _buffer)
{
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

unsigned int CalSocketHandler::CalClientMessage::get_length () const
{
	return ntohl(m_header.m_msg_len)+sizeof(Header);
}

void CalSocketHandler::CalClientMessage::set_connection_time_in_message()
{
	m_header.m_thread_id = 0;
	m_header.m_connect_time = htonl (time(NULL));
}

void CalSocketHandler::add_label_to_ring_buffer()
{
	m_ringbuffer.write_data(m_label.c_str(), m_label.length());
}

void CalSocketHandler::write_data(const std::string& _data)
{
	m_logger->write_trace_message(CAL_LOG_VERBOSE, 0, "%s",_data.c_str());

	complete_connection_to_caldaemon();
	add_data_to_ring_buffer(_data);
	flush();
}

void CalSocketHandler::complete_connection_to_caldaemon()
{
	if(m_client_socket.get_status() == NOT_CONNECTED)
	{
		handle_disconnect();
	}
	else
	if(m_client_socket.get_status() == CONNECTION_IN_PROGRESS)
	{
		handle_connection_in_progress();
	}
}

void CalSocketHandler::add_data_to_ring_buffer(const std::string& _data)
{
	m_msg.construct_cal_message(_data); 
	char* msg = (char*) &m_msg;
	int size =  m_msg.get_length();

	if(!m_ringbuffer.write_data(msg,size))
	{
		set_root_txn_lossy_flag(true);
	}
}

void CalSocketHandler::flush()
{
	if(m_client_socket.get_status() != CONNECTED)
		return;

	int size = m_ringbuffer.used_capacity();
	char buffer[size];
	m_ringbuffer.copy_data(buffer,size);

	int data_sent = m_client_socket.send_data(buffer,size);
	if(data_sent < 0)
	{
		m_logger->write_trace_message (CAL_LOG_WARNING, errno, "Closing connection: failed to send %d bytes. %d", size, errno);
		close_connection();
		set_root_txn_lossy_flag(true);
	}
	else
	{
		m_ringbuffer.remove_data(data_sent);
	}
}

void CalSocketHandler::handle_disconnect()
{
	if(m_client_socket.establish_cald_connection())
	{
		m_msg.set_connection_time_in_message();
	}
}

void CalSocketHandler::handle_connection_in_progress()
{
	/* This would fail in the following cases 
	1) caldaemon down
	2) time out during connection progress 
	In both of the cases it make sense to close connection */

	if(!m_client_socket.connect_to_caldaemon())
	{
		close_connection();
	}
}

void CalSocketHandler::close_connection ()
{
	m_client_socket.close_socket();
	discard_ring_buffer_contents();

	//Always fill the RB with the label, after discarding its data
	add_label_to_ring_buffer();
}

void CalSocketHandler::discard_ring_buffer_contents()
{
	m_ringbuffer.clear();
}

void CalSocketHandler:: handle_new_root_transaction()
{
	if (m_root_txn_lossy_flag)
	{
		flush();
		close_connection();

		set_root_txn_lossy_flag(false);

		complete_connection_to_caldaemon();
	}
}

void CalSocketHandler::set_root_txn_lossy_flag(bool _value)
{
	
	m_root_txn_lossy_flag = _value;
}

////////////////////////////////////
//
// CalClientSocket
//
////////////////////////////////////

 /*
 Give an initial value which is less than the current time, so that calclient always goes in connect mode during initialization 
 */

CalSocketHandler::CalClientSocket::CalClientSocket(CalConfig* _config, CalLog* _logger)
:m_last_connection_attempt_time(0),
 m_logger(_logger),
 m_config(_config),
 m_status(NOT_CONNECTED)
{
	establish_cald_connection();
}

CalSocketHandler::CalClientSocket::~CalClientSocket()
{
	close_socket();
}

void CalSocketHandler:: CalClientSocket::set_status(CalClientSocketStatus _status)
{
	m_status = _status;
}

void CalSocketHandler:: CalClientSocket::print_current_state() const
{
	switch(m_status)
	{
	case CONNECTED:
		m_logger->write_trace_message(CAL_LOG_VERBOSE, 0, "Changed status to CONNECTED");
		break;
	case NOT_CONNECTED:
		m_logger->write_trace_message(CAL_LOG_VERBOSE, 0, "Changed status to NOT_CONNECTED");
		break;
	case CONNECTION_IN_PROGRESS:
		m_logger->write_trace_message(CAL_LOG_VERBOSE, 0, "Changed status to CONNECTION_IN_PROGRESS");
		break;
	default:
		m_logger->write_trace_message(CAL_LOG_VERBOSE, 0, "UNKNOWN-STATE");
		break;
	}
}

CalSocketHandler::CalClientSocketStatus CalSocketHandler::CalClientSocket::get_status() const
{
	return m_status;
}

bool CalSocketHandler::CalClientSocket::has_reconnect_timeout_expired() const 
{
	if(has_timeout_expired())
	{
		return true;
	}
	return false;
}

bool CalSocketHandler::CalClientSocket::has_connection_timeout_expired() const 
{
	if(has_timeout_expired())
	{
		return true;
	}
	return false;
}

bool CalSocketHandler::CalClientSocket::has_timeout_expired() const 
{
	if (time(NULL) >= (m_last_connection_attempt_time+ m_config->get_socket_connect_time()))
	{
		return true;
	}
	return false;
}

bool CalSocketHandler::CalClientSocket::establish_cald_connection()
{
	if(!has_reconnect_timeout_expired()) 
		return false;

	// create socket
	if(!create_socket())
		return false;

	if(!set_socket_non_blocking())
	{
		close_socket();
		return false;
	}

	m_last_connection_attempt_time  = time(NULL);
	if(!connect_to_caldaemon())
	{
		close_socket();
		return false;
	}
	return true;
}

bool CalSocketHandler::CalClientSocket::create_socket()
{
	int s = ::socket (PF_INET, SOCK_STREAM, 0);
	if (s < 0)
	{
		return false;
	}
	m_socket = s;
	return true;
}

bool CalSocketHandler::CalClientSocket::set_socket_non_blocking()
{
	int flags = fcntl(m_socket, F_GETFL, 0);
	if (flags == -1)
	{
		return false;
	}
	fcntl(m_socket, F_SETFL, flags | O_NONBLOCK);
	return true;
}

bool CalSocketHandler::CalClientSocket::connect_to_caldaemon()
{
 	sockaddr_in addr = m_config->get_siteview_addr();
	if (::connect (m_socket, (struct sockaddr *) &addr, sizeof (addr)) < 0)
	{
		return check_socket_connect_errors();
	}

	set_status(CONNECTED);
	return true;
}

bool CalSocketHandler::CalClientSocket::check_socket_connect_errors()
{
	bool rt = false;
	switch(errno)
	{
	case EINPROGRESS:
		set_status(CONNECTION_IN_PROGRESS);
		rt = true;
		break;
	case EISCONN: 
		set_status(CONNECTED);
		rt = true;
		break;
	case EALREADY:
		if(!has_connection_timeout_expired())
		{
			set_status(CONNECTION_IN_PROGRESS);
			rt = true;
		}
		break;
	default:
		break;
	}
	return rt;
}

int CalSocketHandler::CalClientSocket::send_data(const char *msg, int size)
{
	if(get_status() != CONNECTED)
		return 0;

	int sent = ::send (m_socket, msg, size, MSG_NOSIGNAL);
	if( sent < 0)
	{
		int rt;
		switch(errno)
		{
		case EAGAIN:
			rt = 0;
			break;
		default:
			rt = -1;
			break;
		}
		return rt;
	}

	m_logger->write_trace_message (CAL_LOG_VERBOSE, 0, "Sent attempted %d bytes ,Sent %d bytes", size,sent);
	return sent;
}

void CalSocketHandler::CalClientSocket:: close_socket()
{
	if(get_status() != NOT_CONNECTED)
	{
		::close (m_socket);
		set_status(NOT_CONNECTED);
	}
}
