#ifndef __CALSOCKETHANDLER_H
#define __CALSOCKETHANDLER_H

#include <stdint.h>
#include <string>
#include "CalHandler.h"
#include "CalRingBuffer.h"

class CalConfig;
class CalSocketHandler : public CalHandler
{
 public:
	CalSocketHandler (CalConfig* _sockConfig, CalLog* _logger);
	~CalSocketHandler ();
	void write_data (const std::string& data);
	void handle_new_root_transaction();
	void set_root_txn_lossy_flag(bool _value);
	int get_buffered_data_size()	{	return m_ringbuffer.used_capacity();	}
        
 protected:
	CalSocketHandler();
	// No default construction allowed
	CalSocketHandler(const CalSocketHandler& other);
	// No copy construction allowed
	void operator=(const CalSocketHandler& other);

	//Methods for caldaemon connection handling
	void complete_connection_to_caldaemon();
	void handle_disconnect();
	void handle_connection_in_progress();
	void close_connection();	
	

	//Methods operating over ring buffer
	void add_data_to_ring_buffer(const std::string& _data);
	void add_label_to_ring_buffer();
	void flush();
	void discard_ring_buffer_contents();

	//Tester class
	friend class CalSocketHandlerTester;


 private:

	class CalClientMessage
	{
	public:
		CalClientMessage(const std::string & _label);
		~CalClientMessage(){}

		//Methods for handling messages
		void construct_cal_message(const std::string& _buffer);
		/* This method sets the time value in the cal message
		   header whenever calclient connects to the caldaemon
		   *****Not for each message that it delivers 
		      to caldaemon*********
		   The scope of this methods is to support the 
		   wired protocol
		*/
		void set_connection_time_in_message();
		unsigned int get_length() const;
		//Tester class
		friend class CalSocketHandlerTester;

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

	enum CalClientSocketStatus
	{
		CONNECTION_IN_PROGRESS,
		CONNECTED,
		NOT_CONNECTED
	};

	class CalClientSocket
	{
	public:
		CalClientSocket(CalConfig* _sock_config, CalLog* _logger);
		~CalClientSocket();

		CalClientSocketStatus get_status() const;

		void close_socket();
		bool connect_to_caldaemon ();
		bool establish_cald_connection();

		int send_data(const char* msg, int size =0);
		//Tester class
		friend class CalSocketHandlerTester;

	private:
		int     m_socket;
		time_t  m_last_connection_attempt_time;
		CalLog* m_logger;
		CalConfig *m_config;

		CalClientSocketStatus m_status;

		void set_status(CalClientSocketStatus _status);
		void print_current_state() const;

		bool create_socket();
		bool set_socket_non_blocking();
		bool check_socket_connect_errors();

		bool has_reconnect_timeout_expired() const;
		bool has_connection_timeout_expired() const;
		bool has_timeout_expired() const;

	};

 	CalClientMessage m_msg;
	std::string      m_label;
	CalClientSocket  m_client_socket;
	CalRingBuffer    m_ringbuffer;
	bool 	         m_root_txn_lossy_flag;
};

#endif
