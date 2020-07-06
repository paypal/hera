#ifndef __CALHANDLER_H
#define __CALHANDLER_H

#include <string>

#define CAL_MESSAGE_BUFFER_SIZE 4096

class CalLog;
class CalHandler
{
public:
	CalHandler(CalLog* _logger) { m_logger = _logger;} 
	virtual ~CalHandler() { }

	virtual void write_data (const std::string& data) = 0;
	virtual void close_connection (){}
	virtual void handle_new_root_transaction(){}
	virtual void set_root_txn_lossy_flag(bool _value){} 
	virtual int get_buffered_data_size()	{	return 0;	}

protected:
	CalLog* m_logger;

private:
	void operator=(const CalHandler& _other); // not allowed
};

#endif
