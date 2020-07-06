#ifndef _CalClientSession_h
#define _CalClientSession_h

#include "CalMessages.h"
#include <string>
#include <memory>


/**
 * This class simply manages a CalClient transaction for CAL.
 */ 
class CalClientSession
{
public:
	/**
	 * Constructor
	 */ 
	CalClientSession();

	/**
	 * Destructor
	 */ 
	~CalClientSession();

	/**
	 * This API is used to create the transaction for CalClient session and if the transaction
	 * is already active then it will complete the existing transaction and creates the new one
	 */ 
	void start_session(const std::string &_type, const std::string& _session_name);

	/**
	 * This API is used to create the transaction with type CLIENT.
	 */ 
	void start_session(const std::string &_session_name);

	/**
	 * This API is used to complete the CalClient transaction.
	 */ 
	void end_session();

	/**
	 * This API is used to set the status of the CalClient session.
	 */ 
	void set_status(const std::string &_status);
	
	/**
	 * This API is used to indicate whether the url session is active or not.
	 */ 
	bool is_session_active() const;

	/**
	 * Getter method of CalClient transaction.
	 */ 
	CalTransaction* get_session_transaction() const { return m_session_transaction.get(); }

	/**
	 * This API creates trace log id by combining timestamp and pool name.
	 */ 
	void generate_trace_log_id(std::string &_pool);

private:
	std::shared_ptr<CalTransaction> m_session_transaction;
	std::string m_status;
	std::string m_log_id;
};

#endif /* _CalClientSession_h */
