#ifndef _CalURLSession_h
#define _CalURLSession_h

#include "CalClientSession.h"
#include <string>

/**
 * This class simply manages a top-level URL transaction for CAL.
 * It is shared by both admin and webscr.
 */ 
class CalURLSession
{
public:
	/**
	 * This API is used to create the root URL transaction and if the transaction is already
	 * active then it will complete the existing transaction and creates the new one. 
	 */ 
	static bool start(const char* poolname=NULL, const char* ppppname=NULL);

	/**
	 * This API is used to complete the root URL transaction.
	 */ 
	static void end();

	/**
	 * This API is used to indicate whether the url session is active or not.
	 */ 
	static bool is_active();

	/**
	 * This API is used to set the status of the URL session.
	 */ 
	static void set_status(const std::string &_status);

	/**
	 * Getter Method for URL transaction.
	 */ 
	static CalTransaction* get_url_transaction();

private:
	/**
	 * This method is used to get URL session.
	 */ 
	static CalClientSession* get_cal_client_session();
};

#endif /* _CalURLSession_h */
