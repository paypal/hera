#ifndef __CALMESSAGES_H
#define __CALMESSAGES_H

#include <string>
#include <vector>
#include "CalTime.h"
#include "CalConst.h"
#include "CalClient.h"
#include "CalLog.h"
#include "utility/StringUtil.h"

#define CALVALUENOTSET "NotSet"

/**
 * This file contains all the CAL class declarations (CalTransaction, CalEvent,
 * CalHeartBeat) needed for CAL logging.
 */

class CalTransaction;

class CalActivity
{
 public:
	/**	
	 * CalActivity is an abstract base class. No construction allowed.
	 */
  
	/**
	 * Enumeration of flags used in CAL APIs
	 * CAL_PENDING - Flag to prevent CalMessages from being sent across but buffered till root transactions name is finalized.
	 * CAL_FINALIZE_ROOT_NAME - Flag to indicate name of root transaction is finalized. 
	 * CAL_SET_ROOT_STATUS - Flag to indicate that the status has to be set to root transaction.
	 */
	enum CalFlags 
	{
		CAL_DEFAULT			= 0,
		CAL_PENDING			= 1,
		CAL_FINALIZE_ROOT_NAME		= 2,
		CAL_SET_ROOT_STATUS		= 3,
	};

	/**
	 * This is used to send SQL hashing data over to the publisher.
	 * The format of the message sent to publisher is: $<SQL_HASH>"\t"<SQL_QUERY>
	 * @param sql_query SQL query to be hashed and also sent to publisher
	 * @return hash value of the query
	 */
	static ulong	SendSQLData (const std::string& sql_query);
	/**
	 * Setter method for CalActivity name. If activity is already completed, an alert cal event will be 
	 * sent to backend, but the set name operation will be a no-op.
	 * @param name Activity name to be set. Maximum length of name should be 127 chars, if more it will
	 * be truncated to 126 characters and a '+' will be appended in the end. 
	 */
	void SetName(const std::string& name);

	/**
	 * Setter method for CalActivity status
	 * @param status Status to be set for the activity. Should be non-empty string.
	 */
	void SetStatus(const std::string& status);

	/**
	 * Setter method for CalActivity status with return code
	 * @param status Status string to be set
	 * @param rc Return code that will be appended to the above status string passed in with a '.'
	 */
	void SetStatusRc(const std::string &status, long rc);

	/**
	 * Method to append name=value string to the existing CalActivity payload. This method frames a string of 
	 * the format "name=value". Then validates it to make sure it doesnt contain newline characters.
	 * If so, appends to the existing payload after appending a '&'. If newline characters 
	 * are present then it base64 encodes the "name=value" string and appends a string of the format 
	 * "__Base64Data__=<base64 encoded name=value>" to the existing payload. 
	 * If the CalActivity is already completed when this method is called, an "AlreadyCompleted" CalEvent is sent 
	 * and this method is a no-op.
	 * @param name Name parameter 
	 * @param value Value parameter pass in as a long value
	 */
	void AddData(const std::string& name, long  value);

	/**
	 * Method to append name=value string to the existing CalActivity payload. This method frames a string of 
	 * the format "name=value". Then validates it to make sure it doesnt contain newline characters.
	 * If so, appends to the existing payload after appending a '&'. If newline characters 
	 * are present then it base64 encodes the "name=value" string and appends a string of the format 
	 * "__Base64Data__=<base64 encoded name=value>" to the existing payload. 
	 * If the CalActivity is already completed when this method is called, an "AlreadyCompleted" CalEvent is sent 
	 * and this method is a no-op.
	 * @param name Name parameter 
	 * @param value Value parameter pass in as a string value
	 */
	void AddData(const std::string& name, const std::string& value);

	/**
	 * Method to append set of name=value pair strings to the existing CalActivity payload. This method frames a string of 
	 * the format "name=value". Then validates it to make sure it doesnt contain newline characters.
	 * If so, appends to the existing payload after appending a '&'. If newline characters 
	 * are present then it base64 encodes the "name=value" string and appends a string of the format 
	 * "__Base64Data__=<base64 encoded name=value>" to the existing payload. 
	 * If the CalActivity is already completed when this method is called, an "AlreadyCompleted" CalEvent is sent 
	 * and this method is a no-op.
	 * @param nameValuePairs This parameters should be of the form name=value&name=value&...&name=value
	 */
	void AddData(const std::string& nameValuePairs);

	/**
	 * Getter method for CalActivity status field
	 */
	std::string GetStatus() const ;

	/**
	 * Method to add "PoolStack" key parameter in the payload. 
	 * If pool stack is disabled, then this method is a no-op
	 */
	void AddPoolStack();
	

 protected:
	/**
	 * Constructors
	 */
	CalActivity(char message_class, const std::string& type);
	CalActivity(char message_class, const std::string& type, const std::string& name, const std::string& status,
				 const std::string& data);


	/**
	 * CalActivity non-virtual protected destructor. 
	 * We are not supporting polymorphic destruction of CAL objects. The destructor is purposefully made as 
	 * protected, to prevent users from deleting derived pointer through base pointer and causing memory leak. 
	 */
	~CalActivity();

	/**
	 * Internal static method to write the serialized cal message to 
	 * internal buffer and from there to handler
	 */
	static void WriteData (const std::string& data);

	/**
	 * Internal static method to flush the message buffer. 
	 * @param forceFlush - If true, will forcefully flush the buffer to handler. If false, will flush only if pending flag is not set
	 */
	static void FlushMessageBuffer (bool forceFlush=false);

	/**
	 * Internal logger utility method
	 */
	static void WriteTraceMessage(CalLogLevel _loglevel, int _errno, const std::string& _msg);

	
	/**
	 * Getter method to retrieve message buffer pointer
	 */
	static std::vector<std::string>* GetPendingMessageBuffer();


	/**
	 * Getter/Setter method for pending flag
	 */
	static bool IsPending ();
	static void SetPending (bool _pending_flag);
	
	/**
	 * Internal method to send serialized message of the object as it exists currently
	 * Depending on the current state of the object it will send either 'A', or 't' or 'T' or 'E' or 'H' message
	 */
	void SendSelf();


	/**
	 * Getter/Setter for completed flag of the cal activity object
	 */
	bool IsCompleted()  const 
	{
		 return mCompleted;
	}
	void SetCompleted() 
	{
		 mCompleted = true; 
	}

	/**
	 * Getter for type field 
	 */
	std::string GetType() const 
	{	
		return mType;
	} 

	/**
	 * Getter for name field 
	 */
	std::string GetName() const 
	{
		return mName;
	} 

	/**
	 * Validators for type, name, status and data fields. Returns a valid string conforming CAL guidelines
	 */
	std::string ValidateType(const std::string& _type) const;
	std::string ValidateName(const std::string& _name) const;
	std::string ValidateStatus(const std::string& _status) const;
	std::string ValidateData(const std::string& data) const;

	std::string FormatStatusWithRc(const std::string& _status, long rc);


	char		mTimeStamp[12];
	bool		mCompleted;
	char		mClass;
	std::string		mType;
	std::string		mName;
	std::string		mStatus;
	std::string		mData;

 private:
	/**	
	 * No default construction allowed
	 */
	CalActivity();

	/**
	 * No copy construction allowed
	 */
	CalActivity(const CalActivity& other);


	/**
	 * Static method to set lossy root transaction flag to cal handler
	 */
	static void SetLossyRootTxnFlag();

	/**
	 * Returns the message buffer size
	 */
	static int GetMaxMsgBufferSize();

	/**
	 * Internal method to write data to handler object
	 */
	static void WriteBufferToHandler(const std::string& buffer);

	/**
	 * No assignment allowed
	 */
	void operator=(const CalActivity& other);

	/**
	 * Initializes the activity object ith parameters passes
	 */	
	void Initialize (const std::string& type, const std::string& name, const std::string& status,
					const std::string& data);
	/**
	 * Notification function used when an attempt is made to modify a completed activity
	 */
	void ReportAlreadyCompletedEvent(const std::string& function, const std::string& data) const ;

	/**
	 * This friend class declaration is needed to allow CalTransaction to 
	 * access few protected methods of CalActivity via objects other than 
	 * 'this' object. Eg: See CompleteAnyNestedTransactions()
	 */
	friend class CalTransaction;
};


class CalEvent : public CalActivity
{
public:
	/**
	 * CalEvent constructor
	 * @param type Type of CalEvent to be set. Maximum length of type should be 127 chars, if more it will
	 * be truncated to 126 and a '+' will be appended in the end. 
	 * @param name Name of CalEvent to be set. Maximum length of name should be 127 chars, if more it will
	 * be truncated to 126 characters and a '+' will be appended in the end. 
	 * @param status Status to be set for the CalEvent
	 * @param data Payload string for the CalEvent
	 */
	CalEvent(const std::string& type, const std::string& name, const std::string& status, const std::string& data = "");

	/**
	 * CalEvent constructor
	 * @param type Type of CalEvent to be set. Maximum length of type should be 127 chars, if more it will
	 * be truncated to 126 characters and a '+' will be appended in the end. 
	 */
	CalEvent(const std::string& type);

	/**
	 * CalEvent destructor
	 * Calls Completed() routine and completes the CalEvent if not already completed explicitly by the user.
	 */
	~CalEvent();

	/**
	 * Setter method to set the type of CalEvent
	 * @param type Type parameter to be set for the CalEvent.Maximum length of type should be 
	 * 127 characters. If more, it will be truncated to 126 characters and a '+' will be appended in the
	 * end
	 */
	void SetType(const std::string& type);	

	/**
	 * Completed() method completes this CalEvent and sends it over the 
	 * wire as it currently exists. Once sent, the CalEvent is marked as completed and cannot be modified
	 * and resent. Calling it again will be a no-op.
	 */
	void Completed();

private:
	/**
	 * No default construction allowed
	 */
	CalEvent();

	/**
	 * No copy construction allowed
	 */
	CalEvent(const CalEvent& other);

	/**
	 * No assignment allowed
	 */
	void operator=(const CalEvent& other);


	/**
	 * Returns the transaction pointer in which this event was created
	 */
	CalTransaction* GetParent() const	
	{
		return mParent;
	}

	/**
	 * Transaction pointer in which this event is created
	 */
	CalTransaction*    mParent;
};

class CalHeartbeat : public CalActivity
{
public:
	/**
	 * CalHeartbeat constructor
	 * @param type Type of CalHeartbeat 
	 * @param name Name for CalHeartbeat 
	 * @param status Status to be set for the CalHeartbeat
	 * @param data Payload string for the CalHeartbeat
	 */
	CalHeartbeat(const std::string& type, const std::string& name, const std::string& status, const std::string& data = "");
	/**
	 * CalHeartbeat constructor
	 * @param type Type of CalHeartbeat 
	 * @param name Name for CalHeartbeat
	 * @param status Status to be set for the CalHeartbeat
	 * @param data Payload string for the CalHeartbeat
	 */
	CalHeartbeat(const std::string& type);

	/**
	 * Destructor
	 * Calls Completed() routine and completes the CalHeartbeat if not already completed explicitly by the user.
	 */
	~CalHeartbeat();

	/**
	 * Completed() method completes this CalHeartbeat and sends it over the 
	 * wire as it currently exists. Once sent, this CalHeartbeat  is marked as completed and cannot be modified
	 * and resent. Calling it again will be a no-op.
	 */
	void Completed();

private:
	/**
	 * No default construction allowed
	 */
	CalHeartbeat();

	/**
	 * No copy construction allowed
	 */
	CalHeartbeat(const CalHeartbeat& other);

	/**
	 * No assignment allowed
	 */
	void operator=(const CalHeartbeat& other);

	/**
	 * Returns the transaction pointer in which this heartbeat was created
	 */
	CalTransaction* GetParent() const
	{ 
		return mParent;
	}

	/**
	 * Transaction pointer in which this heartbeat is created
	 */
	CalTransaction*    mParent;
};


class CalTransaction : public CalActivity
{
 public:
	/**
	 * Utility Status class that can be used for generating the status format string.
	 */
	class Status
	{
	public:
		/**
		 * Constructors
		 */
		Status() : m_severity(CAL::TRANS_OK), m_module_name(CAL::MOD_NONE), m_system_err_code(CAL::SYS_ERR_NONE), m_module_rc() 
		{
		}

		Status(const std::string & severity, const std::string & module, const std::string & sys_err, const std::string &rc = std::string("0")) :
			m_severity(severity), m_module_name(module), m_system_err_code(sys_err), m_module_rc(rc) 
		{
		}

		Status(const std::string & severity, const std::string & module, const std::string & sys_err, int rc) :
			m_severity(severity), m_module_name(module), m_system_err_code(sys_err) 
		{
			StringUtil::fmt_int(m_module_rc, rc);
		}

		/**
		 * Destructor
		 */
		~Status() 
		{
		}
		
		/**
		 * std::string conversion methods
		 */
		std::string to_string(void) const;
		operator std::string() const 
		{
			 return to_string();
		}

	private:
		const std::string m_severity;
		const std::string m_module_name;
		const std::string m_system_err_code;
		std::string m_module_rc;
	};

	/**
	 * CalTransaction constructor
	 * @param type Type of CalTransaction to be set. Maximum length of type should be 127 chars, if more it will
	 * be truncated to 126 characters and a '+' will be appended in the end. 
	 */
	CalTransaction(const std::string& type);

	/**
	 * CalTransaction destructor
	 * Calls Completed() routine and completes the CalTransaction if not already completed explicitly by the user.
	 */
	~CalTransaction();


	/**
	 * Setters for SessionID and CPUTicks
	 */
	static void   SetSessionID(const std::string& appInfo, bool encrypt=true);
	static void   SetCPUTicks(const std::string& cpuTicks);

	/**
	 * CorrelationID and PoolStack APIs
	 */
	static void   SetCorrelationID();
	static void   SetCorrelationID(std::string corrID);
	static std::string GetCorrelationID();
	static std::string GetCurrentPoolInfo();
	static std::string GetPoolStack();
	static void SetParentStack(const std::string& clientpoolInfo, const std::string operationName="");
	static void SetOperationName(const std::string& opname, bool forceFlag=false);
	static std::string GetOperationName(); // PPSCR00833217 : Function the current operation name


	/**
	 * ** Please use it only when absolutely necessary and with necessary approvals from Architecture **.
	 * Method to append a name=value pair string to the payload of current root transaction.
	 * This should be used to add important information to be used for real time monitoring.
	 * Information added by this would be used by monitoring system built in EMS Program.
	 *
	 * This call gets delegated to one of AddData methods and hence behaves same in terms of validation, erron handling and output format.
	 * It would be a noop if there is no open root transaction.
	 * @param name Name parameter as a std::string value
	 * @param value Value parameter pass in as a std::string value
	 */
	static void AddDataToRoot(const std::string& name, const std::string& value);

	/**
	 * Return the pointer to transaction that is currently in scope
	 */
	static CalTransaction* GetCurrent(); 

	/**
	 * Sets the name of CalTransaction
	 * @param name CalTransaction name to be set. Maximum length of name should be 127 chars,
	 * if more it will
	 * be truncated to 126 characters and a '+' will be appended in the end.
	 * @param flag Flag to indicate whether name needs to be finalized later or is being finalized, 
	 * using CAL_FINALIZE_ROOT_NAME and CAL_PENDING resply. Other flags are ignored.
	 */
	void SetName(const std::string& name, CalFlags flag = CAL_DEFAULT);

	/**
	 * Sets the status of CalTransaction
	 * @param status Status string to be set
	 * @param flag Flag (CAL_SET_ROOT_STATUS) to indicate whether root transaction's status also needs to
	 * be set to the above status parameter. Other flags are ignored.
	 */
	void SetStatus(const std::string& status, CalFlags flag = CAL_DEFAULT);

	/**
	 * Sets the status of CalTransaction along with the return code appended to status string.
	 * @param status Status string to be set
	 * @param rc Return code to be appended to status string
	 * @param flag Flag (CAL_SET_ROOT_STATUS) to indicate whether root transaction's status also needs to
	 * be set to the above status parameter. Other flags are ignored.
	 */
	void SetStatusRc(const std::string& status, long rc, CalFlags flag = CAL_DEFAULT);

	/**
	 * Sets the duration of CalTransaction
	 * @param duration CalTransaction duration to be set. Ideally duration will be computed internally when the CAL transaction closed
	 * this API is exposed to overide the default duration to handle special needs. If the duration passed is negative, duration will be
	 * set to Minimun Duration i.e 0 and if the duration passed > Max Duration i.e 999999, duration will be set to Max Duration 999999
	 */
	void SetDuration(double duration);

	/**
	 * Completed() method completes this CalTransaction and sends it over the wire as it currently exists. 
	 * Once sent, this transaction cannot be modified. If already completed this method is a no-op.
	 * If calling Completed() on an outer transaction when the nested inner transactions are not yet 
	 * completed, this method will forcefully complete all the not yet completed nested transactions 
	 * before completing this transaction.
	 */
	void Completed();

	/**
	 * Completed() method which sets status and completes the transaction
	 */
	 void Completed(const std::string& status);

	/**
	 * Sets the status of Root CalTransaction
	 * @param status Status string to be set
	 */
	static void SetRootTransactionStatus(const std::string& status);

	/**
	 * Get the name of Root CalTransaction
	 */
	static std::string GetRootTransactionName();

 private:
	double m_duration;
	// No default construction allowed
	CalTransaction();
	// No copy construction allowed
	CalTransaction(const CalTransaction& other);

	static CalTransaction* GetRootTransaction();
	static void SetRootTransaction(CalTransaction* root);
	static std::string GetCPUTicks();
	static std::string GetSessionID();
	static void SetCurrent(CalTransaction* _transaction);

	void OnChildCreation();
	void SendSelf();
	
	void OnCompletion();
	void CompleteAnyNestedTransactions();
	void FlushStartTransaction();
	void FlushAtomicTransaction();
	void FlushEndTransaction();
	void PrepareStartOfTransactionMessage(std::string& buffer, char message_class);
	void PrepareEndOfTransactionMessage(std::string& buffer, char message_class);
	void AddAdditionalFieldsForRoot();
	void HandlePendingFlag();
	void HandleFinalizeRootNameFlag(const std::string& name);
	void AdddNameValuePair(std::string& fields, const std::string& name, const std::string& value);

	/**
	 * Returns if this transaction is root transaction or not
	 */
	bool IsRootTransaction() const 
	{
		return (GetParent() == NULL);	
	}

	bool IsStartSent() const;

	/**
	 * Gets the parent transaction pointer in which this transaction is created
	 */
	
	CalTransaction* GetParent() const	
	{
		return mParent;
	}

	/**
	 * No assignment allowed
	 */
	void operator=(const CalTransaction& other);

	CalMicrosecondTimer mTimer;

	/**
	 * Pointer to CalTransaction in which this CalTransaction object was created
	 */
	CalTransaction*    mParent;


	/**
	 * Friend class 
	 */
	friend class CalUtility;
	friend class CalEvent; 
	friend class CalHeartbeat; 
};

#endif
