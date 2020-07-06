
#include <string.h>
#include <sstream>

#include "CalMessages.h"
#include "CalClient.h"
#include "CalHandler.h"
#include "CalConfig.h"
#include "CalUtility.h"
#include <utility/fnv/fnv.h>
#include <utility/encoding/base64.h>
#include <utility/StringUtil.h>

const char *kCALUnset         = "unset";
const char *kCALUnknown       = "U";
const char *kCALZero          = "0";
const char *kCALEndOfLine     = "\r\n";
const char *kCALSetName       = "SetName";
const char *kCALAddDataPairs  = "AddDataPairs";
const char *kCALBase64Data    = "__Base64Data__";
const char *kCALAmpersand     = "&";
const char *kCALEquals        = "=";

const char *kCALTab           = "\t";
const char *kCALDollar        = "$";

const char *kCALPeriod        = ".";
const char *kCALMessageLogPrefix = "CalMessageLog:";
const char *kCALTypeBadInstrumentation = "BadInstrumentation";
const char *kCALNameAlreadyCompleted = "AlreadyCompleted";
const char *kCALNameCompletingParent = "CompletingParentWithUncompletedChild";

const char kCALClassStartTransaction   = 't';
const char kCALClassEndTransaction     = 'T';
const char kCALClassAtomicTransaction  = 'A';
const char kCALClassEvent   = 'E';
const char kCALClassHeartbeat   = 'H';

const unsigned long kCALMaxNameLength = 127;	//PPSCR01148980
const unsigned long kCALMaxTypeLength = 127;	//PPSCR01148980
const unsigned int kCALClientThreadId = 0;
const unsigned long kCALMaxMessageBufferSize = 300;


/**
 * CALCLIENT RE-IMPLEMENTATION NOTE: 
 * Due to the existing improper usage of aleady exposed CAL APIs, in paypal codebase,
 * the re-design of CalActivity/CalEvent/CalTransaction/CalHeartbeat classes dont 
 * adhere strictly to Object Oriented best practices. For instance we couldnt make
 * use of polymorphism for calls like Completed(), ~CalActivity, as it broke 
 * existing working code in some code paths (e.g: an improper usage goes to the 
 * extent of calling methods on NULL CalTransaction pointer without checking if 
 * the pointer is valid or not.) The old calclient code was resilient to these invalid 
 * use cases in some paths with guards (like having checks in all CAL APIs to return 
 * if CAL is disabled). As a result we had to make sure that the new
 * calclient version also behaves the same way. Cleaning up the entire paypal code base usage 
 * of CAL APIs is outside the scope of this calclient reimplementation project. 
 */

/* 
   Note: 
   If you change the format of the data in this function, 
   also change the appropriate code in CalDPubQueue.cpp file 
   in the function CalDPubQueueWrapper::publish(CalDMessage* _data) function.
*/
ulong CalActivity::SendSQLData ( const std::string& sql_query )
{
	if (!CalClient::is_enabled())
		return 0;

	// NOTE: must use "chars" here and not "uchars" because the uchars function
	// will return a string with 2-bytes per character, the high byte of which
	// will always be zero -- and then fnv_64a_str would think the string was
	// only a single byte long!
	unsigned long long hash64 = fnv_64a_str(sql_query.c_str(), FNV1_64A_INIT);
	ulong hi = (ulong) (hash64 >> 32);
	ulong lo = (ulong) hash64;
	ulong hash32 = lo ^ hi;

	std::ostringstream os;
	os << kCALDollar << hash32 << kCALTab << sql_query << kCALEndOfLine;

	WriteData(os.str());
	return hash32;
}

void CalActivity::SetName(const std::string& name)
{
	if (!CalClient::is_enabled())
		return;

	if (IsCompleted())
	{
		ReportAlreadyCompletedEvent (kCALSetName, name);
		return;
	}
	mName = ValidateName(name);
}

//**********************************************
// NOTE: SetStatus() could be called with a litteral 0 parameter because
//       the compiler will treat the 0 as a null char *, and will use it
//       to construct a temporary std::string instance to use as a parameter to
//       the SetStatus() function.  std::string(const char *) will in turn accept
//       a null char * parameter and create an empty string.
//
//       To enable this to work as the user expects if they call SetStatus(0),
//       we add an explicit check on the status parameter for a length of 0
//       and set the string to kCALZero if the length is 0.
//**********************************************

void CalActivity::SetStatus (const std::string& status)
{
	if (!CalClient::is_enabled())
		return;

	// ignore the new status if the member status is already set to non-zero
	if (mStatus != kCALZero && mStatus != kCALUnknown)
		return;

	mStatus = ValidateStatus(status);
}

void CalActivity::SetStatusRc(const std::string& _status, long rc)
{
	if (!CalClient::is_enabled())
		return;

	std::string status = FormatStatusWithRc(_status, rc);
	SetStatus(status);
}

void CalActivity::AddData(const std::string &name, long value)
{
	if (!CalClient::is_enabled())
		return;

	std::ostringstream os;
	os << value;
	AddData(name,os.str());
}

void CalActivity::AddData (const std::string& name, const std::string& value)
{
	if (!CalClient::is_enabled())
		return;

	std::string nameValuePairs;
	if (name.length() > 0)
	{
		nameValuePairs = name;
		nameValuePairs += kCALEquals;
	}
	nameValuePairs += value;

	AddData (nameValuePairs);
}

void CalActivity::AddData (const std::string& nameValuePairs)
{
	if (!CalClient::is_enabled())
		return;

	if (nameValuePairs.length() <= 0 )
		return;

	if (mCompleted)
	{
		ReportAlreadyCompletedEvent (kCALAddDataPairs, nameValuePairs);
		return;
	}

	if (mData.length() > 0)
		mData.append(kCALAmpersand);

	mData += ValidateData (nameValuePairs);
}

std::string CalActivity::GetStatus() const 
{
	if (!CalClient::is_enabled())
		return "";

	 return mStatus; 
}

// This function adds pool stack information to the CAL event with
// event type CAL::TYPE_CLIENT_INFO
void CalActivity::AddPoolStack()
{
	if (!CalClient::is_enabled())
		return;

	if (!CalClient::is_poolstack_enabled() )
		return;

	if (mType==CAL::EVENT_TYPE_CLIENT_INFO) 
	{
		std::string stack_info = CalTransaction::GetPoolStack();
		if(!stack_info.empty())
		    AddData("PoolStack", stack_info);
	}
}


CalActivity::~CalActivity()
{
	if (!CalClient::is_enabled())
		return;

}

CalActivity::CalActivity (char message_class, const std::string& type, const std::string& name, 
	const std::string& status, const std::string& data)
	: mCompleted (false), mClass(message_class), mType(kCALUnset), mName(kCALUnset), mStatus(kCALUnknown)
{
	memset (mTimeStamp, 0, sizeof(mTimeStamp));
	if (!CalClient::is_enabled())
		return;

	Initialize(type, name, status, data);
}

CalActivity::CalActivity (char message_class, const std::string& type)
	: mCompleted (false), mClass(message_class), mType(kCALUnset), mName(kCALUnset), mStatus(kCALUnknown)
{
	memset (mTimeStamp, 0, sizeof(mTimeStamp));
	if (!CalClient::is_enabled())
		return;

	Initialize(type, kCALUnset, kCALUnknown, "");
}

void CalActivity::WriteData (const std::string& buffer)
{
	if (!CalClient::is_initialized() || !CalClient::is_enabled())
		return;

	std::vector<std::string> *message_buffer=CalActivity::GetPendingMessageBuffer();
	if (message_buffer==NULL)
		return;

	if (message_buffer->size() >= GetMaxMsgBufferSize())
	{
		WriteTraceMessage(CAL_LOG_DEBUG, 0, "Message buffer limit of 300 for CAL_PENDING flag exceeded. Forcefully disabling the pending flag.");
		CalActivity::SetPending(false);
	}
	message_buffer->push_back(buffer);
	FlushMessageBuffer();
}

void CalActivity::FlushMessageBuffer (bool forceFlush)
{
	// FlushMessageBuffer could be called directly without WriteData
	// eg: a)Finalizing the root transaction's Name b)Completing root transaction without finalizing its Name 
	if (!CalClient::is_initialized() || !CalClient::is_enabled())
		return;

 	if (CalActivity::IsPending() && !forceFlush)
		return;

	std::vector<std::string> *message_buffer=CalActivity::GetPendingMessageBuffer();
	if (message_buffer==NULL)
		return;

	for (int i = 0; i < message_buffer->size(); i++ )
	{
		WriteBufferToHandler((*message_buffer)[i]);
	}
	message_buffer->clear();
}

void CalActivity::WriteTraceMessage(CalLogLevel _loglevel, int _errno, const std::string& _msg)
{

	//  Dont use logger object if CAL is disabled. Using logger object when exiting seems to
	//  have issues. We disable CAL when the process is exiting using atexit()
	if (!CalClient::is_enabled())
	{
		return;
	}

	CalClient* cal_client = CalClient::get_instance();
	if (!cal_client)
		return;
	CalConfig* cal_config = cal_client->get_config_instance();
	if (!cal_config)
		return;
	CalLog* cal_logger = cal_config->get_logger();
	if (!cal_logger)
		return;

	std::string message = kCALMessageLogPrefix;
	message += _msg;
	cal_logger->write_trace_message(_loglevel, _errno, message.c_str()); 
}

std::vector<std::string>* CalActivity::GetPendingMessageBuffer()
{
	CalClient *cal_client = CalClient::get_instance();
	if (cal_client)
	{
		return cal_client->get_pending_message_buffer();
	}
	return NULL;
}

bool CalActivity::IsPending()
{
	CalClient *cal_client = CalClient::get_instance();
	if (cal_client)
	{
		return cal_client->get_pending_flag();
	}
	return false;
}

void CalActivity::SetPending (bool _pending_flag)
{
	CalClient *cal_client = CalClient::get_instance();
	if (cal_client)
	{
		cal_client->set_pending_flag(_pending_flag);
	}
}

void CalActivity::SendSelf()
{
	std::ostringstream os;
	os << mClass << mTimeStamp << kCALTab << mType << kCALTab << mName << kCALTab << mStatus << kCALTab << mData << kCALEndOfLine;
	WriteData(os.str());
}

//  Validator methods for Type, Name, Data
std::string CalActivity::ValidateType(const std::string& _type) const 
{
	std::string type(_type);
	if (type.length() <= 0)
		return kCALUnset;
	if(type.length() > kCALMaxTypeLength)
	{
		type.resize(kCALMaxTypeLength);
		type[kCALMaxTypeLength-1] = '+';  // mark mType as truncated
	}
	return type;
}

std::string CalActivity::ValidateName(const std::string& _name) const
{
	std::string name(_name);
	if (name.length() <= 0)
		return kCALUnset;
	if (name.length() > kCALMaxNameLength)
	{
		name.resize(kCALMaxNameLength);
		name[kCALMaxNameLength-1] = '+';  // mark mName as truncated.
	}
	return name;
}

std::string CalActivity::ValidateStatus(const std::string& _status) const
{
	// if status.length() is 0, then the string is probably a temporary
	// created by somebody calling SetStatus(0) instead of SetStatus("0").
	if (_status.length() <= 0)
		return kCALZero;
	else
		return _status;
}

std::string CalActivity::ValidateData(const std::string& data) const
{

	if ( (StringUtil::skip_newline(data, 0) < data.size()) )
	{
		std::string tmp_encoded_value;
		base64_encode(data.c_str(), tmp_encoded_value, data.length());
		return std::string(kCALBase64Data) + "=" + tmp_encoded_value;
	} 
	else 
	{
		return data;
	}
}

std::string CalActivity::FormatStatusWithRc(const std::string& _status, long rc)
{
	std::ostringstream os;
	os << _status;

	// if string doesn't already end with a '.', add one.
	if(!StringUtil::ends_with(_status, std::string(".")))	
		os << ".";
	os << rc;
	return os.str();
}

int CalActivity::GetMaxMsgBufferSize()
{
	CalClient* cal_client = CalClient::get_instance();
	if (!cal_client)
		return kCALMaxMessageBufferSize;

	CalConfig* cal_config = cal_client->get_config_instance();
	 if (!cal_config)
		return kCALMaxMessageBufferSize;

	return cal_config->get_msg_buffer_size();
}

void CalActivity::WriteBufferToHandler(const std::string& buffer)
{
	CalClient* cal_client = CalClient::get_instance();
	if (!cal_client)
		return;
	CalHandler* cal_handler = cal_client->get_handler();
	if (cal_handler)
		cal_handler->write_data( buffer );
}

void CalActivity::SetLossyRootTxnFlag()
{
	CalClient* cal_client = CalClient::get_instance();
	if (!cal_client)
		return;
	CalHandler* cal_handler = cal_client->get_handler();
	if (cal_handler)
		cal_handler->set_root_txn_lossy_flag (true);
}

void CalActivity::Initialize (const std::string& type, const std::string& name,
	const std::string& status, const std::string& data)
{
	CalTimeOfDay::Now (mTimeStamp);

	mType = ValidateType(type);
	mName = ValidateName(name);
	mStatus = ValidateStatus(status);

	AddData(data);
}

void CalActivity::ReportAlreadyCompletedEvent (const std::string& function, const std::string& arg) const
{
 	CalEvent event (kCALTypeBadInstrumentation, kCALNameAlreadyCompleted, "1");
	std::ostringstream os;
	os << mClass;
 	event.AddData ("Class", os.str());
 	event.AddData ("When", mTimeStamp);
 	event.AddData ("Type", mType);
 	event.AddData ("Name", mName);
 	event.AddData ("Func", function);
 	event.AddData ("Arg", arg);
 	event.Completed();
}

// ===== class CalEvent =====
CalEvent::CalEvent (const std::string& type, const std::string& name, const std::string& status, const std::string& data)
	: CalActivity (kCALClassEvent, type, name, status, data), mParent(CalTransaction::GetCurrent())
{
	if (!CalClient::is_enabled())
		return;

	if (mParent)
		mParent->OnChildCreation();
}

CalEvent::CalEvent (const std::string& type)
	: CalActivity (kCALClassEvent, type), mParent(CalTransaction::GetCurrent())
{
	if (!CalClient::is_enabled())
		return;

	if (mParent)
		mParent->OnChildCreation();
}

CalEvent::~CalEvent()
{
	if (!CalClient::is_enabled())
		return;

	Completed();
}

void CalEvent::SetType(const std::string& type)
{
	if (!CalClient::is_enabled())
		return;

	mType =	ValidateType(type);
}

void  CalEvent::Completed()
{
	if (!CalClient::is_enabled())
		return;

	if (IsCompleted())
		return;

	SendSelf();
	SetCompleted();
}


// ===== class CalHeartbeat =====
CalHeartbeat::CalHeartbeat(const std::string& type, const std::string& name, const std::string& status, const std::string& data)
: CalActivity(kCALClassHeartbeat, type, name, status, data), mParent(CalTransaction::GetCurrent())
{
	if (!CalClient::is_enabled())
		return;

	if (mParent)
		mParent->OnChildCreation();

}

CalHeartbeat::CalHeartbeat(const std::string& type)
: CalActivity(kCALClassHeartbeat, type), mParent(CalTransaction::GetCurrent())
{
	if (!CalClient::is_enabled())
		return;

	if (mParent)
		mParent->OnChildCreation();

}

CalHeartbeat::~CalHeartbeat()
{
	if (!CalClient::is_enabled())
		return;

	Completed();
}

void  CalHeartbeat::Completed()
{
	if (!CalClient::is_enabled())
		return;

	if (IsCompleted())
		return;

	SendSelf();
	SetCompleted();
}

// ===== class CalTransaction =====

std::string CalTransaction::Status::to_string() const
{
	std::string out(CAL::TRANS_OK);

	if (m_severity != CAL::TRANS_OK)
	{
		out = m_severity;
		out.append(kCALPeriod);
		out.append(m_module_name);
		out.append(kCALPeriod);
		out.append(m_system_err_code);
		out.append(kCALPeriod);
		out.append(m_module_rc);
	}
	return out;
}


CalTransaction::CalTransaction(const std::string& type)
	: CalActivity (kCALClassAtomicTransaction, type), m_duration(-1), mParent(CalTransaction::GetCurrent())
{
	if (!CalClient::is_enabled())
		return;

	if (mParent)
		mParent->OnChildCreation();

	if (!GetRootTransaction())
	{
		WriteTraceMessage(CAL_LOG_DEBUG, 0, "Starting root transaction ");
		SetRootTransaction(this);	
	}
	CalTransaction::SetCurrent (this);
}

CalTransaction::~CalTransaction()
{
	if (!CalClient::is_enabled())
		return;

	Completed();
}

void CalTransaction::SetSessionID(const std::string& appInfo, bool encrypt) 
{
	if (!CalClient::is_enabled())
		return;

	std::string session_id;
	if (appInfo.empty()) 
		return;
	
	if (encrypt)
	{
		Fnv64_t hash_val = fnv_64_str(appInfo.c_str(), FNV1_64_INIT);
		std::ostringstream os;
		os << hash_val;
		session_id = os.str();
	}
	else
	{
		session_id = appInfo;
	}

	CalClient *cal_client = CalClient::get_instance();
	if (cal_client)
	{
		cal_client->set_session_id(session_id);
	}
}

void CalTransaction::SetCPUTicks(const std::string& cpuTicks)
{
	//nop. this API will be deprecated
}

void CalTransaction::SetCorrelationID(std::string corrID)
{
	if (!CalClient::is_enabled())
		return;

	CalUtility *cal_utility = CalUtility::GetInstance();
	if (cal_utility)
	{
		cal_utility->SetCorrelationID(corrID); 
	}
}

void CalTransaction::SetCorrelationID()
{
	if (!CalClient::is_enabled())
		return;

	CalUtility *cal_utility = CalUtility::GetInstance();
	if (cal_utility)
	{
		cal_utility->SetCorrelationID(); 
	}
}

std::string CalTransaction::GetCorrelationID()
{
	if (!CalClient::is_enabled())
		return "";

	CalUtility *cal_utility = CalUtility::GetInstance();
	if (cal_utility)
	{
		return cal_utility->GetCorrelationID(); 
	}
	return "";
}

std::string CalTransaction::GetCurrentPoolInfo()
{ 
	if (!CalClient::is_enabled())
		return "";

	CalUtility *cal_utility = CalUtility::GetInstance();
	if (cal_utility)
	{
		return cal_utility->GetCurrentPoolInfo(); 
	}
	return "";
}

std::string CalTransaction::GetPoolStack()
{ 
	if (!CalClient::is_enabled())
		return "";

	CalUtility *cal_utility = CalUtility::GetInstance();
	if (cal_utility)
	{
		return cal_utility->GetPoolStack(); 
	}
	return "";
}

void CalTransaction::SetOperationName(const std::string& opname, bool forceFlag)
{ 
	if (!CalClient::is_enabled())
		return;

	CalUtility *cal_utility = CalUtility::GetInstance();
	if (cal_utility)
	{
		cal_utility->SetOperationName(opname, forceFlag); 
	}
}

std::string CalTransaction::GetOperationName()
{ 
	if (!CalClient::is_enabled())
		return "";

	CalUtility *cal_utility = CalUtility::GetInstance();
	if (cal_utility)
	{
		return cal_utility->GetOperationName();
	}
	return "";
}

void CalTransaction::AddDataToRoot(const std::string& name, const std::string& value)
{
	if (!CalClient::is_enabled())
		return;
	CalTransaction *root = GetRootTransaction();
	if(root)
		root->AddData(name, value);
}
CalTransaction* CalTransaction::GetCurrent()
{ 
	if (!CalClient::is_enabled())
		return NULL;

	CalClient *cal_client = CalClient::get_instance();
	if (cal_client)
	{
		return cal_client->get_current_transaction();
	}
	return NULL; 
}


void CalTransaction::SetParentStack(const std::string& clientpoolInfo, const std::string operationName) 
{
	if (!CalClient::is_enabled())
		return;

	CalUtility *cal_utility = CalUtility::GetInstance();
	if (cal_utility)
	{
		cal_utility->SetParentStack(clientpoolInfo, operationName);
	}
}

void CalTransaction::SetStatusRc(const std::string& _status, long rc, CalFlags flag)
{	
	if (!CalClient::is_enabled())
		return;

	std::string status = FormatStatusWithRc(_status, rc);
	SetStatus(status, flag);
}

void CalTransaction::SetStatus (const std::string& status, CalFlags flag)
{
	if (!CalClient::is_enabled())
		return;

	CalActivity::SetStatus (status);
	if (flag & CAL_SET_ROOT_STATUS)
	{
		CalTransaction *root = GetRootTransaction();
		if(root)
			root->SetStatus (status);
	}
}

void CalTransaction::SetRootTransactionStatus (const std::string& status)
{
	if (!CalClient::is_enabled())
		return;

	CalTransaction *root = GetRootTransaction();
	if(root)
		root->SetStatus (status);
}

void CalTransaction::SetName (const std::string& name, CalFlags flag)
{
	if (!CalClient::is_enabled())
		return;

	CalActivity::SetName(name);

	if(IsRootTransaction())
	{
		SetOperationName(name,true);
	}

	if ( flag & CAL_PENDING ) 
	{
		HandlePendingFlag();
	}
	else if ( flag & CAL_FINALIZE_ROOT_NAME ) 
	{
		HandleFinalizeRootNameFlag(name);
	}
}

void CalTransaction::SetDuration( const double duration)
{
	if (!CalClient::is_enabled())
		return;
	if (duration < kMinDuration)
	{
		m_duration=kMinDuration;
	}
	else if (duration > kMaxDuration)
	{
		m_duration=kMaxDuration;
	}
	else
	{
		m_duration=duration;
	}
}

void CalTransaction::Completed()
{
	if (!CalClient::is_enabled())
		return;

	if (IsCompleted())
		return;

	if (IsRootTransaction() && IsStartSent()) 
	{
		AddAdditionalFieldsForRoot();
	}
	CompleteAnyNestedTransactions();
	SendSelf();
	SetCompleted();
	OnCompletion();
}

void CalTransaction::Completed(const std::string& status)
{
	if (!CalClient::is_enabled())
		return;

	SetStatus(status);
	Completed(); 
}

CalTransaction* CalTransaction::GetRootTransaction()
{
	CalClient *cal_client = CalClient::get_instance();
	if (cal_client)
	{
		return cal_client->get_root_transaction();
	}
	return NULL;
}

void CalTransaction::SetRootTransaction(CalTransaction* root)
{
	CalClient *cal_client = CalClient::get_instance();
	if (cal_client)
	{
		cal_client->set_root_transaction(root);
		CalHandler* cal_handler = cal_client->get_handler();
		if (cal_handler && root)
			cal_handler->handle_new_root_transaction();
	}
}

std::string CalTransaction::GetCPUTicks()
{
	return ""; //nop, this API will be deprecated
}

std::string CalTransaction::GetSessionID()
{
	CalClient* cal_client = CalClient::get_instance();
	if (cal_client)
	{
		return cal_client->get_session_id();
	}
	return "";
}

void CalTransaction::SetCurrent(CalTransaction* _transaction) 
{
	CalClient *cal_client = CalClient::get_instance();
	if (cal_client)
	{
		cal_client->set_current_transaction(_transaction);
	}
}

void CalTransaction::OnChildCreation()
{
	if (mClass == kCALClassAtomicTransaction)
	{
		mClass = kCALClassStartTransaction;
		SendSelf();
	}	
}
void CalTransaction::SendSelf()
{
	switch (mClass)
	{
		case kCALClassAtomicTransaction: //Send end transaction message with 'A'
			FlushAtomicTransaction();
			break;
		case kCALClassStartTransaction: //Send start transaction mesage with 't'
			FlushStartTransaction();
			mClass = kCALClassEndTransaction;
			break;
		case kCALClassEndTransaction:  //Send end transaction message with 'T'
			FlushEndTransaction();
			break;
		default:
			//  TODO Adding log
			break;
	}
}

void CalTransaction::OnCompletion()
{
	CalTransaction::SetCurrent(mParent);
	if (IsRootTransaction())
	{
		// Flush buffer definitely before the end of a root transaction
		CalActivity::SetPending(false);
		FlushMessageBuffer(true);
		//If root transaction is being completed, reset Root transaction variable to NULL
		SetRootTransaction(NULL);
		WriteTraceMessage(CAL_LOG_DEBUG, 0, "Cleared root transaction ");
	}
}

void CalTransaction::CompleteAnyNestedTransactions()
{
	CalTransaction* nested = CalTransaction::GetCurrent();   	// the innermost transaction
	CalTransaction* self = this;			// the transaction explicity being closed
	
	while (nested && nested != self)
	{
		CalEvent event (kCALTypeBadInstrumentation, kCALNameCompletingParent, "1");
		event.AddData ("ParentType", self->GetType());
		event.AddData ("ParentName", self->GetName());
		event.AddData ("ChildType", nested->GetType());
		event.AddData ("ChildName", nested->GetName());
		event.Completed();
		
		nested->Completed();
		nested = nested->GetParent();
	}
}

void CalTransaction::FlushStartTransaction()
{
	std::string buffer;
	if (IsRootTransaction())
	{
		// to make raw logs more readable (for humans)
		// we put out a blank line before each level 0 non-atomic transaction
		buffer = kCALEndOfLine;
	}
	PrepareStartOfTransactionMessage(buffer, kCALClassStartTransaction);
	WriteData (buffer);
}

void CalTransaction::FlushAtomicTransaction()
{
	std::string buffer;
	PrepareEndOfTransactionMessage(buffer, kCALClassAtomicTransaction);
	WriteData (buffer);
}

void CalTransaction::FlushEndTransaction()
{
	CalTimeOfDay::Now (mTimeStamp);
	std::string buffer;
	PrepareEndOfTransactionMessage(buffer, kCALClassEndTransaction);
	WriteData (buffer);
}

void CalTransaction::PrepareStartOfTransactionMessage(std::string& buffer, char message_class)
{
	std::ostringstream os;
	os << message_class << mTimeStamp << kCALTab << mType << kCALTab << mName << kCALEndOfLine;
	buffer = os.str();
}

void CalTransaction::PrepareEndOfTransactionMessage(std::string& buffer, char message_class)
{
	char duration_str[256];
	double duration = mTimer.Duration();

	if (m_duration >= kMinDuration)
	{
		duration=m_duration;
	}

	CalMicrosecondTimer::PrivFormatDuration (duration_str, duration);

	std::ostringstream os;
	os << message_class << mTimeStamp << kCALTab << mType << kCALTab << mName << kCALTab << mStatus << kCALTab << 
		duration_str << kCALTab << mData << kCALEndOfLine;
	buffer = os.str();
}

void CalTransaction::AdddNameValuePair(std::string& fields, const std::string& name, const std::string& value)
{
	if (!value.empty())
	{
		if (fields.length() > 0)
			fields += kCALAmpersand;
		fields += name + kCALEquals + value; 
	}
}
void CalTransaction::AddAdditionalFieldsForRoot()
{
	std::string fields;
	CalUtility *cal_utility = CalUtility::GetInstance();
	if (cal_utility)
	{
		AdddNameValuePair(fields, "corr_id_", cal_utility->GetCorrelationID());
		AdddNameValuePair(fields, "log_id_", cal_utility->GetLogId(mTimeStamp));
	}
	AdddNameValuePair(fields, "session_id_", CalTransaction::GetSessionID());

	if (mData.length() > 0)
		fields.append(kCALAmpersand);
	mData = fields + mData;
}

void CalTransaction::HandlePendingFlag()
{
	if ( IsRootTransaction() && !IsStartSent() ) 
	{
		// Enable message bufferring if the current transaction is root
		// and there is no CAL message flushed out yet.
		WriteTraceMessage(CAL_LOG_DEBUG, 0, "Starting message buffering with Pending Flag");
		FlushMessageBuffer();
		CalActivity::SetPending(true);
	}
}

void CalTransaction::HandleFinalizeRootNameFlag(const std::string& name)
{
	if (!CalActivity::IsPending() )
		return;
	
	CalTransaction *root = GetRootTransaction();
	std::vector<std::string> *message_buffer = CalActivity::GetPendingMessageBuffer();
	if ( name.length() > 0 && root && message_buffer && 
	     !(message_buffer->empty())) //Transaction is atomic till now
	{
		root->SetName ( name );
		SetOperationName(name, true);
		std::string buffer;
		root->PrepareStartOfTransactionMessage(buffer, kCALClassStartTransaction);
		(*message_buffer)[0] = buffer;
		WriteTraceMessage(CAL_LOG_DEBUG, 0, buffer + " -- Finalized " );
	}
	CalActivity::SetPending(false);
	FlushMessageBuffer ();
}

bool CalTransaction::IsStartSent() const                     
{
	return (mClass == kCALClassEndTransaction) ; 
}

std::string CalTransaction::GetRootTransactionName()
{
	CalTransaction *root = GetRootTransaction();
	if(root)
		return root->GetName();
	return "";
}
