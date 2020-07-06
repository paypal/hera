#include "CalUtility.h"
#include "CalClient.h"
#include "CalMessages.h"
#include "CalTime.h"
#include "CalConfig.h"
#include "CalConst.h"
#include "utility/fnv/fnv.h"
#include <string.h>

#include <fstream>
#include <boost/regex.hpp>
#include <boost/lexical_cast.hpp>
#include <string>
#include <sstream>

const char   kCALFeatureSeperator   = ':';
const char  *kCALPoolSeperator   = "^";
const unsigned long kCALClientThreadId = 0;

std::string CalUtility::GetCurrentPoolInfo()
{
	if (!(CalClient::is_enabled()) || !(CalClient::is_poolstack_enabled())) 
		return "";

	char host_name[40]; host_name[0]='\0';
	if(gethostname(host_name, sizeof(host_name)))
	{
		host_name[39]='\0';
	}

	CalTransaction *root = CalTransaction::GetRootTransaction();
	std::string txn_start_time("TopLevelTxn not set");
	if(root)
		txn_start_time = GetLogId(root->mTimeStamp);

	//the current pool info format <poolname>:<Op name>*CalThreadId=<thread id>*TopLevelTxnStartTime=<toplevel txn start time>*Host=<host>
	std::string CurrOperationName = GetOperationName();
	std::ostringstream current_pool_info;
	current_pool_info << CalClient::get_poolname() << kCALFeatureSeperator << CurrOperationName 
		<< "*CalThreadId=" << kCALClientThreadId 
		<< "*TopLevelTxnStartTime=" << txn_start_time
		<< "*Host=" << host_name;

	return current_pool_info.str();
}

std::string CalUtility::GetPoolStack()
{
	if (!(CalClient::is_enabled()) || !(CalClient::is_poolstack_enabled())) 
		return "";

	std::string current_pool_info = GetCurrentPoolInfo();
	if (!m_parent_stack.empty())
		return (m_parent_stack + kCALPoolSeperator + current_pool_info);
	else  
		return current_pool_info;
}

void CalUtility::SetParentStack(const std::string& clientpoolInfo, const std::string operationName) 
{
	if (!(CalClient::is_enabled()) || !(CalClient::is_poolstack_enabled())) 
		return;

    CalClient* cal_client = CalClient::get_instance();
    if (!cal_client)
        return ;
    CalConfig* cal_config = cal_client->get_config_instance();
    if (!cal_config)
        return ;

	unsigned long max_poolstack_length = cal_config->get_poolstack_length();
	//reduce string size to limit Poolstackinfo
	if (clientpoolInfo.length() >= max_poolstack_length ) 
	{
		size_t first = clientpoolInfo.find_first_of(std::string(kCALPoolSeperator));
		int second = clientpoolInfo.find_first_of(std::string(kCALPoolSeperator), first +1);
		m_parent_stack = clientpoolInfo.c_str() + second + 1; 
	}
	else
		m_parent_stack = clientpoolInfo;

	if (!operationName.empty())
		m_current_operation_name = operationName;
}

void CalUtility::SetOperationName(const std::string& opname, bool forceFlag)
{
	if (!(CalClient::is_enabled()) || !(CalClient::is_poolstack_enabled())) 
		return;

	if (forceFlag)
		m_current_operation_name = opname;
	else if (m_current_operation_name.empty())
		m_current_operation_name = opname;
}

std::string CalUtility::GetOperationName() 
{ 
	if (!(CalClient::is_enabled())) 
		return "";

	return m_current_operation_name; 
} 

void CalUtility::SetCorrelationID(std::string corrID)
{
	if (!(CalClient::is_enabled())) 
		return;

	if (corrID.empty()) 
	{
		m_correlation_id = CALVALUENOTSET;
	} 
	else 
	{
		m_correlation_id = corrID;
	}
}
void CalUtility::SetCorrelationID()
{
	if (!(CalClient::is_enabled())) 
		return;

	// Check for CorrID from Slingshot, before creating a new one.
	char* sl_corr_id = getenv("HTTP_X_PP_CORRID");
	if (sl_corr_id && strlen(sl_corr_id)){
		m_correlation_id = sl_corr_id;
		return;
	}

	// Get timestamp
	struct timeval t;
	gettimeofday(&t, NULL);


	// Get proxy ip for a managed stage if enabled
    CalClient* cal_client = CalClient::get_instance();
    if (!cal_client)
        return ;
    CalConfig* cal_config = cal_client->get_config_instance();
    if (cal_config && cal_config->get_proxybased_corrid()) {
		unsigned int proxy_ip = this->get_stageproxy_ip();
		if(proxy_ip) {
				char buff[256];
				sprintf(buff, "%08x%05x", proxy_ip, (unsigned int)t.tv_usec);
				m_correlation_id = buff;
				return;
		}
	}

	// Get host name
    char hostname[HOST_NAME_MAX + 1];

    hostname[HOST_NAME_MAX] = 0;
    if (gethostname(hostname, HOST_NAME_MAX) != 0)
		return;

	if (hostname == NULL || *hostname == '\0')
	{
		return ;
	}

	// Get PID
	pid_t pid = getpid();


	//Generate CorrelationID
	std::ostringstream os;
	os << hostname << (int)pid << t.tv_sec << t.tv_usec;

	Fnv64_t corr_val = fnv_64_str(os.str().c_str(), FNV1_64_INIT);

	// Clear out existing m_correlation_id
	std::ostringstream os2;
	os2 << std::hex << (unsigned int)corr_val << (unsigned int)t.tv_usec;
	m_correlation_id = os2.str();
}

/*
 * retrive the ip from local /etc/hosts file for stageproxy.private
 * this will be set on all userstages and will point to local proxy 
 * which handles local as well as connections to managed/core stage.
 * ex: 10.57.210.82	stageproxy.private  stage2payment1
 */
unsigned int CalUtility::get_stageproxy_ip()
{	
	unsigned int proxy_ip = -1;
	boost::cmatch matches;
	std::string line;
	
	try	{
		std::ifstream fs("/etc/hosts");
		if(fs.is_open()) {
			boost::regex comment("[[:space:]]*#+.*");
			boost::regex ip("[[:space:]]*([[:digit:]]{1,3})\\.([[:digit:]]{1,3})\\.([[:digit:]]{1,3})\\.([[:digit:]]{1,3})[[:space:]]+.*stageproxy\\.private[[:space:]].*");
			while(getline(fs, line)){
				//skip over comments
				if (boost::regex_match(line.c_str(), comment)) 
					continue; 

				if(boost::regex_match(line.c_str(), matches, ip)) {
					//calculate ip - this is assumed to be ipv4
					for(int i=1; i<=4; i++) {
						proxy_ip = proxy_ip << 8 | boost::lexical_cast<unsigned int>(matches[i]);
					}
				}
			}
			fs.close();
		}
	} catch(...) {
		//ignore all errors for this
	}

	return proxy_ip;
}

std::string CalUtility::GetCorrelationID()
{
	if (!(CalClient::is_enabled())) 
		return "";

	return m_correlation_id; 
}

CalUtility* CalUtility::GetInstance()
{
	static CalUtility *s_utility = NULL;
	if (s_utility==NULL)
	{
		s_utility=new CalUtility();
	}
	return s_utility;
}

std::string CalUtility::GetLogId(const char* _cal_time)
{
	time_t t=time(NULL);
	tm* start_day = localtime(&t);
	start_day->tm_sec=0;
	start_day->tm_min=0;
	start_day->tm_hour=0;
	unsigned start_time = CalDayTime::CentisecondsFromTime(_cal_time);
	std::ostringstream os;
	os << std::hex << (unsigned long long)mktime(start_day)*1000 + start_time*10;
	return os.str();
}

void CalUtility::CreateAtomicTransaction(std::string type, std::string name, std::string status, double duration, std::string data)
{
	CalTransaction cal_txn(type);
	cal_txn.SetName(name);
	cal_txn.SetStatus(status);
	cal_txn.SetDuration(duration);
	cal_txn.AddData(data);
	cal_txn.Completed();
}

CalUtility::CalUtility()
	: m_current_operation_name ()
	, m_parent_stack()
	, m_correlation_id(CALVALUENOTSET)
{
}
