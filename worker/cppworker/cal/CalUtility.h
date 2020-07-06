#ifndef CAL_UTILITY_H
#define CAL_UTILITY_H

#include <string>

class CalUtility
{
	public:
		friend class CalTransaction;

		std::string GetCurrentPoolInfo();
		std::string GetPoolStack();
		void SetParentStack(const std::string& clientpoolInfo, const std::string operationName);
		void SetOperationName(const std::string& opname, bool forceFlag);
		std::string GetOperationName(); 

		void   SetCorrelationID();
		void   SetCorrelationID(std::string corrID);
		std::string GetCorrelationID();

		static CalUtility* GetInstance();
		void CreateAtomicTransaction(std::string type, std::string name, std::string status, double duration, std::string data);

	private:
		CalUtility ();
		std::string m_current_operation_name;
		std::string m_parent_stack;
		std::string m_correlation_id;

		std::string GetLogId(const char* _cal_time);
		unsigned int get_stageproxy_ip();

};

#endif // CAL_UTILITY_H
