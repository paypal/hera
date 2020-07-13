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
