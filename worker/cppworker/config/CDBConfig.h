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
#ifndef _CDBCONFIG_H_
#define _CDBCONFIG_H_

#include <string>
#include <unordered_map>

/*
  Fast CDB-based config class
*/

#include "Config.h"

class CDBRead;

class CDBConfig : public Config
{
public:
	CDBConfig(const std::string& name);
	virtual ~CDBConfig();

	virtual bool get_value(const std::string& name, std::string& value) const;
	bool get_all_values (std::unordered_map<std::string,std::string>& _values_out) const;

	// Check to see if the file has been modified since the last check
	virtual bool check_if_changed();

	// Load from file if the file has been modified since the last check
	virtual bool load_if_changed();

private:
	CDBRead * cdb_read;

	std::string m_filename;
	
	// last modification time for file
    time_t m_mtime;

private:
	// copies not supported
	CDBConfig(const CDBConfig& rhs);
	CDBConfig& operator=(const CDBConfig& rhs);
	bool set_filename(const char * filename);
};

#endif
