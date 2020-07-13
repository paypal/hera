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
/*
 * ShardKeyUtil.cpp
 */

#include "ShardKeyUtil.h"
#include <sstream>
#include <boost/regex.hpp>

void ShardKeyUtil::append_escape(std::string& dest, const std::string & src)
{
	//printf("size=%lu\n", src.size());
	const char *src_buf = src.c_str();
	
	std::stringstream ss;
	for (uint j = 0; j < src.size(); j++) {
		switch (src_buf[j]) {
			case ';':
			case '\\':
				ss << '\\';
				// fall thru
			default:
				ss << src_buf[j];
				break;
		}
	}
	
	dest = ss.str();
}

int ShardKeyUtil::tokenize(const std::string& _str, char _escape, char _sep,
	std::vector<std::string>& _values)
{
	bool inEscape = false;
	const char  *str = _str.c_str();
	size_t len = _str.size();
	
	std::string v;
	for (uint pos = 0; pos <= len; pos++) {
		char c = str[pos];
		if (inEscape) {
			inEscape = false;
		} else if (c == _escape) {
			inEscape = true;
			continue;
		} else if (c == _sep || c == '\0') {
			_values.push_back(v);
			v.clear();
			continue;
		}
		v.append(1, c);
	}
	if (inEscape) {
		// error!
		return -1;
	}
	
	return 0;
}

void
ShardKeyUtil::gen_shard_key(const std::string &_key_name, std::vector<std::string> &_values,
	std::string& _shard_info)
{
	std::string payload;
	
	static std::stringstream ss;
	ss.str("");
	ss.clear();
	ss << _key_name.c_str() << "=";
	
	for (uint i = 0; i < _values.size(); i++) {
		std::string res;
		append_escape(res, _values[i]);
		ss << res;
		if (i < _values.size() - 1) {
			ss << ';';
		}
	}
	
	_shard_info = ss.str().c_str();
}

// format: key_name=value1;value;value
//   value may be escaped \;

int ShardKeyUtil::parse_shard_key(const std::string& _shard_info, std::string& _key_name,
	std::vector<std::string>& _key_values)
{
	char escape = '\\'; // escape
	char seperator = ';'; // seperator
	
	size_t pos = _shard_info.find("=");
	
	_key_name = _shard_info.substr(0, pos);

	// make it lower case
	for (size_t i=0; i< _key_name.length(); i++)
	{
		_key_name[i] = tolower(_key_name[i]);
	}

	std::string shard_values = _shard_info.substr(pos+1);
	return tokenize(shard_values, escape, seperator, _key_values);
}

void
ShardKeyUtil::process_bind_name(const std::string& _name, std::string& _res_name)
{
	size_t start = 0;
	if (_name[0]==':')
	{
		start = 1;
	}

	size_t found = _name.find_last_of('_');
	if (found == std::string::npos)
	{
		_res_name = _name.substr(start, _name.size() - start);
		return;
	}

	bool cut_tail = true;
	for (uint pos = found+1; pos < _name.size(); pos++)
	{
		if (!isdigit(_name[pos]))
		{
			cut_tail = false;
			break;
		}
	}

	if (cut_tail)
	{
		_res_name = _name.substr(start, found - start);
	}
	else
	{
		_res_name = _name.substr(start, _name.size() - start);
	}

	// make it lower case
	for (size_t i=0; i< _res_name.length(); i++)
	{
		_res_name[i] = tolower(_res_name[i]);
	}
}

// slow
void
ShardKeyUtil::process_bind_name2(const std::string& _name, std::string& _res_name)
{
	try {
		boost::regex expr("^(:)|(_\\d+)$");
		std::string fmt("");
		_res_name = boost::regex_replace(_name, expr, fmt);
	}
	catch (boost::regex_error& e)
	{
		_res_name = _name;
	}
}
