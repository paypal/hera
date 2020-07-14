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

#include "SimpleConfig.h"
#include <fstream>
#include <regex>

SimpleConfig::SimpleConfig(const std::string& filename)
{
	std::ifstream in(filename);
	if (!in.good()) {
		throw ConfigException("Can't read file");
	}
	std::string buf;
	std::regex e("(.*)=(.*)");
	while (std::getline(in, buf)) {
		std::smatch match;
		if (std::regex_search(buf, match, e) && match.size() > 1) {
    		values[match.str(1)] = match.str(2);
		}
	}
}

SimpleConfig::~SimpleConfig()
{
}

bool SimpleConfig::get_value(const std::string& name, std::string& value) const
{
	std::unordered_map<std::string,std::string>::const_iterator it = values.find(name);
	if (it != values.end()) {
		value = it->second;
		return true;
	}
	return false;
}
