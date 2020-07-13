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
#include "OPSConfig.h"
#include <sstream>

OPSConfig *OPSConfig::m_instance = 0;

OPSConfig::OPSConfig(const std::string& filename): m_cfg(filename)
{
	const char* start = filename.c_str();
	const char* end = start + filename.size() - 4;
	if ((end <= start) || (*end != '.')) 
	{
		std::ostringstream os;
		os << "Invalid file " << filename;
		throw ConfigException(os.str());
	}
	const char* p = end;
	while ((p >= start) && (*p != '/')) p--;
	p++;
	std::ostringstream os;
	os << "opscfg." << std::string(p, end - p) << ".server.";
	m_keyPrefix = os.str();
}

bool OPSConfig::get_value(const std::string& name, std::string& value) const
{
	std::string key = m_keyPrefix + name;
	if (m_cfg.get_value(key, value)) {
		return true;
	}
	return m_cfg.get_value(std::string("opscfg.default.server.") + name, value);
}

bool OPSConfig::load_if_changed()
{
	return true;
}

OPSConfig& OPSConfig::create_instance(const std::string& filename) {
	m_instance = new OPSConfig(filename);
	return *m_instance;
}
