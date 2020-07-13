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
#ifndef _SIMPLECONFIG_H_
#define _SIMPLECONFIG_H_

/*
  Fast hash-based config class

  Config format:
  [lwsp]name[lwsp]=[lwsp]value[lwsp][n]

  lwsp = linear white space
  n = newline

  Lines starting with # are comments

*/

#include <string>
#include <unordered_map>
#include "Config.h"

//redefine this if you think your config file will be larger
#ifndef SIMPLE_CONFIG_INITIAL_SIZE
#define SIMPLE_CONFIG_INITIAL_SIZE 200
#endif

// SimpleConfigIterator class unused and removed for PPSCR00111530

class SimpleConfig : public Config
{
public:
	SimpleConfig(const std::string& filename);
	~SimpleConfig();

	virtual bool get_value(const std::string& name, std::string& value) const;
	bool get_all_values (std::unordered_map<std::string,std::string>& _values_out) const;

private:
	std::unordered_map<std::string,std::string> values;
};

#endif
