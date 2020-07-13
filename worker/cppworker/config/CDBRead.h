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
#ifndef _CDBREAD_H_
#define _CDBREAD_H_

#include <string>
#include <fstream> 
#include <unordered_map>

#include <config/CDBCommon.h>

class CDBRead
{
public:
	CDBRead(std::ifstream &_in);
	~CDBRead();

	bool get(const std::string& key, std::string &value);

	bool get_all_values (std::unordered_map<std::string,std::string>& values);

private:
	std::string file_contents;

	// returns the hashvalue of the given key
	unsigned int hash(const char * key, int length);

	// matches key with the input stream
	int match(const char * key, int key_length, uint& file_offset);

};

#endif
