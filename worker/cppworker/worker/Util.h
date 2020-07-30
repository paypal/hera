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
#ifndef _OCC_UTIL_H_
#define _OCC_UTIL_H_

#include <string>
#include <vector>

class NetstringWriter;
class ColumnInfo;

class Util
{
public:
	/**
	 * The SQL hash from CAL.
	 */
	static unsigned long sql_hash(const char *sql);
	/**
	 * The SQL hash from CAL, with _sql "transliterated" (new lines replaced with space, etc).
	 */
	static unsigned long sql_CAL_hash(const char *sql);

	static int out_col_names(NetstringWriter* _writer, std::vector<ColumnInfo>* _cols);
	static int out_col_info(NetstringWriter* _writer, std::vector<ColumnInfo>* _cols);

	static std::string get_command_name(int _cmd);

	static void netstring(int _cmd,  const std::string& _payload, std::string& _buff);
};

#endif
