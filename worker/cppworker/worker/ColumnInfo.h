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
#ifndef _COLUMNINFO_H_
#define _COLUMNINFO_H_

#include <string>
#include <oci.h>

struct ColumnInfo
{
	std::string name;
	ub2	   type;
	ub2    width;
	ub1    precision;
	ub1    scale;

	ColumnInfo();
	ColumnInfo(const ColumnInfo& _col);
	ColumnInfo& operator = (const ColumnInfo& _col);

private:
	void copy(const ColumnInfo& _col);
};

#endif
