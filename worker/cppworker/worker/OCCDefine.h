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
#ifndef _OCCDEFINE_H_
#define _OCCDEFINE_H_

#include <oci.h>

//-----------------------------------------------------------------------------

struct column_output {
	ub2 type;
	char* data;
	int column_size;
	ub2 str_size;
	sb2 indicator;
	OCILobLocator* lob;
	OCIDateTime* datetime;
};

//-----------------------------------------------------------------------------

class OCCDefine
{
public:
	OCCDefine();
	~OCCDefine();

	// methods return 0 on success, -1 on failure
	// if the error was an oci error, it can be retrieved with get_oci_rc()
	
	// can only be called once per instance
	// envhp is required for types SQLT_BLOB and SQLT_CLOB
	int init(int _rows, int _column_size, ub2 _type, OCIEnv* _envhp, char *data_buf = NULL, sb2 *indicator_buf = NULL, ub2 * str_size_buf = NULL);
	int clear_indicators();
	int define_by_pos(OCIStmt* stmthp, OCIError* errhp, int column_pos, 
						ub4 oracle_lobprefetch_size = 0);
	int get_column(int i, column_output* output);

	int get_oci_rc();

	// accessors
	int get_num_rows(void) const { return rows; }
	int get_column_size(void) const { return column_size; }
	ub2 get_type(void) const { return type; }

protected:
	// amount and type of data to receive
	int rows;
	int column_size;
	ub2 type;

	// output buffers
	// string data is in one combined buffer for all rows, for SQLT_STR type only
	bool own_data;
	char* data;
	// LOB locators, for SQLT_BLOB and SQLT_CLOB types
	OCILobLocator** lob;
	// SQLT_TIMESTAMP
	OCIDateTime** datetime;
	// NULL indicators
	bool own_indicator;
	sb2* indicator;
	// string data size;
	bool own_str_size;
	ub2* str_size;

	// oracle define
	OCIDefine* define;

	int oci_rc;
	int initialized;
};

#endif
