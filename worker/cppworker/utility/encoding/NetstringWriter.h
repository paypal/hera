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
#ifndef _NETSTRINGWRITER_H_
#define _NETSTRINGWRITER_H_

/*
  NetstringWriter

  Writes an augmented "netstring" from an input stream
  ftp://koobera.math.uic.edu/www/proto/netstrings.txt

  The data portion of the netstring is expected to be
  COMMAND [' ' data]
  Where COMMAND is a base-10 positive integer.  If it is a command that contains additional
  data, then it is followed by a single space and the data.

  This also supports 1-level nested netstrings.  Nested netstrings are indicated by
  a command value of '0'.

*/

#include <string>
#include <iostream>
#include <vector>

class NetstringWriter
{
public:
	NetstringWriter(std::ostream * _out);
	~NetstringWriter();

	//these write directly to the output stream
	//returns 0 on success, -1 on failure
	int write(uint code);
	int write(uint code, const std::string& value);
	int write(uint code, const char * value);
	int write(uint code, const char * value, uint value_length);

	//these add to the "accumulated" netstring
	//Note:  it would probably be a little more "elegant" to set up an outputstream
	//to a string object, and then send that string object to the writer, but
	//this seems a little more convenient
	int add(uint code, const std::string& value);
	int add(uint code, const char * value);
	int add(uint code, const char * value, uint value_length);
	// creates a new netstring message, does not flush current accumulation
	void separate();
	//this will flush the "accumulated" netstring
	int write();
	//this will clear the "accumulated" netstring
	void clear();

private:
	std::ostream * out;
	std::vector<std::string> accumulated_msgs;		//used for subnetstrings
	std::vector<int>   count;

	//this will create a netstring with the correct format in "buffer"
	//the string is appended to buffer
	int add_value(std::string * out_buffer, uint code, const char * value, uint value_length);
};

#endif
