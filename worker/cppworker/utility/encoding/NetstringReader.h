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
#ifndef _NETSTRINGREADER_H_
#define _NETSTRINGREADER_H_

/*
  NetstringReader

  Reads an augmented "netstring" from an input stream
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

class NetstringReader {
public:
	NetstringReader(std::istream * _in);
	~NetstringReader();

	//Reads one netstring from the input stream
	//nested netstrings are hidden from you
	//returns the command value
	//or -1 if there was an error
	//the data is stored in out_buffer (copied)
	int read();
	int read(std::string * out_buffer);

	bool is_buffer_empty();
	bool is_incomplete() { return m_is_incomplete; }
	uint32_t get_count(){ return m_cnt; }

private:
	//where the data comes from
	std::istream * in;
	//temporary storage for subnetstring detection
	std::string * buffer;
	//the index into the current subnetstring
	//this should be zero if there is no subnetstring
	unsigned int subnetstring_index;
	bool m_is_incomplete;

	//for subnetstring support
	int read_next(std::string * out_buffer);

	// counter of how many were read
	uint32_t m_cnt;
};

#endif
