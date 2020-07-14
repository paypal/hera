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
#include "NetstringWriter.h"
#include "utility/StringUtil.h"
#include <string.h>



NetstringWriter::NetstringWriter(std::ostream * _out)
{
	out = _out;
	*out << std::noskipws;	
	separate();
}

NetstringWriter::~NetstringWriter()
{
}

int NetstringWriter::write(uint code)
{
	return write(code, "");
}

int NetstringWriter::write(uint code, const std::string& value)
{
	return write(code, value.c_str(), value.length());
}

int NetstringWriter::write(uint code, const char * value)
{
	return write(code, value, strlen(value));
}

int NetstringWriter::write(uint code, const char * value, uint value_length)
{
	static std::string buffer;
	buffer.clear(); // reset to zero size...doesn't deallocate memory tho
	add_value(&buffer, code, value, value_length);
	out->write(buffer.c_str(), buffer.length());
	out->flush();
	if (!out->good())
		return -1;
	return 0;
}

int NetstringWriter::add(uint code, const std::string& value)
{
	return add(code, value.c_str(), value.length());
}

int NetstringWriter::add(uint code, const char * value)
{
	return add(code, value, strlen(value));
}

int NetstringWriter::add(uint code, const char * value, uint value_length)
{
	count[count.size() - 1]++;
	return add_value(&(accumulated_msgs[accumulated_msgs.size()-1]), code,
			value, value_length);
}

void NetstringWriter::separate() 
{
	accumulated_msgs.push_back(std::string());
	count.push_back(0);
}

void NetstringWriter::clear() 
{
	accumulated_msgs.clear();
	count.clear();
	separate();
}

int NetstringWriter::write()
{
	std::string rawOut;
	for (int i = 0; i < accumulated_msgs.size(); i++) {
		std::string &accumulated = accumulated_msgs[i];

		// don't write anything if nothing accumulated
		if (count[i] == 0)
			continue;
		// if only one message, don't nest
		if (count[i] == 1) {
			rawOut.append(accumulated);
			continue;
		}

		// write the accumulated sub-netstring buffer
		std::string tmpOut;
		add_value(&tmpOut, 0, accumulated.c_str(), accumulated.length());
		rawOut.append(tmpOut);

		accumulated.clear();
	}
	accumulated_msgs.clear();
	count.clear();
	separate();

	if (rawOut.empty()) {
		return 0;
	}
	out->write(rawOut.c_str(), rawOut.length());
	out->flush();
	if (!out->good()) {
		return -1;
	}
	return 0;
}

int NetstringWriter::add_value(std::string * out_buffer, uint code, const char * value, uint value_length)
{
	//
	// During 28.0 development, we found that this key method was
	// called literally hundreds of thousands of times in a busy run.
	//
	// Previously, it was implemented using lots of std::string::append()
	// calls; since each of those has memory allocation and
	// NUL-checking overhead, the method was reimplemented using as
	// few std::string method calls as possible.
	//
	// Basically, we-pre-calculate the total length of the buffer by
	// formatting the code and payload length strings (ASCII numbers),
	// then taking the length of those plus the Netstring format
	// overhead, and allocating a std::string with that length directly.
	//

	char code_str[14];
	char payload_len_str[14];
	uint total_length;
	uint code_length;
	uint payload_len_str_length;

	total_length = value_length;
	if(value_length>0) {
		//account for space
		total_length++;
	}

	// Preformat the return code (an integer in ASCII characters) so
	// we can calculate the total length of this Netstring
	code_length = StringUtil::fmt_uint(code_str,code);
	total_length+=code_length;

	// Preformat the payload length (an integer in ASCII characters)
	// so we can calculate the total length of this Netstring
	payload_len_str_length = StringUtil::fmt_uint(payload_len_str,total_length);

	uint buffer_len;

	if (value_length > 0)
	{
		// value_length > 0 means we have a Netstring with a payload.
		// The format of such a Netstring is:
		//
		// total_length:code payload,
		//
		// So, we'll sum up these lengths.

		buffer_len = payload_len_str_length +  // exact size for payload_len_str
		             code_length +             // exact size for code_str
		             value_length +            // exact size for value
		             3;                        // colon, space, comma
	}
	else
	{
		// value_length == 0 means we have a Netstring with no
		// payload.  The format of such a Netstring is:
		//
		// total_length:code,

		buffer_len = payload_len_str_length +  // exact size for payload_len_str
		             code_length +             // exact size for code_str
		             2;                        // colon, comma
	}

	// Since this method appends to an existing std::string, we'll need to
	// start writing this many bytes in to out_buffer.
	int current_length = out_buffer->length();

	// Make sure we have enough room to append the new Netstring into
	// out_buffer.
	out_buffer->resize(current_length + buffer_len);
	char *buf = (char*)(out_buffer->c_str());
	
	// Append after the current contents of out_buffer
	buf += current_length;

	// No matter the format, we need to copy the payload length string
	memcpy(buf, payload_len_str, payload_len_str_length);
	buf += payload_len_str_length;

	// Between the payload length and code comes a single colon
	*buf = ':';
	buf += 1;

	// Next, all Netstrings need a code string
	memcpy(buf, code_str, code_length);
	buf += code_length;

	if (value_length > 0)
	{
		// Netstrings with a payload need a space, then the payload
		*buf = ' ';
		buf += 1;

		memcpy(buf, value, value_length);
		buf += value_length;
	}

	// Whether or not we have a payload, we need to append a comma at
	// the end
	*buf = ',';

	return 0;
}
