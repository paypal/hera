#include "NetstringReader.h"
#include "utility/FileUtil.h"


/*
  by Eric Huss

  Copyright 1999 Confinity
*/

NetstringReader::NetstringReader(std::istream * _in): m_cnt(0)
{
	in = _in;
	*in >> std::noskipws;
	buffer = new std::string();
	subnetstring_index = 0;
	m_is_incomplete = false;
}
NetstringReader::~NetstringReader()
{
	delete buffer;
}

int NetstringReader::read()
{
	std::string tmp;
	return read(&tmp);
}

int NetstringReader::read(std::string * out_buffer)
{
char ch = 0;
int length = 0;
int code = 0;
int rc;

	if(subnetstring_index!=0) {
		//there is data to be read from the buffer
		rc = read_next(out_buffer);
		if(rc!=-1) {
			return rc;
		}
		//out of data...read from the input stream
	}

	//read directly from the input stream
	out_buffer->clear();
	//read the netstring size
	while (true) {
		(*in) >> ch;
		if (in->good()) {
			rc = 0;
		} else {
			if (in->eof()) {
				rc = 0;
			} else {
				rc = -1;
			}
			break;
		}
		if(ch<'0' || ch>'9') {
			break;
		}
		length = length * 10 + (ch-'0');
	} 

	if(rc!=1) m_is_incomplete = true;
	if(rc==-1) return -1;
	//check for seperator
	if(ch!=':') {
		//failed to read proper netstring
		return -1;
	}
	//read the code
	while (true) {
		(*in) >> ch;
		if (in->good()) {
			rc = 0;
		} else {
			if (in->eof()) {
				rc = 0;
			} else {
				rc = -1;
			}
			break;
		}
		if(ch<'0' || ch>'9') {
			break;
		}
		code = code * 10 + (ch-'0');
		length--;
	}
	if(rc!=1) m_is_incomplete = true;
	if(rc==-1) return -1;
	//check for seperator
	if(ch==',') {
		//zero length string
		m_cnt++;
		return code;
	}
	if(ch!=' ') {
		//failed to read proper netstring
		return -1;
	}
	length--;
	//read the value
	//assume we are going to read a nested netstring
	buffer->clear();
	buffer->resize(length);
	if (!FileUtil::read_full(in, buffer, length)) {
		//failed to read total netstring
		buffer->clear();
		m_is_incomplete = true;
		return -1;
	}
	//check for terminator
	(*in) >> ch;
	if((!in->good()) || ch!=',') {
		//failed to read terminator
		if (!in->good()) m_is_incomplete = true;
		return -1;
	}
	//check for subnetstring
	if(code==0) {
		//yes, it is
		return read_next(out_buffer);
	} else {
		//normal...copy into the user's buffer
		*out_buffer = *buffer;
		m_cnt++;
		return code;
	}
}

int NetstringReader::read_next(std::string * out_buffer)
{
char ch = 0;
int length = 0;
int code = 0;

	//at this point, "buffer"+subnetstring_index should
	//start with the data we want
	out_buffer->clear();

	if (subnetstring_index > (uint) buffer->length())
	{
		buffer->clear();
		subnetstring_index = 0;
		return -1;
	}

	const char* raw_buffer = buffer->c_str();

	//read the netstring size
	while (subnetstring_index < (uint) buffer->length())
	{
		ch = raw_buffer[subnetstring_index++];
		if(ch<'0' || ch>'9') {
			break;
		}
		length = length * 10 + (ch-'0');
	}
	//check for seperator
	if(ch!=':') {
		//failed to read proper netstring
		buffer->clear();
		subnetstring_index = 0;
		return -1;
	}
	//read the code
	while (subnetstring_index < (uint) buffer->length())
	{
		ch = raw_buffer[subnetstring_index++];
		if(ch<'0' || ch>'9') {
			break;
		}
		code = code * 10 + (ch-'0');
		length--;
	}
	//check for seperator
	if(ch==',') {
		//zero length string
		out_buffer->clear();
		m_cnt++;
		return code;
	}
	if(ch!=' ') {
		//failed to read proper netstring
		buffer->clear();
		subnetstring_index = 0;
		return -1;
	}
	length--;
	//read the value
	if (subnetstring_index + length > (uint) buffer->length())
	{
		buffer->clear();
		subnetstring_index = 0;
		return -1;
	}
	out_buffer->assign(raw_buffer + subnetstring_index, length);
	//check for terminator
	subnetstring_index+=length;
	ch = (*buffer)[subnetstring_index];
	if(ch!=',') {
		//failed to read terminator
		return -1;
	}
	//skip the comma
	subnetstring_index++;
	m_cnt++;
	return code;
}

bool NetstringReader::is_buffer_empty()
{
	// netstream buffer is empty and the included stream's buffer is empty
	return (((0 == subnetstring_index) || (subnetstring_index == (uint) buffer->length())) &&
			(in->rdbuf()->in_avail() <= 0));
}

