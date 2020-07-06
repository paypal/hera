#include <fcntl.h>
#ifdef SOLARIS_PORT
#define _XPG4_2 // to get munmap(void*,size_t) decl.
#endif
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

#include <config/CDBCommon.h>
#include "CDBRead.h"

//converts from little endian to native format
static inline unsigned int unpack(const unsigned char *buf)
{
	unsigned int num;

	num = buf[3]; num <<= 8;
	num += buf[2]; num <<= 8;
	num += buf[1]; num <<= 8;
	num += buf[0];
	return num;
}

CDBRead::CDBRead(std::ifstream &_in): file_contents((std::istreambuf_iterator<char>(_in)), std::istreambuf_iterator<char>())
{
	/*
	_in.seekg(0, std::ios::end);
    size_t len = _in.tellg();
    _in.seekg(0);
	file_contents.reserve(len + 1);
    _in.read(&file_contents[0], len);	
	*/
}

CDBRead::~CDBRead()
{
}

bool CDBRead::get(const std::string& key, std::string &value)
{
	const char *packbuf;
	uint position;
	uint h;
	uint lenhash;
	uint h2;
	uint loop;
	uint poskd;
	uint newpos;
	uint data_length;

	if (key.size() == 0) {
		return false;
	}

	h = hash(key.c_str(), key.size());
	position = 8 * (h & 255);
	if (position + 8 > file_contents.size()) return -1;
	packbuf = file_contents.c_str() + position;

	position = unpack((unsigned char *)packbuf);
	lenhash = unpack((unsigned char *)packbuf + 4);

	if (!lenhash) return false;
	h2 = (h >> 8) % lenhash;

	for (loop = 0;loop < lenhash;loop++) {
		newpos = position + 8 * h2;
		//read the position
		if (newpos + 8 > file_contents.size()) return -1;
		packbuf = file_contents.c_str() + newpos;
		poskd = unpack((unsigned char *)packbuf + 4);
		if (!poskd) return 0;
		if (unpack((unsigned char *)packbuf) == h) {
			if (poskd + 8 > file_contents.size()) return -1;
			packbuf = file_contents.c_str() + poskd;
			if(unpack((unsigned char *)packbuf) == key.size())
			{
				poskd += 8;  // account for the 8 bytes we just read
				switch(match(key.c_str(), key.size(), poskd)) {
				case -1:
					return false;
				case 1:
					data_length = unpack((unsigned char *)packbuf + 4);
					//FOUND! read the data

					poskd += key.size();
					value.assign(file_contents.c_str() + poskd, data_length);

					return true; // Found it !
				}
			}
		}
		if (++h2 == lenhash) h2 = 0;
	}
	// key not found
  return false;
}

unsigned int CDBRead::hash(const char * key, int length)
{
	unsigned int h;

	if(length < 0)
	{
		length = strlen(key);
	}

	h = 5381;
	while (length--)
	{
		h += (h << 5);
		h ^= (unsigned int) *key++;
	}
	return h;
}

int CDBRead::match(const char * _key, const int _key_length, uint& file_offset)
{
	if (_key_length + file_offset > file_contents.size()) {
		return -1;
	}

	if (memcmp(file_contents.c_str() + file_offset, _key, _key_length) != 0) {
		return 0;
	}
	return 1;
}

// Get all the values inside this CDB file into a Hashtable
bool CDBRead::get_all_values (std::unordered_map<std::string,std::string>& _values_out) {
	const char *values = file_contents.c_str();

	// Clear the result hashtable...
	_values_out.clear();

	// get the first hashtable's location - this is the end of the key/value pairs
	// length is the offset where the last piece of data in the cdb is located.
	// relative to the current location.
	// index starts out at 2048 since there are 256 hashtable pointer entries, each taking up 8 bytes
	// (4 for offset, 4 for length)
	unsigned int length = unpack((const unsigned char *)values);
	unsigned int index = 2048;	// We need to skip over the hashtable pointer/length area
	
	// Loop while we still have keys/values
	while (length>index) {
		uint keylength=0, datalength=0;

		// 4 bytes for the key length, 4 for the data length
		keylength = unpack((const unsigned char*) values+index);
		datalength = unpack((const unsigned char*) values+index+4);
		index += 8;

		// key
		std::string key(values+index, keylength);
		index += keylength;

		// data
		std::string data(values+index, datalength);
		index += datalength;

		// put it into our result Hashtable except for charset key
		if(key != CDB_KEY_CHARSET)
			_values_out[key] = data;
	}

	return true;
}

