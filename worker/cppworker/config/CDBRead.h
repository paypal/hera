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
