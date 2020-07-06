#ifndef SHARDKEY_UTIL_H
#define SHARDKEY_UTIL_H

/*
 * ShardKeyUtil.h
 */

#include <string>
#include <vector>

class ShardKeyUtil
{
public:
	static void gen_shard_key(const std::string& _key_name, std::vector<std::string>& _values,
		std::string& _shard_info);

	static int parse_shard_key(const std::string& _shard_info, std::string& _key_name,
		std::vector<std::string>& _key_values);

	static void process_bind_name(const std::string& _name, std::string& _res_name);
	static void process_bind_name2(const std::string& _name, std::string& _res_name);


private:
	static void append_escape(std::string& _dest, const std::string& _src);
	static int tokenize(const std::string& _str, char _escape, char _sep,
						std::vector<std::string>& _values);
};

#endif //SHARDKEY_UTIL_H
