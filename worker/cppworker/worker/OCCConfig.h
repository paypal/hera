#ifndef OCCCONFIG_H
#define OCCCONFIG_H

#include <string>

enum ShardingAlgo
{
	HASH_MOD = 0,
	MOD_ONLY = 1,
};

const unsigned int ABS_MAX_CHILDREN_ALLOWED = 2000;
const int ABS_MAX_SCUTTLE_BUCKETS = 1024;
const std::string DEFAULT_SCUTTLE_ATTR_NAME = "scuttle_id";
const std::string DEFAULT_SHARDING_ALGO = "hash";
const std::string MOD_ONLY_SHARDING_ALGO = "mod";

#endif // OCCCONFIG_H