#ifndef HASH_UTIL_H
#define HASH_UTIL_H

#include <stdint.h>

class HashUtil {
public:
	/** Preferred. */
	static uint32_t MurmurHash3Sharding(const long long key);

	/** Deprecated, only for sharding. */
	static uint32_t MurmurHash3(const long long key);
};

#endif // HASH_UTIL_H
