// On 2020-07, https://github.com/aappleby/smhasher notes that MurmurHash is public domain
#include "HashUtil.h"
#include <string>

// just in this file
static uint32_t MurmurHash3(const char * key, int len);

uint32_t HashUtil::MurmurHash3Sharding(const long long key)
{
	return MurmurHash3(key) % 1024;
}
uint32_t HashUtil::MurmurHash3(const long long key)
{
	return ::MurmurHash3((const char*)&key, sizeof(long long));
}
uint32_t HashUtil::MurmurHash3(std::string key)
{
	const char *shardkey_bytes = const_cast<char*>(key.c_str());
	return ::MurmurHash3(shardkey_bytes, key.size());
}

//https://code.google.com/p/smhasher/wiki/MurmurHash3
// MurmurHash3
#define	FORCE_INLINE inline __attribute__((always_inline))

inline uint32_t rotl32 ( uint32_t x, int8_t r )
{
  return (x << r) | (x >> (32 - r));
}
#define	ROTL32(x,y)	rotl32(x,y)

FORCE_INLINE uint32_t getblock32 ( const uint32_t * p, int i )
{
  return p[i];
}

//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche

FORCE_INLINE uint32_t fmix32 ( uint32_t h )
{
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;

  return h;
}

uint32_t MurmurHash3(const char * key, int len)
{
  const uint8_t * data = (const uint8_t*)key;
  const int nblocks = len / 4;

  // seed for sharding!
  uint32_t h1 = 0x183d1db4;

  const uint32_t c1 = 0xcc9e2d51;
  const uint32_t c2 = 0x1b873593;

  //----------
  // body

  const uint32_t * blocks = (const uint32_t *)(data + nblocks*4);

  for(int i = -nblocks; i; i++)
  {
    uint32_t k1 = getblock32(blocks,i);

    k1 *= c1;
    k1 = ROTL32(k1,15);
    k1 *= c2;
    
    h1 ^= k1;
    h1 = ROTL32(h1,13); 
    h1 = h1*5+0xe6546b64;
  }

  //----------
  // tail

  const uint8_t * tail = (const uint8_t*)(data + nblocks*4);

  uint32_t k1 = 0;

  switch(len & 3)
  {
  case 3: k1 ^= tail[2] << 16;
  case 2: k1 ^= tail[1] << 8;
  case 1: k1 ^= tail[0];
          k1 *= c1; k1 = ROTL32(k1,15); k1 *= c2; h1 ^= k1;
  };

  //----------
  // finalization

  h1 ^= len;

  h1 = fmix32(h1);

  return h1;
} 

