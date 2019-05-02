// Package utility defines generic utilitarian functions
package utility

// Fowler/Noll/Vo- hash
// FNV hashes are designed to be fast while maintaining a low
// collision rate. The FNV speed allows one to quickly hash lots
// of data while maintaining a reasonable collision rate.  See:
//
//      http://www.isthe.com/chongo/tech/comp/fnv/index.html
//
// for more details as well as other forms of the FNV hash.

// GetSQLHash calculates FNV hash
func GetSQLHash(sqltext string) (sqlhash uint32) {
	var hash uint64 = 0xcbf29ce484222325
	for i := 0; i < len(sqltext); i++ {
		hash ^= uint64(sqltext[i])
		hash *= 0x100000001b3
	}
	var lo = uint32(hash & 0x00000000ffffffff)
	var hi = uint32((hash >> 32) & 0x00000000ffffffff)
	return hi ^ lo
}
