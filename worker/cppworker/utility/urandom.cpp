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
#include <climits>
#include <fcntl.h>
#include <unistd.h>
#include "urandom.h"

/**----------------------------------------------------------------------------
 * @brief rand returns a random uint on [0, UINT_MAX]
 */
uint urandom::rand()
{
	// fill a uint with random data
	uint raw_random;
	read_bytes(&raw_random, sizeof(uint));

	// and return it
	return raw_random;
}

/**----------------------------------------------------------------------------
 * @brief rand(_range) returns a random uint on [0, _range - 1]
 *        _range == 0 returns 0
 *
 * Starts with a random uint and then scales it to fit the requested range.
 */
uint urandom::rand(uint _range)
{
	// get a full-range random uint
	uint raw_random = rand();

	// Even though the low bits from /dev/urandom are just as good as the high
	// bits, scaling is slightly preferable to % for mapping to the requested
	// range.  The math is very cheap compared to the read from /dev/urandom,
	// and the distribution is more uniform.  Worst case for using % would be a
	// requested range of something like 0-0xaaaaaaaa.  raw_random % 0xaaaaaaab
	// would give an average result of 0x471c71c6 instead of the desired
	// 0x55555555.
 
	// calculate the appropriate factor to map the full uint range to the
	// requested range
	double factor = (double)_range / ((double)UINT_MAX + 1);
	// multiply them out for the final result
	return (uint)(raw_random * factor);
}

/**----------------------------------------------------------------------------
 * @brief fill_buffer fills _buffer with _length bytes of random data
 */
void urandom::fill_buffer(std::string& _buffer, uint _length)
{
	// just call the internal version
	_buffer.resize(_length);
	read_bytes((void*)(_buffer.c_str()), _length);
}

/**----------------------------------------------------------------------------
 * @brief fill_buffer fills _buffer with _length bytes of random data
 *
 * This is actually identical to read_bytes, but read_bytes is separate to make
 * the internal/external division clear
 */
void urandom::fill_buffer(void* _buffer, uint _length)
{
	// just call the internal version
	read_bytes(_buffer, _length);
}

/**----------------------------------------------------------------------------
 * read_bytes is the method that actually opens and reads from /dev/urandom.
 * The other methods use this one to get their raw data.
 */
void urandom::read_bytes(void* _buffer, uint _count)
{
	// open /dev/urandom
	int fd = open("/dev/urandom", O_RDONLY);
	if(fd == -1) {
		// throw on failure
		throw urandomException("failed to open /dev/urandom");
	}

	uint bytes_read = 0;
	ssize_t read_rc;

	// read _length bytes into _out
	while(bytes_read < _count)
	{
		// try to read the amount we want
		read_rc = read(fd, (char*)_buffer + bytes_read, _count - bytes_read);
		
		if(read_rc <= 0) {
			// throw on failure
			close(fd);
			throw urandomException("failed to read from /dev/urandom");
		}

		// record the amount read
		bytes_read += read_rc;
	}

	// impossible to reach here unless we've read the full amount
	close(fd);
}
