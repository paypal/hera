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

// TimeUtil.cpp
//
// Various time-related utility functions



#include <sys/time.h>
#include <time.h>

#include "utility/TimeUtil.h"

// -----------------------------------------------------------------------------------------------------------------------------
// normalize a struct timeval after doing add/subtract
// for internal use only
static int tv_normalize(struct timeval& tv)
{
	// now normalize the result
	if ((tv.tv_sec > 0) && (tv.tv_usec < 0))
	{
		tv.tv_sec--;
		tv.tv_usec += 1000000;
		return 1;
	}
	if ((tv.tv_sec < 0) && (tv.tv_usec > 0))
	{
		tv.tv_sec++;
		tv.tv_usec -= 1000000;
		return -1;
	}
	if (tv.tv_usec >= 1000000)
	{
		tv.tv_sec++;
		tv.tv_usec -= 1000000;
		return 1;
	}
	if (tv.tv_usec <= -1000000)
	{
		tv.tv_sec--;
		tv.tv_usec += 1000000;
		return -1;
	}

	// just figure out the sign
	if ((tv.tv_sec > 0) || (tv.tv_usec > 0))
		return 1;
	if ((tv.tv_sec < 0) || (tv.tv_usec < 0))
		return -1;
	return 0;
}

// -----------------------------------------------------------------------------------------------------------------------------
// this function subtracts one struct timeval from another.
// it is not well-defined what the result will be if either of the input
// structs is mal-formed (e.g. the tv_usec is < 0 or > 999999)
// return value is 1 if (a > b), 0 if (a == b), and -1 if (a < b)
// note that if (a < b) then you will get an output like { -1, -500000 }
int tv_add(const struct timeval& a, const struct timeval& b, struct timeval& out)
{
	// do initial add...
	out.tv_sec = a.tv_sec + b.tv_sec;
	out.tv_usec = a.tv_usec + b.tv_usec;
	return tv_normalize(out);
}

// -----------------------------------------------------------------------------------------------------------------------------
// this function subtracts one struct timeval from another.
// it is not well-defined what the result will be if either of the input
// structs is mal-formed (e.g. the tv_usec is < 0 or > 999999)
// return value is 1 if (a > b), 0 if (a == b), and -1 if (a < b)
// note that if (a < b) then you will get an output like { -1, -500000 }
int tv_subtract(const struct timeval& a, const struct timeval& b, struct timeval& out)
{
	// do initial subtract...
	out.tv_sec = a.tv_sec - b.tv_sec;
	out.tv_usec = a.tv_usec - b.tv_usec;
	return tv_normalize(out);
}



