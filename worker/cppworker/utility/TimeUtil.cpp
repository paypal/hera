// TimeUtil.cpp
//
// Various time-related utility functions
//
// James Hogan
// 5/23/00



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



