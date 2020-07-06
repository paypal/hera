// TimeUtil.cpp
//
// Various time-related utility functions

//##L10N##
//Do Not Use These Functions!!

#ifndef _TIMEUTIL_H_
#define _TIMEUTIL_H_

#include <time.h>

int tv_add(const struct timeval& a, const struct timeval& b, struct timeval& result);
int tv_subtract(const struct timeval& a, const struct timeval& b, struct timeval& result);

#endif // _TIME_UTIL_H_
