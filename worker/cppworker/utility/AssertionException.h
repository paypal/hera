#ifndef ASSERTION_EXCEPTION_H
#define ASSERTION_EXCEPTION_H

#include "utility/PPException.h"
#include <string>

/** Simple class for Assertion exceptions, derived from PPException */

class AssertionException : public PPException
{
public:
	AssertionException(const std::string &_message) : PPException(_message) { }
	virtual ~AssertionException() PPEX_NOTHROW;

	virtual std::string get_name(void) const;
};

void throw_assertion_exception(const char* msg);

#endif
