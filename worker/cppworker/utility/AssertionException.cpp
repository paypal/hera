#include <string>
#include "utility/AssertionException.h"

AssertionException::~AssertionException() PPEX_NOTHROW
{
	// WARNING!!!
	// ** DO NOT ** inline this function in the header file!
	// gcc will make several thousand copies of this function if you do and it
	// will increase webscr object code size by 25% and compile time by 35%!
}

std::string AssertionException::get_name(void) const
{
	return "AssertionException"; 
}


void 
throw_assertion_exception(const char* msg)
{
	throw AssertionException(msg);
}
