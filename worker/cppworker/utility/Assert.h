#ifndef ASSERT_H
#define ASSERT_H

#include "AssertionException.h"
#include <sstream>

/**
 * ASSERT(EXPR) tests EXPR and, if false, throws AssertionException with
 * a message identifying the EXPR and the location.
 */
#define ASSERT(expr) do { if (!(expr)) { std::ostringstream os; os << "assertion '" << #expr << "' failed in " << __FILE__ << " function " << __PRETTY_FUNCTION__ <<", line " << __LINE__; throw AssertionException(os.str()); } } while (0)

/**
 * ASSERT_MSG(EXPR,MSG) tests EXPR and, if false, throws an
 * AssertionException with a message identifying the EXPR, the
 * location and a given MSG.  MSG is a list of parameters, enclosed in
 * parentheses, compatible with String::copy_formatted().
 *
 * Example:  ASSERT_MSG(val >= 24,("val (%d) is too small",a));
 * If val is 10, this would throw an AssertionException with the message
 *
 *     assertion 'val >= 24' failed in function ...,
 *     line ...: val (10) is too small
 *
 * Example:
 *   int failure = hashtable.insert(key,value);
 *   ASSERT_MSG(!failure,AssertionException,("duplicate key %S",key.uchars()));
 *
 * Both these examples illustrate how we often need more meaningful
 * exceptions with real data in the message.
 */
#define ASSERT_MSG(EXPR,MSG) do { if (!(EXPR)) { ::String msg_; msg_.copy_formatted("assertion '%s' failed in %s function %s, line %d: ", #EXPR, __FILE__, __PRETTY_FUNCTION__, __LINE__); msg_.append_formatted MSG; throw AssertionException(msg_); } } while (0)

/**
 * ASSERT_EXC(EXPR,EXC) tests EXPR and, if false, throws an exception of
 * type EXC with a message identifying the EXPR and the location.
 */
#define ASSERT_EXC(EXPR,EXC) do { if (!(EXPR)) { ::String msg; msg.copy_formatted("assertion '%s' failed in %s function %s, line %d", #EXPR, __FILE__, __PRETTY_FUNCTION__, __LINE__); throw EXC(msg); } } while (0)

/**
 * Deprecated, orignially for use in header files
 */
#define HEADER_ASSERT(EXPR) ASSERT(EXPR)
#define HEADER_ASSERT_MSG(EXPR,MSG) ASSERT_MSG(EXPR,MSG)
#define HEADER_ASSERT_EXC(EXPR,EXC) ASSERT_EXC(EXPR,EXC)


/*
 * ASSERT() that two ostream<< compatible values compare according to OP
 */
#define ASSERT_OP(OP, LHS, RHS, NOP, SLHS, SRHS)                             \
	do { decltype(LHS) lhs = LHS; decltype(RHS) rhs = RHS;                       \
	if (!(lhs OP rhs)) {                                                     \
		std::stringstream s;                                                 \
		s << lhs << #NOP << rhs;                                             \
		::String msg;                                                        \
		msg.copy_formatted(                                                  \
			"assertion '%s' failed (%s) in %s"               \
			" function %s, line %d", SLHS " " #OP " " SRHS, s.str().c_str(), \
			__FILE__, __PRETTY_FUNCTION__, __LINE__);                          \
		throw AssertionException(msg);                                       \
	} } while (0)

// You must #include <sstream> to use these
#define ASSERT_EQ(LHS,RHS) ASSERT_OP(==,LHS,RHS,!=,#LHS,#RHS)
#define ASSERT_NE(LHS,RHS) ASSERT_OP(!=,LHS,RHS,==,#LHS,#RHS)
#define ASSERT_LT(LHS,RHS) ASSERT_OP(<,LHS,RHS,>=,#LHS,#RHS)
#define ASSERT_GT(LHS,RHS) ASSERT_OP(>,LHS,RHS,<=,#LHS,#RHS)
#define ASSERT_GE(LHS,RHS) ASSERT_OP(>=,LHS,RHS,<,#LHS,#RHS)
#define ASSERT_LE(LHS,RHS) ASSERT_OP(<=,LHS,RHS,>,#LHS,#RHS)
#define ASSERT_NOT_NULL(PTR) ASSERT_OP(!=,PTR,(decltype(PTR))NULL,==,#PTR,"NULL")
#define ASSERT_NULL(PTR)     ASSERT_OP(==,PTR,(decltype(PTR))NULL,!=,#PTR,"NULL")

#endif
