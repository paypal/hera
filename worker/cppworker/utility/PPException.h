#ifndef PP_EXCEPTION_H
#define PP_EXCEPTION_H

#include <stdexcept>
#include <boost/shared_ptr.hpp>

#include <string>

#define PPEX_NOTHROW throw()

/** Base class for PayPal exceptions. */
class PPException : public std::exception {
public:
	PPException(const std::string &_message);
	// ctor used for exc chaining, _root_cause is "chained" to this exception
	PPException(const std::string& _message, const PPException& _root_cause);

	/**
	 * Destructor.
	 * See above for meaning of PPEX_NOTHROW.
	 * Yes, all PPException-derived classes will have to have the PPEX_NOTHROW eventually.
	 */
	virtual ~PPException() PPEX_NOTHROW;

	/// get the full debug string of this exception, including the message and
	///  any other fields that might be added in a base class
	virtual std::string get_string(void) const;

	/// derived classes must overload this to return the name of the exception (classname)
	virtual std::string get_name(void) const = 0;

	/// get the text message for this exception
	virtual const std::string &get_message(void) const;

	/**
	 * Implementation of std::exception::what().
	 * See above for meaning of PPEX_NOTHROW.
	 * No, you shouldn't override what() in your PPException-derived classes.
	 */
	virtual const char* what() const PPEX_NOTHROW;

	// chain this exception to _root_cause
	void set_root_cause(const PPException& _root_cause);
protected:
    /// Get the details of this exception as a string
	virtual std::string get_details_string(void) const;

	/// the text message associated with this exception, explaining why it was thrown.
	std::string m_message;

private:
	mutable std::string m_what;
	
	struct PPExceptionSummary
	{
		std::string message;
		boost::shared_ptr<PPExceptionSummary> root_cause_next; 
	};
	boost::shared_ptr<PPExceptionSummary> m_root_cause;
};

#endif
