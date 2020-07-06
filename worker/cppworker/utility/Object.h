#ifndef _NMSI_OBJECT_H_
#define _NMSI_OBJECT_H_

#include <string>

typedef long long long64;
typedef unsigned long long ulong64;

//unique types associated with an object
typedef ulong64 ObjectType;

/**
	The base class for most objects
*/
class Object {
public:

	Object() {};
	virtual ~Object() {};

	// converts the object to string representation
	// pass it a pre-allocated buffer
	// returns 0 for success... -1 for failure
	// must append the contents to str
	virtual int to_string(std::string * str);

	// returns the hash code for an object
	virtual uint hashcode() const;

	// compares itself to obj
	// returns 0 if equal
	// <0 if obj is less than
	// >0 if obj is greater than
	// -1 if different but lt/gt is not important
	// always check for a NULL obj
	// you may assume that type of obj is the same as you
	virtual int equals( const Object * obj ) const;
	
	// clone oneself
	virtual Object * clone();

	// return a unique value
	virtual ObjectType object_type();
};

#endif
