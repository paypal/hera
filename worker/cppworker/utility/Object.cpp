#include <string.h>
#include <sstream>
#include "utility/Object.h"



int Object::to_string(std::string * str)
{
	std::ostringstream os;
	os << "Object@" << hashcode();
	*str = os.str();
	return 0;
}

Object * Object::clone()
{
Object * ret = new Object;
	memcpy( ret, this, sizeof(Object));		// a very poor way to copy objects
	return ret;
}

unsigned int Object::hashcode() const
{
// should override this one
int mycode = 0;
const int * me = (const int *) this; // point at myself
	
	for(unsigned int i=0; i<sizeof(Object)/sizeof(int); i++) {
		mycode +=*me;
		me++;
	}
	return mycode;
}

int Object::equals( const Object * obj ) const
{
	// equality test
	if (obj == this)		// for pure Objects, equality means the _same_ object
		return 0;

	return -1;
}

ObjectType Object::object_type()
{
	//Unknown object type
	return 0;
}
