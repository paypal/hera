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
