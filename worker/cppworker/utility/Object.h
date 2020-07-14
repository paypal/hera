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
