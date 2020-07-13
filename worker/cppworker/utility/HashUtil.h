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
#ifndef HASH_UTIL_H
#define HASH_UTIL_H

#include <stdint.h>

class HashUtil {
public:
	/** Preferred. */
	static uint32_t MurmurHash3Sharding(const long long key);

	/** Deprecated, only for sharding. */
	static uint32_t MurmurHash3(const long long key);
};

#endif // HASH_UTIL_H
