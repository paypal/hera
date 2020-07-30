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
#ifndef OCCCONFIG_H
#define OCCCONFIG_H

#include <string>

enum ShardingAlgo
{
	HASH_MOD = 0,
	MOD_ONLY = 1,
};

const unsigned int ABS_MAX_CHILDREN_ALLOWED = 2000;
const int ABS_MAX_SCUTTLE_BUCKETS = 1024;
const std::string DEFAULT_SCUTTLE_ATTR_NAME = "scuttle_id";
const std::string DEFAULT_SHARDING_ALGO = "hash";
const std::string MOD_ONLY_SHARDING_ALGO = "mod";

#endif // OCCCONFIG_H
