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

// TimeUtil.cpp
//
// Various time-related utility functions

//##L10N##
//Do Not Use These Functions!!

#ifndef _TIMEUTIL_H_
#define _TIMEUTIL_H_

#include <time.h>

int tv_add(const struct timeval& a, const struct timeval& b, struct timeval& result);
int tv_subtract(const struct timeval& a, const struct timeval& b, struct timeval& result);

#endif // _TIME_UTIL_H_
