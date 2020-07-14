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
#ifndef _CalURLSession_h
#define _CalURLSession_h

#include "CalClientSession.h"
#include <string>

/**
 * This class simply manages a top-level URL transaction for CAL.
 */ 
class CalURLSession
{
public:
	/**
	 * This API is used to create the root URL transaction and if the transaction is already
	 * active then it will complete the existing transaction and creates the new one. 
	 */ 
	static bool start(const char* poolname=NULL, const char* ppppname=NULL);

	/**
	 * This API is used to complete the root URL transaction.
	 */ 
	static void end();

	/**
	 * This API is used to indicate whether the url session is active or not.
	 */ 
	static bool is_active();

	/**
	 * This API is used to set the status of the URL session.
	 */ 
	static void set_status(const std::string &_status);

	/**
	 * Getter Method for URL transaction.
	 */ 
	static CalTransaction* get_url_transaction();

private:
	/**
	 * This method is used to get URL session.
	 */ 
	static CalClientSession* get_cal_client_session();
};

#endif /* _CalURLSession_h */
