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
#ifndef _OCCCACHEDRESULTS_H_
#define _OCCCACHEDRESULTS_H_

#include <string>
#include <vector>

class Config;
class LogWriterBase;

class OCCCachedResults 
{
public:
	// get or create a results cache object for a particular query
	// once created, each one persists until the process exits
	// the query text and the TTL are initialized from the config
	static OCCCachedResults * get_cache_entry(uint _query_id, Config * _config, LogWriterBase * _logfile);

	// true if caching is enabled for this query, false otherwise
	bool enabled() const;

	// true if the results have not expired, false otherwise
	bool valid() const;
	
	// clear the results and set time_executed to 0
	void expire();

	// set time_executed to now
	void validate();

	// get the text of the query
	const std::string & get_query() const { return query; }

	// accessors and setters
	uint get_num_columns() const { return num_columns; }
	void set_num_columns(uint _num_columns) { num_columns = _num_columns; }
	uint get_num_rows() const { return num_rows; }
	void set_num_rows(uint num_rows_) { num_rows = num_rows_; }

	const std::vector<std::string> & get_results() const { return results; }
	void add_result(const std::string &_value) { results.push_back(_value); }

private:

	// you have to go through the factory method
	OCCCachedResults(uint _query_id, const std::string &_query, ulong _max_age);

	// the query
	std::string query;

	// From OCCCommands.h
	uint query_id;

	// the number of seconds the results are valid
	ulong max_age; 

	// last time the results were captured
	ulong time_populated;

	// the values
	std::vector<std::string> results;

	// the number of columns in the query
	uint num_columns;

	// the number of rows in the result set
	uint num_rows;

	// the global cache
	static std::vector<OCCCachedResults*> cache;

	// Temporary: QueryCache can create objects of this type, without actually putting them in the local cache
	// TODO: implement API to load & store to MayFly
	friend class QueryCache;
};

#endif
