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
#include <time.h>
#include "config/Config.h"
#include "log/LogFactory.h"
#include "worker/OCCCachedResults.h"

std::vector<OCCCachedResults*> OCCCachedResults::cache;

OCCCachedResults * 
OCCCachedResults::get_cache_entry(uint _query_id, Config * _config, LogWriterBase *_logfile)
{
	std::string query_buf;
	std::string max_age_buf;

	// Do we already have an entry for this query_id?
        
	// Linear search in the array, can replace with a hash if necessary
	for (int i = 0; i < cache.size(); i++)
	{
		if (cache[i]->query_id == _query_id)
			return cache[i];
	}

	// No, need a new one. Initialize from config.
	std::ostringstream os;
	os << "special_query_" << _query_id << "_text";
	if (!_config->get_value(os.str(), query_buf))
	{
		_logfile->write_entry(LOG_WARNING, "%s undefined or invalid", os.str().c_str());
		return NULL;
	}
		
	std::ostringstream oss;
	oss << "special_query_" << _query_id << "_max_age";
	if (!_config->get_value(oss.str(), max_age_buf))
	{
		_logfile->write_entry(LOG_WARNING, "%s undefined or invalid", oss.str().c_str());
		return NULL;
	}	

	OCCCachedResults * cache_entry = new OCCCachedResults(_query_id, query_buf, StringUtil::to_int(max_age_buf));
	
	// Stick the singleton cache entry into the global array
	cache.push_back(cache_entry); 

	return cache_entry;
}

OCCCachedResults::OCCCachedResults(uint _query_id, const std::string &_query, ulong _max_age)
{
	// initialize query information
	query_id       = _query_id;
	query          = _query;
	max_age        = _max_age;
	
	// clean out the cache
	expire();
}

void OCCCachedResults::expire()
{
	time_populated = 0;
	num_columns    = 0;
	results.clear();
}

void OCCCachedResults::validate()
{
	time_populated = time(NULL);
}

bool OCCCachedResults::enabled() const
{
	// If the max_age value is 0, caching for the query is turned off
	return (max_age > 0);
}

bool OCCCachedResults::valid() const
{
	if (!enabled())
		return false;

	ulong now = time(NULL);

	return ((time_populated + max_age) >= now);
}

