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
#include "SQLRewriter.h"
#include "cal/CalMessages.h"
#include "log/LogFactory.h"
#include "worker/Util.h"
#include <string.h>

/**
 * This class modifies the SQLs to inject a scuttle_id condition everywhere a shard key is present. examples:
 * Select party_id, name from party where party_id=:party_id
 * select ... JOIN ON a.party_id=b.party_id ... where party_id=:party_id
 * insert party (..., party_id, ...) values (..., :party_id, ...)
 * update party (...) set (...) where ... party_id=:party_id
 * Become respectively
 * Select party_id, name from party where party_id=:party_id AND scuttle_id=:scuttle_id
 * select ... JOIN ON a.party_id=b.party_id  AND a.scuttle_id=b.scuttle_id ... where party_id=:party_id AND scuttle_id=:scuttle_id
 * insert party (..., party_id, scuttle_id, ...) values (..., :party_id, :scuttle_id, ...)
 * update party (...) set (...) where ... party_id=:party_id AND scuttle_id=:scuttle_id
 *
 */

/*
const std::string SQLRewriter::sSCUTTLE_ID_WHERE_EQ = "scuttle_id = :scuttle_id ";
const std::string SQLRewriter::sDOT_SCUTTLE_ID_EQ = ".scuttle_id = ";
const std::string SQLRewriter::sDOT_SCUTTLE_ID = ".scuttle_id";
const std::string SQLRewriter::sSCUTTLE_ID = "scuttle_id";
const std::string SQLRewriter::sCOMMA_SCUTTLE_ID = ", :scuttle_id";
*/
const std::string SQLRewriter::sAND = " AND ";
const std::string SQLRewriter::sCOMMA = ", ";

SQLRewriter::SQLRewriter()
{
}

void SQLRewriter::init(const std::string& _shard_key_name, const std::string& _scuttle_attr_name) {
	m_sSCUTTLE_NAME = _scuttle_attr_name;
	std::ostringstream os;
	os << m_sSCUTTLE_NAME << " = :" << m_sSCUTTLE_NAME << " ";
	m_sSCUTTLE_ID_WHERE_EQ = os.str();
	os.str("");
	os << "." << m_sSCUTTLE_NAME << " = ";
	m_sDOT_SCUTTLE_ID_EQ = os.str();
	os.str("");
	os << "." << m_sSCUTTLE_NAME;
	m_sDOT_SCUTTLE_ID = os.str();
	m_sSCUTTLE_ID = m_sSCUTTLE_NAME;
	os.str("");
	os << ", :" << m_sSCUTTLE_NAME;
	m_sCOMMA_SCUTTLE_ID = os.str();
	
	
	m_shard_key_name = _shard_key_name;
	os.str("");
	os << ":" << m_shard_key_name;
	m_colon_shard_key_name = os.str();
	const char* query_type_needles[] = {"select", "insert", "update", "delete", "declare", "begin"};
	m_query_type_finder.init(query_type_needles, sizeof(query_type_needles) / sizeof(const char*));
	const char* select_needles[] = {"select", "from", "where", "join", NULL /*csk*/, NULL /*sk*/};
	select_needles[4] = m_colon_shard_key_name.c_str();
	select_needles[5] = m_shard_key_name.c_str();
	m_select_finder.init(select_needles, sizeof(select_needles) / sizeof(const char*));

}

SQLRewriter::Finder::Finder(): m_needles(0), m_ptrs(0), m_count(0) {
}

void SQLRewriter::Finder::init(const char** needles, int count) {
	clear();
	m_needles = new const char*[count];
	m_ptrs = new const char*[count];
	for (int i = 0; i < count; i++) {
		m_needles[i] = strdup(needles[i]);
		m_ptrs[i] = m_needles[i];
	}
	m_count = count;
}

void SQLRewriter::Finder::clear() {
	for (int i = 0; i < m_count; i++) {
		free((void*)m_needles[i]); // allocated with strdup()
	}
	delete m_needles;
	delete m_ptrs;
	m_needles = 0;
	m_ptrs = 0;
	m_count = 0;
}

SQLRewriter::Finder::~Finder() {
	clear();
}

bool SQLRewriter::Finder::eq_case_i(char c1, char c2) {
	const int DELTA = 'A' - 'a';
	return (((int)c1 == (int)c2) || ((int)c1 == (int)c2 + DELTA) || ((int)c1 == (int)c2 - DELTA));
}

bool SQLRewriter::Finder::find(const char* haystack) {
	for (int i = 0; i < m_count; i++)
		m_ptrs[i] = m_needles[i];
	m_next = 0;
	bool prev_alpha = false;
	while (*haystack) {
		bool alpha = is_alpha_underscore(*haystack) || (*haystack == ':');
		for (int i = 0; i < m_count; i++) {
			if (	(alpha)
					&&
					/* the needle has to be a token, not part of a token. i.e it has separators to the left*/
					(	(!prev_alpha) ||
						(m_ptrs[i] != m_needles[i])
					)
					&&
					eq_case_i(*haystack, *m_ptrs[i])) {
				m_ptrs[i]++;
				m_next = haystack + 1;
				if (*m_ptrs[i] == 0) {
					if (!is_alpha_underscore(*m_next) /*it has separator at the right*/) {
						m_found = i;
						m_next = haystack + 1;
						return true;
					} else {
						m_ptrs[i] = m_needles[i]; // reset
					}
				}
			} else {
				m_ptrs[i] = m_needles[i]; // reset
			}
		}
		prev_alpha = alpha;
		haystack++;
	}
	return false;
}

const char* SQLRewriter::Finder::next() {
	return m_next;
}

int SQLRewriter::Finder::found() {
	return m_found;
}

bool SQLRewriter::is_alpha_underscore(char c) {
	return (isalpha(c) || isdigit(c) || (c == '_'));
}

SQLRewriter::SqlType SQLRewriter::get_type(const std::string& _sql) {
	if (m_query_type_finder.find(_sql.c_str())) {
		return (SqlType)m_query_type_finder.found();
	}
	return UNKNOWN;
}

bool SQLRewriter::has_scuttle_id(const std::string& _sql) {
	return (0 != strcasestr(_sql.c_str(), m_sSCUTTLE_NAME.c_str()));
}

void SQLRewriter::rewrite_select(const std::string& _sql)
{
	RewriteInfo val;
	val.rewritten = false;
	const char* query = _sql.c_str();
	const char* start = query;
	const char* next = query;
	int last_token = -1;
	bool inner_select = false;
	while (true) {
		if (!m_select_finder.find(start))
			break;
		next = m_select_finder.next();
		switch (m_select_finder.found()) {
			case 0: // SELECT
				inner_select = (last_token == 1 /*FROM*/);
				val.sql.append(start, next - start);
				break;
			case 1: // FROM
			case 2: // WHERE
			case 3: // JOIN
				val.sql.append(start, next - start);
				break;
			case 4: // found the shard key
			case 5: // found the shard key
				val.rewritten = true;
				switch (last_token) {
					case 2: //WHERE
					{
						int alias_len = 0;
						const char *alias = 0;
						get_alias(start, next - m_shard_key_name.length(), alias, alias_len);
						// skip whitespace
						while (isspace(*next))
							next++;
						// expect '='
						if (*next == '=') {
							next++;
							// skip whitespace
							while (isspace(*next))
								next++;
							// expect ':'
							if (*next != ':') {
								return rewrite_asis(_sql, ERR_SELECT_WHERE);
							}
							next++;
							// skip whitespace
							while (isspace(*next))
								next++;
							// expect varbind name same as shard key name
							if (strncasecmp(next, m_shard_key_name.c_str(), m_shard_key_name.length()) != 0) {
								return rewrite_asis(_sql, ERR_SELECT_WHERE);
							}
							next += m_shard_key_name.length();
							if (isalpha(*next) || isdigit(*next)) {
								return rewrite_asis(_sql, ERR_SELECT_WHERE);
							}
							if (*next == '_') {
								next++;
								if (*next == '0') {
									next ++;
								} else {
									return rewrite_asis(_sql, ERR_SELECT_WHERE);
								}
							}
						} else {
							// see if this is an IN clause
							if (strncasecmp(next, "IN", 2) != 0) {
								return rewrite_asis(_sql, ERR_SELECT_WHERE);
							}
							next += 2;
							while (isspace(*next))
								next++;
							if (*next != '(') {
								return rewrite_asis(_sql, ERR_SELECT_WHERE);
							}
							next++;
							while (isspace(*next))
								next++;
							if (*next != ':') {
								return rewrite_asis(_sql, ERR_SELECT_WHERE);
							}
							next++;
							while (isspace(*next))
								next++;
							// expect varbind name same as shard key name
							if (strncasecmp(next, m_shard_key_name.c_str(), m_shard_key_name.length()) != 0) {
								return rewrite_asis(_sql, ERR_SELECT_WHERE);
							}
							next += m_shard_key_name.length();
							if (isalpha(*next) || isdigit(*next)) {
								return rewrite_asis(_sql, ERR_SELECT_WHERE);
							}
							if (*next == '_') {
								next++;
								if (*next == '0') {
									next ++;
								} else {
									return rewrite_asis(_sql, ERR_SELECT_WHERE);
								}
							}
							while (isspace(*next))
								next++;
							if (*next != ')') {
								return rewrite_asis(_sql, ERR_SELECT_WHERE);
							}
							next++;
						}

						val.sql.append(start, next - start);

						val.sql.append(sAND.c_str(), sAND.length());

						if (alias) {
							val.sql.append(alias, alias_len + 1); // copy the alias name plus '.'
						}
						val.sql.append(m_sSCUTTLE_ID_WHERE_EQ.c_str(), m_sSCUTTLE_ID_WHERE_EQ.length()); // "scuttle_id = :scuttle_id "
						break;
					}
					case 3: // JOIN
					{
						int alias_len = 0;
						const char *alias = 0;
						get_alias(start, next - m_shard_key_name.length(), alias, alias_len);
						if (alias == 0) {
							return rewrite_asis(_sql, ERR_JOIN_NO_ALIAS);
						}
						std::string alias1(alias, alias_len);
						// now get alias for the right
						const char* p = next;
						while (isspace(*p))
							p++;
						if (*p != '=')
							return rewrite_asis(_sql, ERR_JOIN_NO_EQ);
						p++;
						while (isspace(*p))
							p++;
						next = p;
						while ((*next) && (*next != '.'))
							next++;
						if (*next != '.')
							return rewrite_asis(_sql, ERR_JOIN_NO_ALIAS);
						std::string alias2(p, next - p);
						next++;
						// expect shard key name too
						if (strncasecmp(next, m_shard_key_name.c_str(), m_shard_key_name.length()) != 0) {
							return rewrite_asis(_sql, ERR_NO_SHARD_KEY);
						}
						next += m_shard_key_name.length();
						if (is_alpha_underscore(*next)) {
							return rewrite_asis(_sql, ERR_NO_SHARD_KEY);
						}

						val.sql.append(start, next - start);
						val.sql.append(sAND.c_str(), sAND.length());
						val.sql.append(alias1);
						val.sql.append(m_sDOT_SCUTTLE_ID_EQ.c_str(), m_sDOT_SCUTTLE_ID_EQ.length()); // ".scuttle_id = "
						val.sql.append(alias2);
						val.sql.append(m_sDOT_SCUTTLE_ID.c_str(), m_sDOT_SCUTTLE_ID.length()); //".scuttle_id"
						break;
					}
					case 0: // select
						if (inner_select) {
							int alias_len = 0;
							const char *alias = 0;
							get_alias(start, next - m_shard_key_name.length(), alias, alias_len);
							val.sql.append(start, next - start);
							val.sql.append(sCOMMA.c_str(), sCOMMA.length()); // ", "
							if (alias) {
								val.sql.append(alias, alias_len + 1); // alias + "."
							}
							val.sql.append(m_sSCUTTLE_ID.c_str(), m_sSCUTTLE_ID.length()); // "scuttle_id"
						} else {
							val.sql.append(start, next - start);
						}
						break;
					default:
						val.sql.append(start, next - start);
						break;
				} // sk switch
				break;
		} // switch
		start = next;
		last_token = m_select_finder.found();
	}

	// append the rest
	while (*start) {
		val.sql.append(start, 1);
		start++;
	}
	if (val.rewritten) {
		val.err = 0;
	} else {
		val.err = ERR_NO_SHARD_KEY;
	}
	m_sqls[_sql] = val;
}

void SQLRewriter::rewrite_insert(const std::string& _sql)
{
	RewriteInfo val;
	val.rewritten = false;
	const char* query = _sql.c_str();
	const char* sk = m_shard_key_name.c_str();

	// column names
	const char* start = query;
	while (true) {
		const char* next = find_exact_i(start, sk);
		if (next == 0)
			return rewrite_asis(_sql, ERR_NO_SHARD_KEY);
		int alias_len = 0;
		const char *alias = 0;
		get_alias(start, next, alias, alias_len);
		next += m_shard_key_name.length();
		while (isspace(*next))
			next++;
		val.sql.append(start, next - start);
		if (!is_alpha_underscore(*next)) {
			val.sql.append(sCOMMA.c_str(), sCOMMA.length()); // ", "
			if (alias) {
				val.sql.append(alias, alias_len + 1);
			}
			val.sql.append(m_sSCUTTLE_ID.c_str(), m_sSCUTTLE_ID.length()); // "scuttle_id"
			start = next;
			break;
		}
		start = next;
	}

	// values
	sk = m_colon_shard_key_name.c_str();
	const char* next = start;
	while (true) {
		next = find_exact_i(next, sk);
		if (next == 0)
			return rewrite_asis(_sql, ERR_NO_SHARD_KEY);
		next += m_colon_shard_key_name.length();
		while (isspace(*next))
			next++;
		if (!is_alpha_underscore(*next)) {
			val.sql.append(start, next - start);
			val.sql.append(m_sCOMMA_SCUTTLE_ID.c_str(), m_sCOMMA_SCUTTLE_ID.length()); // ", :scuttle_id"
			break;
		}
	}

	// append the rest
	while (*next) {
		val.sql.append(next, 1);
		next++;
	}

	val.rewritten = true;
	val.err = 0;
	m_sqls[_sql] = val;
}

void SQLRewriter::rewrite_update(const std::string& _sql)
{
	RewriteInfo val;
	const char* query = _sql.c_str();
	const char* sk = m_colon_shard_key_name.c_str();

	// where clause
	const char* start = query;
	sk = m_colon_shard_key_name.c_str();
	while (true) {
		const char* next = find_exact_i(start, "where");
		if (next == 0)
			return rewrite_asis(_sql, ERR_NO_SHARD_KEY);
		next = find_exact_i(next, sk);
		if (next == 0)
			return rewrite_asis(_sql, ERR_NO_SHARD_KEY);
		// look for alias
		const char* p = next - 1;
		// skip whitespace
		while ((p > start) && isspace(*p))
			p--;
		// expect '='
		if (*p != '=') {
			return rewrite_asis(_sql, ERR_UPDATE_NO_EQ);
		}
		p--;
		// skip whitespace
		while ((p > start) && isspace(*p))
			p--;

		int alias_len = 0;
		const char *alias = 0;
		get_alias(start, p - m_shard_key_name.length() + 1, alias, alias_len);

		next += m_colon_shard_key_name.length();

		if (!is_alpha_underscore(*next)) {
			val.sql.assign(query, next - query);
			val.sql.append(sAND.c_str(), sAND.length());
			if (alias) {
				val.sql.append(alias, alias_len + 1);
			}
			val.sql.append(m_sSCUTTLE_ID_WHERE_EQ.c_str(), m_sSCUTTLE_ID_WHERE_EQ.length()); // "scuttle_id = :scuttle_id "
			start = next;
			break;
		}
		start = next;
	}

	// append the rest
	while (*start) {
		val.sql.append(start, 1);
		start++;
	}

	val.rewritten = true;
	val.err = 0;
	m_sqls[_sql] = val;
}

void SQLRewriter::rewrite_delete(const std::string& _sql)
{
	rewrite_update(_sql);
}

void SQLRewriter::rewrite_asis(const std::string& _sql, int _err)
{
	RewriteInfo val;
	val.sql = _sql;
	val.rewritten = false;
	val.err = _err;
	m_sqls[_sql] = val;
}

void SQLRewriter::logCAL(const std::string& _hash, int _err) {
	std::string name = _hash;
	name += '_';


	switch(_err) {
		case ERR_SELECT_WHERE:
			name += "where";
			break;
		case ERR_JOIN_NO_ALIAS:
			name += "join_no_alias";
			break;
		case ERR_JOIN_NO_EQ:
			name += "join_no_eq";
			break;
		case ERR_UPDATE_NO_EQ:
			name += "update_no_eq";
			break;
		case ERR_UNKNOWN_SQL:
			name += "unk_sql";
			break;
		case ERR_HAS_SCUTTLE_ID:
			name += "has_scuttle_id";
			break;
		case ERR_PLSQL:
			name += "plsql";
			break;
		case ERR_NO_SHARD_KEY:
			name += "no_skey";
			break;
		case ERR_INTERNAL_ERR:
			name += "internal_err";
			break;
		default:
			name += "internal_err_";
			std::ostringstream os;
			os << _err;
			name += os.str();
			break;
	}

	CalEvent rwev("SQL_RW");
	rwev.SetName(name);
	rwev.SetStatus(CAL::TRANS_OK);
	rwev.Completed();
}

void SQLRewriter::rewrite(const std::string& _sql, const std::string*& _rewritten_sql, bool& _rewritten, int& _err)
{
	std::unordered_map<std::string, RewriteInfo>::iterator it = m_sqls.find(_sql);

	if (it == m_sqls.end()) {
		if (has_scuttle_id(_sql)) {
			rewrite_asis(_sql, ERR_HAS_SCUTTLE_ID);
		} else {
			SqlType type = get_type(_sql);
			switch (type) {
				case SELECT:
					rewrite_select(_sql);
					break;
				case INSERT:
					rewrite_insert(_sql);
					break;
				case UPDATE:
					rewrite_update(_sql);
					break;
				case DELETE:
					rewrite_delete(_sql);
					break;
				case PL_SQL_DECLARE:
				case PL_SQL_BEGIN:
					rewrite_asis(_sql, ERR_PLSQL);
					break;
				case UNKNOWN:
					rewrite_asis(_sql, ERR_UNKNOWN_SQL);
					break;
				default:
					rewrite_asis(_sql, ERR_INTERNAL_ERR);
					break;
			}
		}
		it = m_sqls.find(_sql);
		if (!(it->second.rewritten)) {
			StringUtil::fmt_ulong(it->second.cal_hash, Util::sql_CAL_hash(_sql.c_str()));
		}
	}
	_rewritten_sql = &(it->second.sql);
	_rewritten = it->second.rewritten;
	_err = it->second.err;
	if (!_rewritten) {
		logCAL(it->second.cal_hash, _err);
	}
}

void SQLRewriter::get_alias(const char* start, const char* key, const char* & alias, int& alias_len) {
	key--;
	if ((key > start) && (*key == '.')) {
		alias = key - 1;
		while ((alias > start) && (is_alpha_underscore(*alias)))
			alias--;
		alias++;
		alias_len = (key - alias);
	}
}

const char* SQLRewriter::find_exact_i(const char* hastack, const char* needle) {
	const char* prev = hastack;
	int len = strlen(needle);
	while (true) {
		const char* next = strcasestr(prev, needle);
		if (next == 0)
			return 0;
		if ((next == prev) || is_alpha_underscore(*(next - 1)) || is_alpha_underscore(*(next + len))) {
			prev = next + len;
			continue;
		}
		return next;
	}
	return 0;
}

