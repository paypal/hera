#ifndef _SQLREWRITER_H_
#define _SQLREWRITER_H_

#include <vector>
#include <string>
#include <unordered_map>

class SQLRewriter {
	struct RewriteInfo {
		std::string sql;
		bool rewritten;
		int err;
		std::string cal_hash; // cache CAL hash so that it is not computed every time
	};
	typedef enum SqlType{
		SELECT = 0,
		INSERT = 1,
		UPDATE = 2,
		DELETE = 3,
		PL_SQL_DECLARE = 4,
		PL_SQL_BEGIN = 5,
		UNKNOWN = 50
	} SqlType;

	static const std::string sAND;
	static const std::string sCOMMA;
	
	class Finder {
		const char** m_needles;
		const char** m_ptrs;
		int m_count;
		const char* m_next;
		int m_found;
		void clear();
		bool eq_case_i(char c1, char c2);
	public:
		Finder();
		~Finder();
		void init(const char** needles, int count);

		bool find(const char* haystack);
		const char* next();
		int found();
	};
private:
	std::unordered_map<std::string, RewriteInfo> m_sqls;
	std::string m_shard_key_name;
	std::string m_colon_shard_key_name;
	std::string m_sSCUTTLE_NAME;
	std::string m_sSCUTTLE_ID_WHERE_EQ;
	std::string m_sDOT_SCUTTLE_ID_EQ;
	std::string m_sDOT_SCUTTLE_ID;
	std::string m_sSCUTTLE_ID;
	std::string m_sCOMMA_SCUTTLE_ID;

	Finder m_query_type_finder, m_select_finder;

private:
	static void logCAL(const std::string& _hash, int _err);
	static bool is_alpha_underscore(char c);
	void get_alias(const char* start, const char* key, const char* & alias, int& alias_len);

	SqlType get_type(const std::string& _sql);
	bool has_scuttle_id(const std::string& _sql);
	void rewrite_select(const std::string& _sql);
	void rewrite_insert(const std::string& _sql);
	void rewrite_update(const std::string& _sql);
	void rewrite_delete(const std::string& _sql);
	void rewrite_asis(const std::string& _sql, int _err);
	const char* find_exact_i(const char* hastack, const char* needle);

public:
	// error codes for rewrite
	enum {
		OK = 0,
		ERR_SELECT_WHERE = 1,
		ERR_JOIN_NO_ALIAS = 2,
		ERR_JOIN_NO_EQ = 3,
		ERR_INSERT = 4,
		ERR_UPDATE_NO_EQ = 5,
		ERR_UNKNOWN_SQL = 6,
		ERR_HAS_SCUTTLE_ID = 7,
		ERR_PLSQL = 8,
		ERR_NO_SHARD_KEY = 9,
		ERR_INTERNAL_ERR = 10
	};

public:
	SQLRewriter();
	void init(const std::string& _shard_key_name, const std::string& _scuttle_attr_name);
	/*
	 * _sql: the input SQL
	 * _rewritten_sql: [output] the rewritten SQL.
	 * _rewritten: [output] true if the SQL was rewritten so a scuttle_id was inserted. false if either the SQL had scuttle_id or the SQL had no shard key or some error
	 * _err: [output] 0 if OK, non-zero indicate the erro code
	 *
	 */
	void rewrite(const std::string& _sql, const std::string*& _rewritten_sql, bool& _rewritten, int& _err);
};

#endif
