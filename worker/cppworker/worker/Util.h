#ifndef _OCC_UTIL_H_
#define _OCC_UTIL_H_

#include <string>
#include <vector>

class NetstringWriter;
class ColumnInfo;

class Util
{
public:
	/**
	 * The SQL hash from CAL.
	 */
	static unsigned long sql_hash(const char *sql);
	/**
	 * The SQL hash from CAL, with _sql "transliterated" (new lines replaced with space, etc).
	 */
	static unsigned long sql_CAL_hash(const char *sql);

	static int out_col_names(NetstringWriter* _writer, std::vector<ColumnInfo>* _cols);
	static int out_col_info(NetstringWriter* _writer, std::vector<ColumnInfo>* _cols);

	static std::string get_command_name(int _cmd);

	static void netstring(int _cmd,  const std::string& _payload, std::string& _buff);
};

#endif
