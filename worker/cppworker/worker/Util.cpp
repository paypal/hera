#include <cstddef>
#include <sstream>
#include "utility/fnv/fnv.h"
#include "utility/encoding/NetstringWriter.h"
#include "worker/Util.h"
#include "utility/StringUtil.h"
#include "worker/ColumnInfo.h"
#include "worker/OCCCommands.h"

ulong Util::sql_hash(const char *sql)
{
	// from CalActivity::SendSQLData (m_query_str);
	// NOTE: must use "chars" here and not "uchars" because the uchars function
	// will return a string with 2-bytes per character, the high byte of which
	// will always be zero -- and then fnv_64a_str would think the string was
	// only a single byte long!
	unsigned long long hash64 = fnv_64a_str(sql, FNV1_64A_INIT);
	ulong hi = (ulong) (hash64 >> 32);
	ulong lo = (ulong) hash64;
	ulong hash32 = lo ^ hi;
	return hash32;
}

ulong Util::sql_CAL_hash(const char *sql)
{
	std::string query_str(sql);
	StringUtil::normalizeSQL(query_str);

	return sql_hash(query_str.c_str());
}


int Util::out_col_names(NetstringWriter* _writer, std::vector<ColumnInfo>* _cols)
{
	unsigned int col_cnt = (unsigned int)_cols->size();

	// Send the column count
	std::string cnt;

	StringUtil::fmt_ulong(cnt, col_cnt);
	_writer->add(OCC_VALUE, cnt);

	for (unsigned int i = 0; i < col_cnt; ++i)
	{
		_writer->add(OCC_VALUE, (*_cols)[i].name);
	}

	return _writer->write();

}

int Util::out_col_info(NetstringWriter* _writer, std::vector<ColumnInfo>* _cols)
{
	unsigned int col_cnt = (unsigned int)_cols->size();

	// Send the column count
	std::string cnt;

	StringUtil::fmt_ulong(cnt, col_cnt);
	_writer->add(OCC_VALUE, cnt);

	for (unsigned int i = 0; i < col_cnt; ++i)
	{
		_writer->add(OCC_VALUE, (*_cols)[i].name);
		StringUtil::fmt_ulong(cnt, (*_cols)[i].type);
		if (_writer->add(OCC_VALUE, cnt) < 0)
			return -1;
		StringUtil::fmt_ulong(cnt, (*_cols)[i].width);
		if (_writer->add(OCC_VALUE, cnt) < 0)
			return -1;
		StringUtil::fmt_ulong(cnt, (*_cols)[i].precision);
		if (_writer->add(OCC_VALUE, cnt) < 0)
			return -1;
		StringUtil::fmt_ulong(cnt, (*_cols)[i].scale);
		if (_writer->add(OCC_VALUE, cnt) < 0)
			return -1;
	}

	return _writer->write();
}

std::string Util::get_command_name(int _cmd)
{
	switch (_cmd)
	{
	case OCC_PREPARE:
		return std::string("PREPARE");
	case OCC_PREPARE_V2:
		return std::string("PREPARE_V2");
	case OCC_BIND_NAME:
		return std::string("BIND_NAME");
	case OCC_BIND_VALUE:
		return std::string("BIND_VALUE");
	case OCC_EXECUTE:
		return std::string("EXECUTE");
	case OCC_ROWS:
		return std::string("ROWS");
	case OCC_COLS:
		return std::string("COLS");
	case OCC_FETCH:
		return std::string("FETCH");
	case OCC_COMMIT:
		return std::string("COMMIT");
	case OCC_ROLLBACK:
		return std::string("ROLLBACK");
	case OCC_BIND_TYPE:
		return std::string("BIND_TYPE");
	case OCC_CLIENT_INFO:
		return std::string("CLIENT_INFO");
	case OCC_BACKTRACE:
		return std::string("BACKTRACE");
	case OCC_BIND_OUT_NAME:
		return std::string("BIND_OUT_NAME");
	case OCC_PREPARE_SPECIAL:
		return std::string("PREPARE_SPECIAL");
	case OCC_TRANS_START:
		return std::string("TRANS_START");
	case OCC_TRANS_TIMEOUT:
		return std::string("TRANS_TIMEOUT");
	case OCC_TRANS_ROLE:
		return std::string("TRANS_ROLE");
	case OCC_TRANS_PREPARE:
		return std::string("TRANS_PREPARE");
	case OCC_COLS_INFO:
		return std::string("OCC_COLS_INFO");
	case OCC_SHARD_KEY:
		return std::string("OCC_SHARD_KEY");
	default:
		return std::string("");
	}

}

void Util::netstring(int _cmd,  const std::string& _payload, std::string& _buff) {
	std::ostringstream stream;
	NetstringWriter nw(&stream);
	nw.write(_cmd, _payload);
	_buff = stream.str();
}

