package com.paypal.jmux.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.sql.Types;

import com.paypal.jmux.ex.OccInternalErrorException;
import com.paypal.jmux.util.OccColumnMeta;

public class OccResultSetMetaData implements ResultSetMetaData {
	private static Map<Integer, Integer> typeMap = null;
	private ArrayList<OccColumnMeta> meta;
	
	public static final int PAYLOAD_SIZE_VIRTUAL_COLUMN = -1;
	private int payloadSizeInBytes = 0;

	public OccResultSetMetaData(ArrayList<OccColumnMeta> _meta) {
		meta = _meta;
	}
	
	/***********************************************
	 *** RESULTSETMETADATA CALLS                 ***
	 ***********************************************/
	
	public int getColumnCount() throws SQLException {
		return meta.size();
	}
	
	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
	}
	
	public boolean isCaseSensitive(int column) throws SQLException {
		return false;	
	}
	
	public boolean isSearchable(int column) throws SQLException {
		return true;
	}
	
	public boolean isCurrency(int column) throws SQLException {
		return false;
	}
	
	public int isNullable(int column) throws SQLException {
		return columnNullable;
	}
	
	public boolean isSigned(int column) throws SQLException {
		return true;
	}
	
	public String getColumnLabel(int column) throws SQLException {
		return getColumnName(column);
	}
	
	public String getColumnName(int column) throws SQLException {
		return meta.get(column - 1).getName();
	}
	
	public String getSchemaName(int column) throws SQLException {
		return "";
	}
	
	public String getTableName(int column) throws SQLException {
		return "";
	}
	
	public String getCatalogName(int column) throws SQLException {
		return "";
	}
	
	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}
	
	public boolean isWritable(int column) throws SQLException {
		return false;
	}
	
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return false;
	}
	
	public int getColumnType(int column) throws SQLException {
		Integer type = typeMap.get(meta.get(column - 1).getType());
		if (type == null)
			throw new OccInternalErrorException("Oracle OCI type " + meta.get(column - 1).getType() + " not mapped to a SQL type");
		return type;
	}
	
	public String getColumnTypeName(int column) throws SQLException {
		throw new SQLException("getColumnTypeName() is not supported");
	}
	
	public String getColumnClassName(int column) throws SQLException {
		throw new SQLException("getColumnClassName() is not supported");
	}
	
	public int getColumnDisplaySize(int column) throws SQLException {
		if (column == PAYLOAD_SIZE_VIRTUAL_COLUMN){
			return payloadSizeInBytes;
		}
		return meta.get(column - 1).getWidth();
	}
	
	public int getPrecision(int column) throws SQLException {
		return meta.get(column - 1).getPrecision();
	}
	
	public int getScale(int column) throws SQLException {
		return meta.get(column - 1).getScale();
	}
	
	protected void setPayloadSizeInBytes(int sizeInBytes){
		payloadSizeInBytes = sizeInBytes;
	}

	// JDBC 4.0
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("isWrapperFor() is not supported");
	}
	
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("unwrap() is not supported");
	}
	
	static {
		typeMap = new HashMap<Integer, Integer>();
		// ocidfn.h
		typeMap.put(1 /*SQLT_CHR*/, Types.VARCHAR);
		typeMap.put(2 /*SQLT_NUM*/, Types.NUMERIC);
		typeMap.put(3 /*SQLT_INT*/, Types.INTEGER);
		typeMap.put(4 /*SQLT_FLT*/, Types.FLOAT);
		typeMap.put(5 /*SQLT_STR*/, Types.VARCHAR);
		typeMap.put(6 /*SQLT_VNU NUM with preceding length byte*/, Types.VARCHAR);
		typeMap.put(7 /*SQLT_PDN*/, Types.VARCHAR);
		typeMap.put(8 /*SQLT_LNG*/, Types.BIGINT);
		typeMap.put(9 /*SQLT_VCS*/, Types.VARCHAR);
		typeMap.put(12 /*SQLT_DAT*/, Types.DATE);
		typeMap.put(15 /*SQLT_VBI*/, Types.VARBINARY);
		typeMap.put(21 /*SQLT_BFLOAT*/, Types.FLOAT);
		typeMap.put(22 /*SQLT_BDOUBLE*/, Types.DOUBLE);
		typeMap.put(23 /*SQLT_BIN*/, Types.VARBINARY);
		typeMap.put(24 /*SQLT_LBI*/, Types.LONGVARBINARY);
		typeMap.put(68 /*SQLT_UIN*/, Types.NUMERIC);
		typeMap.put(94 /*SQLT_LVC*/, Types.LONGVARCHAR);
		typeMap.put(95 /*SQLT_LVB*/, Types.LONGVARBINARY);
		typeMap.put(96 /*SQLT_AFC*/, Types.VARCHAR);
		typeMap.put(97 /*SQLT_AVC*/, Types.VARCHAR);
		typeMap.put(104 /*SQLT_RDD - rowid type*/, Types.VARCHAR);
		typeMap.put(112 /*SQLT_CLOB*/, Types.CLOB);
		typeMap.put(113 /*SQLT_BLOB*/, Types.BLOB);
		typeMap.put(155 /*SQLT_VST*/, Types.VARCHAR);
		typeMap.put(156 /*SQLT_ODT*/, Types.DATE);
		typeMap.put(184 /*SQLT_DATE*/, Types.DATE);
		typeMap.put(185 /*SQLT_TIME*/, Types.TIME);
		typeMap.put(186 /*SQLT_TIME_TZ*/, Types.TIME);
		typeMap.put(187 /*SQLT_TIMESTAMP*/, Types.TIMESTAMP);
		typeMap.put(188 /*SQLT_TIMESTAMP_TZ*/, Types.TIMESTAMP);
		typeMap.put(232 /*SQLT_TIMESTAMP_LTZ*/, Types.TIMESTAMP);
	}

}
