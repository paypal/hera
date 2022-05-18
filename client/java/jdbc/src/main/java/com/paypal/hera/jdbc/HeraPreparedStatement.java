package com.paypal.hera.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.constants.BindType;
import com.paypal.hera.constants.Consts;
import com.paypal.hera.ex.HeraClientException;
import com.paypal.hera.ex.HeraExceptionBase;
import com.paypal.hera.ex.HeraIOException;
import com.paypal.hera.ex.HeraInternalErrorException;
import com.paypal.hera.ex.HeraRuntimeException;
import com.paypal.hera.ex.HeraSQLException;
import com.paypal.hera.util.HeraJdbcConverter;
import com.paypal.hera.util.HeraJdbcUtil;
import com.paypal.hera.util.Pair;
import com.paypal.hera.util.HeraStatementsCache.ShardingInfo;

public class HeraPreparedStatement extends HeraStatement implements PreparedStatement {
	
	final static Logger LOGGER = LoggerFactory.getLogger(HeraPreparedStatement.class);
	
	
	enum OBJECT_TYPE {
		String,
		Integer
	};

	private static final byte[] NULL_VAL = new byte[0];
	private String sql;
	private Map<Integer, Pair<BindType, byte[]> > in_params;
	protected HeraJdbcConverter converter; 
	private ArrayList<Pair<BindType, ArrayList<byte[]> > > batched_in_params;
	private ArrayList<Integer> max_batched_value_size;
	
	public HeraPreparedStatement(HeraConnection heraConnection, String _sql) {
		super(heraConnection);
		sql = _sql;
		in_params = new HashMap<Integer, Pair<BindType, byte[]> >();
		converter = heraConnection.getConverter();
		batched_in_params = new ArrayList<Pair<BindType, ArrayList<byte[]> > >();
		max_batched_value_size = new ArrayList<Integer>();
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraPreparedStatement created, id: " + this.hashCode() + ", conn id: " + heraConnection.hashCode() + ", SQL: " + _sql);
	}

	protected void prepare() throws HeraExceptionBase {
		super.prepare(sql);
	}

	protected void unsupportedParamType(String name) throws SQLException
	{
		checkOpened();
		throw new SQLFeatureNotSupportedException("Parameter type is not supported: " + name + " for prepared statement: " + sql);
	}

	protected void bindIn(Map<Integer, Pair<BindType, byte[]> > params) throws HeraExceptionBase {
		if (params.size() > stCache.getParamCount())
			throw new HeraSQLException("Illegal number of bind parameters, trying to bind " + params.size() + " instead of " + stCache.getParamCount());

		for (Entry<Integer, Pair<BindType, byte[]>> entry : params.entrySet()){
			connection.getHeraClient().bind(stCache.actualParamName(entry.getKey()), 
						entry.getValue().getFirst(), entry.getValue().getSecond());
		}
	}

	// override the base class method to throw exc, per JDBC 4.0 spec
	public ResultSet executeQuery(String sql) throws SQLException {
		notSupported();
		return null;
	}

	// override the base class method to throw exc, per JDBC 4.0 spec
	public int executeUpdate(String sql) throws SQLException {
		notSupported();
		return 0;
	}

	protected void bindAndShardInfo() throws SQLException {
		byte[] shardKeyPayload = null;
		if (connection.shardingEnabled()) {
			if (connection.getShardID() == -1) {
				shardKeyPayload = connection.getShardKeyPayload();
				if (shardKeyPayload == null) {
					ShardingInfo shardingInfo = stCache.getShardingInfo();
					if (shardingInfo != null) {
						shardKeyPayload = shardProcessing(shardingInfo.sk, shardingInfo.skPos, shardingInfo.scuttle_idPos);
					}
				} else {
					connection.setShardKeyPayload(null);
				}
			} else {
				if (null != stCache.getShardingInfo()) {
					connection.getHeraClient().reset();
					throw new HeraClientException("Before query: '" + sql + "' the shard hint was not reset");
				}
				if (null != connection.getShardKeyPayload()) {
					connection.getHeraClient().reset();
					throw new HeraClientException("For query: '" + sql + "' the shard hint was not reset before setting new hint");
				}
			}
		}
		bindIn(in_params);
		if (shardKeyPayload != null) {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("HeraPreparedStatement (id: " + this.hashCode() + ") shard key set");
			connection.getHeraClient().shardKey(shardKeyPayload);
		}
	}
	
	public ResultSet executeQuery() throws SQLException {
		checkOpened();
		helperInitExecute();
		prepare();
		bindAndShardInfo();
		return helperExecuteQuery();
	}

	public int executeUpdate() throws SQLException {
		checkOpened();
		helperInitExecute();
		prepare();
		bindAndShardInfo();
		return helperExecuteUpdate(connection.getAutoCommit());
	}
	
	public void addBatch() throws SQLException {
		checkOpened();
		if (batched_in_params.isEmpty()) {
			for (int i = 1; i <= in_params.size(); i++) {
				Pair<BindType, byte[]> it = in_params.get(i);
				batched_in_params.add(new Pair<BindType, ArrayList<byte[]> >(it.getFirst(), new ArrayList<byte[]>()));
				max_batched_value_size.add(0);
			}
		}
		if (in_params.size() != batched_in_params.size()) {
			throw new HeraSQLException("Illegal number of varbinds: " + batched_in_params.size() + " and " + in_params.size() + ". Each row needs to have the same number of varbinds.");
		}
		for (int i = 0; i < in_params.size(); i++) {
			byte[] data = in_params.get(i + 1).getSecond();
			batched_in_params.get(i).getSecond().add(data);
			if (max_batched_value_size.get(i) < data.length)
				max_batched_value_size.set(i, data.length);
		}
		in_params.clear();
	}

	public int[] executeBatch() throws SQLException {
		if (!connection.batchEnabled()) {
			throw new SQLFeatureNotSupportedException("Batch not supported");
		}
		checkOpened();
		helperInitExecute();
		prepare();
		int batch_size = batched_in_params.get(0).getSecond().size();
		for (int i = 0; i < batched_in_params.size(); i++) {
			connection.getHeraClient().bindArray(stCache.actualParamName(i + 1), max_batched_value_size.get(i), 
					batched_in_params.get(i).getFirst(), batched_in_params.get(i).getSecond());
		}
		try {
			clearBatch();
			int rows = helperExecuteUpdate(connection.getAutoCommit());
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("HeraPreparedStatement::executeBacth(): " + rows + " rows changed");
		} catch (SQLException ex) {
			if (batch_size > 1) {
				String errorMessage = ex.getMessage().substring(Consts.HERA_SQL_ERROR_PREFIX.length());
				int updateCounts[] = HeraJdbcUtil.getArrayCounts(errorMessage, batch_size);
				throw new BatchUpdateException(updateCounts, ex);
			} else {
				int[] updateCounts = {Statement.EXECUTE_FAILED};
				throw new BatchUpdateException(updateCounts, ex);
			}
		}
		int[] ret = new int[batch_size];
		for (int i = 0; i < batch_size; i++) {
			ret[i] = Statement.SUCCESS_NO_INFO;
		}
		return ret;
	}
	
	public void clearBatch() throws SQLException {
		max_batched_value_size.clear();
		batched_in_params.clear();
	}

	public boolean execute() throws SQLException {
		checkOpened();
		helperInitExecute();
		prepare();
		bindAndShardInfo();
		return helperExecute(connection.getAutoCommit());
		
	}

	public void clearParameters() throws SQLException {
		checkOpened();
		in_params.clear();
	}

	// bellow comment from DBIT - leave it around for a while
	/*
	* This will always be null, because we don't need this 
	* on the client, as our uses for it are only needed on the 
	* DBIT server for it to determine column types and get clobs,
	* long raw's, etc.  DAL FromDBRule needs to check for null
	* and if null then it uses getString(). In the future if the DAL 
	* supports clob handles for the appcode to iterate through,
	* then it will be specifically asking for a clob handle, so then 
	* we can handle it that way on the client, but current expectation 
	* is to get everything in the clob and pop it into a string.
	 */
	public ResultSetMetaData getMetaData() throws SQLException {
		checkOpened();
		throw new HeraSQLException("MetaData is available from ResultSet only");
	}

	protected final void checkParamStart() throws SQLException {
		checkOpened();
	}
	
	protected final void bindString(int _index, byte[] _value) throws SQLException {
		in_params.put(Integer.valueOf(_index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_STRING, _value));
	}

	protected final void bindBoolean(int _index, byte[] _value) throws SQLException {
		in_params.put(Integer.valueOf(_index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_BOOLEAN, _value));
	}

	protected final void bindInt(int _index, byte[] _value) throws SQLException {
		in_params.put(Integer.valueOf(_index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_INT, _value));
	}

	protected final void bindDateTime(int _index, byte[] _value) throws SQLException {
		in_params.put(Integer.valueOf(_index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_TIMESTAMP, _value));
	}

	protected final void bindDateTimeTZ(int _index, byte[] _value) throws SQLException {
		in_params.put(Integer.valueOf(_index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_TIMESTAMP_TZ, _value));
	}
	
	/***********************************************
	 *** SETTING PARAMETERS                      ***
	 ***********************************************/

	public void setNull(int _index, int sqlType) throws SQLException {
		checkParamStart();
		switch(sqlType) {
		case Types.VARCHAR:
			bindString(_index, NULL_VAL);
			break;
		case Types.BLOB:
			in_params.put(Integer.valueOf(_index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_BLOB_SINGLE_ROUND, NULL_VAL));
			break;
		case Types.CLOB:
			in_params.put(Integer.valueOf(_index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_CLOB_SINGLE_ROUND, NULL_VAL));
			break;
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			in_params.put(Integer.valueOf(_index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_RAW, NULL_VAL));
			break; 
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			if (connection.enableDateNullFix())
				bindDateTime(_index, NULL_VAL);
			else
				bindString(_index, NULL_VAL);
			break;
		default:
			bindString(_index, NULL_VAL);
			break;
//			unsupportedParamType("Type.sql." + sqlType);
		}
	}

	public void setNull(int index, int sqlType, String typeName)
		throws SQLException
	{
		setNull(index, sqlType);
	}

	public void setInt(int index, int x) throws SQLException {
		checkParamStart();
		bindInt(index, HeraJdbcConverter.int2hera(x));
	}

	public void setBoolean(int index, boolean x) throws SQLException {
		checkParamStart();
		bindBoolean(index, HeraJdbcConverter.int2hera(x ? 1 : 0));
	}

	public void setByte(int index, byte x) throws SQLException {
		setShort(index, x);
	}

	public void setShort(int index, short x) throws SQLException {
		checkParamStart();
		bindString(index, HeraJdbcConverter.short2hera(x));
	}

	public void setLong(int index, long x) throws SQLException {
		checkParamStart();
		bindString(index, HeraJdbcConverter.long2hera(x));
	}

	public void setFloat(int index, float x) throws SQLException {
		checkParamStart();
		bindString(index, HeraJdbcConverter.float2hera(x));
	}

	public void setDouble(int index, double x) throws SQLException {
		checkParamStart();
		bindString(index, HeraJdbcConverter.double2hera(x));
	}

	public void setString(int index, String x) throws SQLException {
		if (x == null) {
			setNull(index, Types.VARCHAR);
			return;
		}

		checkParamStart();
		bindString(index, x.getBytes());
	}

	public void setDate(int index, Date x) throws SQLException {
		if (x == null) {
			setNull(index, Types.DATE);
			return;
		}

		checkParamStart();
		try {
			bindDateTime(index, converter.date2hera(x));
		} catch (UnsupportedEncodingException e) {
			throw new HeraSQLException("Failed to encode Date", e);
		}
	}

	public void setTime(int index, Time x) throws SQLException {
		if (x == null) {
			setNull(index, Types.TIME);
			return;
		}

		checkParamStart();
		try {
			bindDateTime(index, converter.time2hera(x));
		} catch (UnsupportedEncodingException e) {
			throw new HeraSQLException("Failed to encode Time", e);
		}
	}

	public void setTimestamp(int index, Timestamp x) throws SQLException
	{
		if (x == null) {
			setNull(index, Types.TIMESTAMP);
			return;
		}

		checkParamStart();
		try {
			bindDateTime(index, converter.timestamp2hera(x));
		} catch (UnsupportedEncodingException e) {
			throw new HeraSQLException("Failed to encode Timestamp", e);
		}
	}

	public void setDate(int index, Date x, Calendar cal)
		throws SQLException
	{
		if (x == null) {
			setNull(index, Types.DATE);
			return;
		}

		checkParamStart();
		try {
			bindDateTimeTZ(index, converter.date2hera(x, cal));
		} catch (UnsupportedEncodingException e) {
			throw new HeraSQLException("Failed to encode Timestamp", e);
		}
	}

	public void setTime(int index, Time x, Calendar cal)
		throws SQLException
	{
		if (x == null) {
			setNull(index, Types.TIME);
			return;
		}

		checkParamStart();
		try {
			bindDateTimeTZ(index, converter.time2hera(x, cal));
		} catch (UnsupportedEncodingException e) {
			throw new HeraSQLException("Failed to encode Timestamp", e);
		}
	}

	public void setTimestamp(int index, Timestamp x, Calendar cal) throws SQLException
	{
		if (x == null) {
			setNull(index, Types.TIMESTAMP);
			return;
		}

		checkParamStart();
		try {
			bindDateTimeTZ(index, converter.timestamp2hera(x, cal));
		} catch (UnsupportedEncodingException e) {
			throw new HeraSQLException("Failed to encode Timestamp", e);
		}
	}

	public void setCharacterStream(int index,
		Reader reader, int length) throws SQLException
	{
		if (reader == null) {
			setNull(index, Types.CLOB);
			return;
		}

		checkParamStart();
		char[] chars = new char[length];
		int totalCharsRead = 0;
		while (totalCharsRead < length) {
			int charsRead = -1;
			try {
				charsRead = reader.read(chars, totalCharsRead, length - totalCharsRead);
				totalCharsRead += charsRead;
			} catch (IOException e) {
				if (LOGGER.isDebugEnabled())
					LOGGER.error(e.getMessage());
				charsRead = -1;
			}
			if (charsRead == -1)
				throw new HeraRuntimeException("Cannot read input stream to bind");
		}
		try {
			in_params.put(Integer.valueOf(index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_CLOB_SINGLE_ROUND, HeraJdbcConverter.string2hera(new String(chars))));
		} catch (UnsupportedEncodingException e) {
			throw new HeraSQLException("Can't encode char stream");
		}
	}

	public void setBinaryStream(int index, InputStream value, int length)
		throws SQLException
	{
		if (value == null) {
			setNull(index, Types.BLOB);
			return;
		}

		checkParamStart();
		byte[] bytes = new byte[length];
		int totalBytesRead = 0;
		while (totalBytesRead < length) {
			int bytesRead = -1;
			try {
				bytesRead = value.read(bytes, totalBytesRead, length - totalBytesRead);
				totalBytesRead += bytesRead;
			} catch (IOException e) {
				if (LOGGER.isDebugEnabled())
					LOGGER.error(e.getMessage());
				bytesRead = -1;
			}
			if (bytesRead == -1)
				throw new HeraRuntimeException("Cannot read input stream to bind");
		}
		in_params.put(Integer.valueOf(index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_BLOB_SINGLE_ROUND, bytes));
	}

	public void setBytes(int index, byte[] value) throws SQLException {
		if (value == null) {
			setNull(index, Types.VARBINARY);
			return;
		}
		checkParamStart();
		in_params.put(Integer.valueOf(index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_RAW, value));
	}

	private final char getSuccessor(char c, int n) {
		return ((c == 'y') && (n == 2)) ? 'X' : (((c == 'y') && (n < 4)) ? 'y' : ((c == 'y') ? 'M' : (((c == 'M') && (n == 2)) ? 'Y'
				: (((c == 'M') && (n < 3)) ? 'M' : ((c == 'M') ? 'd' : (((c == 'd') && (n < 2)) ? 'd' : ((c == 'd') ? 'H' : (((c == 'H') && (n < 2)) ? 'H'
						: ((c == 'H') ? 'm' : (((c == 'm') && (n < 2)) ? 'm' : ((c == 'm') ? 's' : (((c == 's') && (n < 2)) ? 's' : 'W'))))))))))));
	}

	private final String getDateTimePattern(String dt, boolean toTime) throws IOException {
		if (dt == null) {
			return "HH:mm:ss";
		}
		//
		// Special case
		//
		int dtLength = (dt != null) ? dt.length() : 0;

		if ((dtLength >= 8) && (dtLength <= 10)) {
			int dashCount = 0;
			boolean isDateOnly = true;

			for (int i = 0; i < dtLength; i++) {
				char c = dt.charAt(i);

				if (!Character.isDigit(c) && (c != '-')) {
					isDateOnly = false;

					break;
				}

				if (c == '-') {
					dashCount++;
				}
			}

			if (isDateOnly && (dashCount == 2)) {
				return "yyyy-MM-dd";
			}
		}

		//
		// Special case - time-only
		//
		boolean colonsOnly = true;

		for (int i = 0; i < dtLength; i++) {
			char c = dt.charAt(i);

			if (!Character.isDigit(c) && (c != ':')) {
				colonsOnly = false;

				break;
			}
		}

		if (colonsOnly) {
			return "HH:mm:ss";
		}

		int n;
		int z;
		int count;
		int maxvecs;
		char c;
		char separator;
		StringReader reader = new StringReader(dt + " ");
		ArrayList<Object[]> vec = new ArrayList<Object[]>();
		ArrayList<Object[]> vecRemovelist = new ArrayList<Object[]>();
		Object[] nv = new Object[3];
		Object[] v;
		nv[0] = Character.valueOf('y');
		nv[1] = new StringBuilder();
		nv[2] = Integer.valueOf(0);
		vec.add(nv);

		if (toTime) {
			nv = new Object[3];
			nv[0] = Character.valueOf('h');
			nv[1] = new StringBuilder();
			nv[2] = Integer.valueOf(0);
			vec.add(nv);
		}

		while ((z = reader.read()) != -1) {
			separator = (char) z;
			maxvecs = vec.size();

			for (count = 0; count < maxvecs; count++) {
				v = vec.get(count);
				n = ((Integer) v[2]).intValue();
				c = getSuccessor(((Character) v[0]).charValue(), n);

				if (!Character.isLetterOrDigit(separator)) {
					if ((c == ((Character) v[0]).charValue()) && (c != 'S')) {
						vecRemovelist.add(v);
					} else {
						((StringBuilder) v[1]).append(separator);

						if ((c == 'X') || (c == 'Y')) {
							v[2] = Integer.valueOf(4);
						}
					}
				} else {
					if (c == 'X') {
						c = 'y';
						nv = new Object[3];
						nv[1] = (new StringBuilder(((StringBuilder) v[1]).toString())).append('M');
						nv[0] = Character.valueOf('M');
						nv[2] = Integer.valueOf(1);
						vec.add(nv);
					} else if (c == 'Y') {
						c = 'M';
						nv = new Object[3];
						nv[1] = (new StringBuilder(((StringBuilder) v[1]).toString())).append('d');
						nv[0] = Character.valueOf('d');
						nv[2] = Integer.valueOf(1);
						vec.add(nv);
					}

					((StringBuilder) v[1]).append(c);

					if (c == ((Character) v[0]).charValue()) {
						v[2] = Integer.valueOf(n + 1);
					} else {
						v[0] = Character.valueOf(c);
						v[2] = Integer.valueOf(1);
					}
				}
			}

			int size = vecRemovelist.size();

			for (int i = 0; i < size; i++) {
				v = vecRemovelist.get(i);
				vec.remove(v);
			}

			vecRemovelist.clear();
		}

		int size = vec.size();

		for (int i = 0; i < size; i++) {
			v = vec.get(i);
			c = ((Character) v[0]).charValue();
			n = ((Integer) v[2]).intValue();

			boolean bk = getSuccessor(c, n) != c;
			boolean atEnd = (((c == 's') || (c == 'm') || ((c == 'h') && toTime)) && bk);
			boolean finishesAtDate = (bk && (c == 'd') && !toTime);
			boolean containsEnd = (((StringBuilder) v[1]).toString().indexOf('W') != -1);

			if ((!atEnd && !finishesAtDate) || (containsEnd)) {
				vecRemovelist.add(v);
			}
		}

		size = vecRemovelist.size();

		for (int i = 0; i < size; i++) {
			vec.remove(vecRemovelist.get(i));
		}

		vecRemovelist.clear();
		v = vec.get(0); // might throw exception

		StringBuilder format = (StringBuilder) v[1];
		format.setLength(format.length() - 1);

		return format.toString();
	}

	private void setNumericObject(int parameterIndex, Object parameterObj, int targetSqlType, int scale) throws SQLException {
		Number parameterAsNum;

		if (parameterObj instanceof Boolean) {
			parameterAsNum = ((Boolean) parameterObj).booleanValue() ? Integer.valueOf(1) : Integer.valueOf(0);
		} else if (parameterObj instanceof String) {
			switch (targetSqlType) {
			case Types.BIT:
				if ("1".equals(parameterObj) || "0".equals(parameterObj)) {
					parameterAsNum = Integer.valueOf((String) parameterObj);
				} else {
					boolean parameterAsBoolean = "true".equalsIgnoreCase((String) parameterObj);

					parameterAsNum = parameterAsBoolean ? Integer.valueOf(1) : Integer.valueOf(0);
				}

				break;

			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
				parameterAsNum = Integer.valueOf((String) parameterObj);

				break;

			case Types.BIGINT:
				parameterAsNum = Long.valueOf((String) parameterObj);

				break;

			case Types.REAL:
				parameterAsNum = Float.valueOf((String) parameterObj);

				break;

			case Types.FLOAT:
			case Types.DOUBLE:
				parameterAsNum = Double.valueOf((String) parameterObj);

				break;

			case Types.DECIMAL:
			case Types.NUMERIC:
			default:
				parameterAsNum = new java.math.BigDecimal((String) parameterObj);
			}
		} else {
			parameterAsNum = (Number) parameterObj;
		}

		switch (targetSqlType) {
		case Types.BIT:
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			setInt(parameterIndex, parameterAsNum.intValue());

			break;

		case Types.BIGINT:
			setLong(parameterIndex, parameterAsNum.longValue());

			break;

		case Types.REAL:
			setFloat(parameterIndex, parameterAsNum.floatValue());

			break;

		case Types.FLOAT:
		case Types.DOUBLE:
			setDouble(parameterIndex, parameterAsNum.doubleValue());

			break;

		case Types.DECIMAL:
		case Types.NUMERIC:

			if (parameterAsNum instanceof java.math.BigDecimal) {
				BigDecimal scaledBigDecimal = null;

				try {
					scaledBigDecimal = ((java.math.BigDecimal) parameterAsNum).setScale(scale);
				} catch (ArithmeticException ex) {
					try {
						scaledBigDecimal = ((java.math.BigDecimal) parameterAsNum).setScale(scale, BigDecimal.ROUND_HALF_UP);
					} catch (ArithmeticException arEx) {
						throw new HeraClientException("Can't set scale of '" + scale + "' for DECIMAL argument '" + parameterAsNum + "'", "42000");
					}
				}

				setBigDecimal(parameterIndex, scaledBigDecimal);
			} else if (parameterAsNum instanceof java.math.BigInteger) {
				setBigDecimal(parameterIndex, new java.math.BigDecimal((java.math.BigInteger) parameterAsNum, scale));
			} else {
				setBigDecimal(parameterIndex, BigDecimal.valueOf(parameterAsNum.doubleValue()));
			}

			break;
		}
	}

	/**
	 * implemented as in MySQL driver
	 */
	public void setObject(int index, Object x,
			int targetSqlType, int scale) throws SQLException
	{
		if (x == null) {
			setNull(index, java.sql.Types.OTHER);
		} else {
			try {
				/*
				 * From Table-B5 in the JDBC Spec
				 */
				switch (targetSqlType) {
				case Types.BOOLEAN:

					if (x instanceof Boolean) {
						setBoolean(index, ((Boolean) x).booleanValue());

						break;
					} else if (x instanceof String) {
						setBoolean(index, "true".equalsIgnoreCase((String) x) || !"0".equalsIgnoreCase((String) x));

						break;
					} else if (x instanceof Number) {
						int intValue = ((Number) x).intValue();

						setBoolean(index, intValue != 0);

						break;
					} else {
						throw new HeraClientException("No conversion from " + x.getClass().getName() + " to Types.BOOLEAN possible.", "4200");
					}

				case Types.BIT:
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.INTEGER:
				case Types.BIGINT:
				case Types.REAL:
				case Types.FLOAT:
				case Types.DOUBLE:
				case Types.DECIMAL:
				case Types.NUMERIC:

					setNumericObject(index, x, targetSqlType, scale);

					break;

				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
					if (x instanceof BigDecimal) {
						setBigDecimal(index, (BigDecimal) x);
					} else {
						setString(index, x.toString());
					}

					break;

				case Types.CLOB:

					if (x instanceof java.sql.Clob) {
						setClob(index, (java.sql.Clob) x);
					} else {
						setString(index, x.toString());
					}

					break;

				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
				case Types.BLOB:

					if (x instanceof byte[]) {
						setBytes(index, (byte[]) x);
					} else if (x instanceof java.sql.Blob) {
						setBlob(index, (java.sql.Blob) x);
					} else {
						throw new SQLFeatureNotSupportedException("setObject(): can't convert " + x.getClass().getName() + " to Types.BINARY");
					}

					break;

				case Types.DATE:
				case Types.TIMESTAMP:

					java.util.Date parameterAsDate;

					if (x instanceof String) {
						ParsePosition pp = new ParsePosition(0);
						java.text.DateFormat sdf = new java.text.SimpleDateFormat(getDateTimePattern((String) x, false), Locale.US);
						parameterAsDate = sdf.parse((String) x, pp);
					} else {
						parameterAsDate = (java.util.Date) x;
					}

					switch (targetSqlType) {
					case Types.DATE:

						if (parameterAsDate instanceof java.sql.Date) {
							setDate(index, (java.sql.Date) parameterAsDate);
						} else {
							setDate(index, new java.sql.Date(parameterAsDate.getTime()));
						}

						break;

					case Types.TIMESTAMP:

						if (parameterAsDate instanceof java.sql.Timestamp) {
							setTimestamp(index, (java.sql.Timestamp) parameterAsDate);
						} else {
							setTimestamp(index, new java.sql.Timestamp(parameterAsDate.getTime()));
						}

						break;
					}

					break;

				case Types.TIME:

					if (x instanceof String) {
						java.text.DateFormat sdf = new java.text.SimpleDateFormat(getDateTimePattern((String) x, true), Locale.US);
						setTime(index, new java.sql.Time(sdf.parse((String) x).getTime()));
					} else if (x instanceof Timestamp) {
						Timestamp xT = (Timestamp) x;
						setTime(index, new java.sql.Time(xT.getTime()));
					} else {
						setTime(index, (java.sql.Time) x);
					}

					break;

				case Types.OTHER:
				{
					ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
					ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);
					objectOut.writeObject(x);
					objectOut.flush();
					objectOut.close();
					bytesOut.flush();
					bytesOut.close();

					byte[] buf = bytesOut.toByteArray();
					ByteArrayInputStream bytesIn = new ByteArrayInputStream(buf);
					setBinaryStream(index, bytesIn, buf.length);
				}
				break;

				default:
					try {
						OBJECT_TYPE type = OBJECT_TYPE.valueOf(x.getClass().getSimpleName());
						switch (type) {
						case String:
							setString(index, (String) x);
							break;
						case Integer:
							setInt(index, (Integer) x);
							break;
						default:
							throw new SQLFeatureNotSupportedException("Type " + targetSqlType + " is not supported in setObject()" + 
									" for pos: " + index + ", object: " + x.toString() + ",type: " + x.getClass().getName());
						}
					} catch (IllegalArgumentException e) {
						unsupportedParamType("Object at pos " + index + ": type=" + x.getClass().getSimpleName());
					}
				}
			} catch (Exception ex) {
				if (ex instanceof SQLException) {
					throw (SQLException) ex;
				}

				throw new HeraClientException("Exception ", ex);
			}
		}
	}

	public void setObject(int index, Object x, int targetSqlType)
		throws SQLException
	{
        if (!(x instanceof BigDecimal)) {
            setObject(index, x, targetSqlType, 0);
        } else {
            setObject(index, x, targetSqlType, ((BigDecimal) x).scale());
        }

		/* old impl
		*/
	}

	public void setObject(int index, Object x) throws SQLException
	{
		if (x == null) {
		  setNull(index, Types.OTHER);
		} else if (x instanceof SQLXML) {
		  setSQLXML(index, (SQLXML) x);
		} else if (x instanceof String) {
		  setString(index, (String) x);
		} else if (x instanceof BigDecimal) {
		  setBigDecimal(index, (BigDecimal) x);
		} else if (x instanceof Short) {
		  setShort(index, (Short) x);
		} else if (x instanceof Integer) {
		  setInt(index, (Integer) x);
		} else if (x instanceof Long) {
		  setLong(index, (Long) x);
		} else if (x instanceof Float) {
		  setFloat(index, (Float) x);
		} else if (x instanceof Double) {
		  setDouble(index, (Double) x);
		} else if (x instanceof byte[]) {
		  setBytes(index, (byte[]) x);
		} else if (x instanceof java.sql.Date) {
		  setDate(index, (java.sql.Date) x);
		} else if (x instanceof Time) {
		  setTime(index, (Time) x);
		} else if (x instanceof Timestamp) {
		  setTimestamp(index, (Timestamp) x);
		} else if (x instanceof Boolean) {
		  setBoolean(index, (Boolean) x);
		} else if (x instanceof Byte) {
		  setByte(index, (Byte) x);
		} else if (x instanceof Blob) {
		  setBlob(index, (Blob) x);
		} else if (x instanceof Clob) {
		  setClob(index, (Clob) x);
		} else if (x instanceof Array) {
		  setArray(index, (Array) x);
		} else if (x instanceof Character) {
		  setString(index, ((Character) x).toString());
		  //#if mvn.project.property.postgresql.jdbc.spec >= "JDBC4.2"
		} else if (x instanceof Date) {
		  setDate(index, (Date) x);
		} else {
			  // Can't infer a type.
			throw new HeraSQLException("Can''t infer the SQL type in setObject for " + x.getClass().getName());
		}
	}

	public void setBlob(int index, Blob x) throws SQLException {
		if (x == null) {
			setNull(index, Types.BLOB);
			return;
		}
		checkParamStart();
		in_params.put(Integer.valueOf(index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_BLOB_SINGLE_ROUND, x.getBytes(1, (int)x.length())));
	}

	public void setClob(int index, Clob x) throws SQLException {
		if (x == null) {
			setNull(index, Types.CLOB);
			return;
		}
		checkParamStart();
		try {
			in_params.put(Integer.valueOf(index), new Pair<BindType, byte[]>(BindType.HERA_TYPE_CLOB_SINGLE_ROUND, HeraJdbcConverter.string2hera(x.getSubString(1, (int)x.length()))));
		} catch (UnsupportedEncodingException e) {
			throw new HeraSQLException("Can't encode clob");
		}
	}

	public void setRef(int index, Ref x) throws SQLException {
		unsupportedParamType("Ref at pos " + index);
	}

	public void setArray(int index, Array x) throws SQLException {
		unsupportedParamType("Array at pos " + index);
	}

	public void setAsciiStream(int index, InputStream x, int length)
		throws SQLException
	{
		unsupportedParamType("AsciiStream at pos " + index);
	}

	/**
	 * @deprecated
	 */
	public void setUnicodeStream(int index, InputStream x, int length)
		throws SQLException
	{
		unsupportedParamType("UnicodeStream at pos " + index);
	}

	public void setBigDecimal(int index, BigDecimal x)
		throws SQLException
	{
		if (x == null) {
			setNull(index, Types.VARCHAR);
			return;
		}
		checkParamStart();
		bindString(index, HeraJdbcConverter.bigDecimal2hera(x));
	}

	// JDBC3.0 COMPATIBILITY

	public void setURL(int parameterIndex, URL x)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setURL is not implemented");
	}

	public ParameterMetaData getParameterMetaData()
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.getParameterMetaData is not implemented");
	}

	// JDBC 4.0 
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setAsciiStream is not implemented");
	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setAsciiStream is not implemented");
	}

	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setBinaryStream is not implemented");
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, Types.VARBINARY);
			return;
		}
		if (length > Integer.MAX_VALUE)
			throw new HeraSQLException("InputStream too large");
		byte b[] = new byte[(int)length];
        try {
        	int n = x.read(b);
        	if (n != b.length) {
        		LOGGER.error("Invalid read, only " + n + " bytes read instead of " + b.length);
        	}
			setBytes(parameterIndex, b);
		} catch (IOException e) {
			throw new HeraSQLException("IO exception reading Reader", e);
		}
	}

	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setBlob is not implemented");
	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setBlob is not implemented");
	}

	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		if (reader == null) {
			setNull(parameterIndex, Types.VARCHAR);
			return;
		}
        char cb[] = new char[4096];
        try {
        	StringBuilder buf = new StringBuilder();
        	int len = 0;
			while ((len = reader.read(cb)) != -1) {
				buf.append(cb, 0, len);
			}
			setString(parameterIndex, buf.toString());
		} catch (IOException e) {
			throw new HeraSQLException("IO exception reading Reader", e);
		}
	}

	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		if (reader == null) {
			setNull(parameterIndex, Types.VARCHAR);
			return;
		}
		if (length > Integer.MAX_VALUE)
			throw new HeraSQLException("Reader too large");

        char cb[] = new char[(int)length];
        try {
        	int bytes = reader.read(cb);
        	if (bytes > 0) {
        		setString(parameterIndex, new String(cb));
        	} else {
        		setString(parameterIndex, new String(""));
        	}
		} catch (IOException e) {
			throw new HeraSQLException("IO exception reading Reader", e);
		}
	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setClob is not implemented");
	}

	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setClob is not implemented");
	}

	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setNCharacterStream is not implemented");
	}

	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setNCharacterStream is not implemented");
	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setNClob is not implemented");
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setNClob is not implemented");
	}

	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setNClob is not implemented");
	}

	public void setNString(int parameterIndex, String value) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setNString is not implemented");
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setRowId is not implemented");
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setSQLXML is not implemented");
	}

	public boolean isClosed() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.isClosed is not implemented");
	}

	public boolean isPoolable() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.isPoolable is not implemented");
		
	}

	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.setPoolable is not implemented");
		
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.isWrapperFor is not implemented");
		
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraPreparedStatement.unwrap is not implemented");
		
	}

	public byte[] shardProcessing(String _shard_key, ArrayList<Integer> _shard_key_bind_pos, ArrayList<Integer> _scuttle_id_pos) throws SQLException {
		if (_shard_key_bind_pos.size() == 0) {
			return null;
		}
		if (_shard_key_bind_pos.size() != _scuttle_id_pos.size()) {
			throw new HeraInternalErrorException("shard_key_vec.size() != scuttle_id_vec.size()");
		}
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();	
			baos.write(_shard_key.getBytes());
			baos.write('=');
			for (int i = 0; i < _shard_key_bind_pos.size(); i++) {
				if (i != 0) {
					baos.write(';');
				}
				byte[] _data = in_params.get(_shard_key_bind_pos.get(i)).getSecond();
				baos.write(_data);
				// scuttle id
				setInt(_scuttle_id_pos.get(i), HeraJdbcUtil.getScuttleID(_data));
			}
			return baos.toByteArray();
		} catch (IOException ex) {
			throw new HeraIOException(ex);
		}
	}
	
}
