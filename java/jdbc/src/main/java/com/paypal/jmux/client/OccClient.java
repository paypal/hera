package com.paypal.jmux.client;

import java.sql.SQLException;
import java.util.ArrayList;

import com.paypal.jmux.constants.BindType;
import com.paypal.jmux.ex.OccClientException;
import com.paypal.jmux.ex.OccExceptionBase;
import com.paypal.jmux.ex.OccIOException;
import com.paypal.jmux.ex.OccInternalErrorException;
import com.paypal.jmux.ex.OccProtocolException;
import com.paypal.jmux.ex.OccSQLException;
import com.paypal.jmux.ex.OccTimeoutException;
import com.paypal.jmux.util.OccColumnMeta;

public interface OccClient {
	public void prepare(String _sql) throws OccIOException;
	public boolean execute(int _num_rows, boolean _add_commit) throws OccIOException, OccTimeoutException, OccClientException, OccProtocolException;
	public ArrayList<OccColumnMeta> iterateColumns() throws OccTimeoutException, OccIOException, OccClientException;
	public ArrayList<OccColumnMeta> execQuery(int _num_rows, boolean _column_meta) throws OccIOException, OccTimeoutException, OccClientException;
	public void execDML(boolean _add_commit) throws SQLException;
	public ArrayList<ArrayList<byte[]> > fetch(int _num_rows) throws OccIOException, OccClientException, OccTimeoutException, OccInternalErrorException ;
	public void bind(String _variable, BindType _type, byte[] _value) throws OccIOException, OccSQLException;
	public void bindOut(String _variable) throws OccIOException, OccSQLException;
	public ArrayList<ArrayList<byte[]> > fetchOutBindVars(int _bind_var_count) throws OccTimeoutException, OccIOException, OccClientException, OccInternalErrorException ;
	public void commit() throws OccClientException, OccIOException, OccProtocolException;
	public void rollback() throws OccIOException, OccProtocolException, OccClientException;
	public void sendCalCorrId() throws OccIOException;
	public String sendClientInfo(String _info, String _name) throws OccExceptionBase ;
	public int getRows();
	public void reset();
	public void close() throws OccIOException;
	public ArrayList<OccColumnMeta> getColumnMeta() throws OccIOException;
	public void bindArray(String _variable, int _max_sz, BindType _type, ArrayList<byte[]> _values) throws OccIOException, OccSQLException;
	public void shardKey(byte[] _data) throws OccIOException;
	public int getNumShards() throws OccIOException, OccProtocolException, OccClientException;
	public void setShard(int _shard_id) throws OccIOException, OccProtocolException, OccClientException;
	public void ping() throws OccExceptionBase;
	public void setServerLogicalName(String name);
	public void setCalLogOption(String isCalEnabled);
	public void setOccBoxName(String occBoxName);
}
