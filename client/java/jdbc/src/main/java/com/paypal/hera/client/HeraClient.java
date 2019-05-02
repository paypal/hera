package com.paypal.hera.client;

import java.sql.SQLException;
import java.util.ArrayList;

import com.paypal.hera.constants.BindType;
import com.paypal.hera.ex.HeraClientException;
import com.paypal.hera.ex.HeraExceptionBase;
import com.paypal.hera.ex.HeraIOException;
import com.paypal.hera.ex.HeraInternalErrorException;
import com.paypal.hera.ex.HeraProtocolException;
import com.paypal.hera.ex.HeraSQLException;
import com.paypal.hera.ex.HeraTimeoutException;
import com.paypal.hera.util.HeraColumnMeta;

public interface HeraClient {
	public void prepare(String _sql) throws HeraIOException;
	public boolean execute(int _num_rows, boolean _add_commit) throws HeraIOException, HeraTimeoutException, HeraClientException, HeraProtocolException;
	public ArrayList<HeraColumnMeta> iterateColumns() throws HeraTimeoutException, HeraIOException, HeraClientException;
	public ArrayList<HeraColumnMeta> execQuery(int _num_rows, boolean _column_meta) throws HeraIOException, HeraTimeoutException, HeraClientException;
	public void execDML(boolean _add_commit) throws SQLException;
	public ArrayList<ArrayList<byte[]> > fetch(int _num_rows) throws HeraIOException, HeraClientException, HeraTimeoutException, HeraInternalErrorException ;
	public void bind(String _variable, BindType _type, byte[] _value) throws HeraIOException, HeraSQLException;
	public void bindOut(String _variable) throws HeraIOException, HeraSQLException;
	public ArrayList<ArrayList<byte[]> > fetchOutBindVars(int _bind_var_count) throws HeraTimeoutException, HeraIOException, HeraClientException, HeraInternalErrorException ;
	public void commit() throws HeraClientException, HeraIOException, HeraProtocolException;
	public void rollback() throws HeraIOException, HeraProtocolException, HeraClientException;
	public void sendCalCorrId() throws HeraIOException;
	public String sendClientInfo(String _info, String _name) throws HeraExceptionBase ;
	public int getRows();
	public void reset();
	public void close() throws HeraIOException;
	public ArrayList<HeraColumnMeta> getColumnMeta() throws HeraIOException;
	public void bindArray(String _variable, int _max_sz, BindType _type, ArrayList<byte[]> _values) throws HeraIOException, HeraSQLException;
	public void shardKey(byte[] _data) throws HeraIOException;
	public int getNumShards() throws HeraIOException, HeraProtocolException, HeraClientException;
	public void setShard(int _shard_id) throws HeraIOException, HeraProtocolException, HeraClientException;
	public void ping() throws HeraExceptionBase;
	public void setServerLogicalName(String name);
	public void setCalLogOption(String isCalEnabled);
	public void setHeraHostName(String heraBoxName);
}
