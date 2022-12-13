package com.paypal.hera.client;

import com.paypal.hera.cal.CalClientConfigMXBeanImpl;
import com.paypal.hera.cal.CalPoolStackInfo;
import com.paypal.hera.cal.CalStreamUtils;
import com.paypal.hera.cal.CalTransaction;
import com.paypal.hera.cal.CalTransactionFactory;
import com.paypal.hera.cal.CalTransactionHelper;
import com.paypal.hera.cal.ClsLogOutputHelper;
import com.paypal.hera.cal.StackTrace;
import com.paypal.hera.conn.HeraClientConnection;
import com.paypal.hera.constants.BindType;
import com.paypal.hera.constants.Consts;
import com.paypal.hera.constants.HeraConstants;
import com.paypal.hera.constants.HeraJdbcDriverConstants;
import com.paypal.hera.ex.HeraClientException;
import com.paypal.hera.ex.HeraExceptionBase;
import com.paypal.hera.ex.HeraIOException;
import com.paypal.hera.ex.HeraInternalErrorException;
import com.paypal.hera.ex.HeraProtocolException;
import com.paypal.hera.ex.HeraSQLException;
import com.paypal.hera.ex.HeraTimeoutException;
import com.paypal.hera.jdbc.HeraConnection;
import com.paypal.hera.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.SocketException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

@SuppressWarnings("deprecation")
public class HeraClientImpl implements HeraClient{
	private enum State {
		INITIAL, FETCH_CMD_NEEDED, FETCH_CMD_SENT, FETCH_IN_PROGRESS, FETCH_DONE;
	};
	
	private class ClientInfo {
		public long pid;
		public String hostName;
		public String cmdLine;
		public String poolName;
		public String poolStack;
	};
	
	static final Logger LOGGER = LoggerFactory.getLogger(HeraClientImpl.class);
	private static final String ENABLE_FULL_CAL = "-1";
	private static final String DISABLE_CAL = "0";
	private NetstringReader is;
	private NetstringWriter os;
	private int connTimeout;
	
	private State state;
	private int rows;
	private int columns;
	private Iterator<NetStringObj> response;
	
	private ClientInfo  clientInfo;
	private boolean columnNamesEnabled;
	private boolean columnInfoEnabled;
	private HeraClientConnection conn;
	ArrayList<HeraColumnMeta> colMetaData;
	private String sql;
	private long lastStmtId;
	private int byteCount;
	private String serverLogicalName;
	private String calLogFrequency;
	private String heraHostName;

	private boolean readOnly;
	private boolean isFirstSQL;
	//TODO: migrate stale conn impl from OpenDAK.

	public HeraClientImpl(HeraClientConnection _conn, int _connTimeout, boolean _columnNamesEnabled, boolean _columnInfoEnabled) throws HeraExceptionBase{
		conn = _conn;
		is = new NetstringReader(new BufferedInputStream(conn.getInputStream()));
		os = new NetstringWriter(conn.getOutputStream());
		connTimeout = _connTimeout;
		state = State.INITIAL;
		response = null;
		clientInfo = new ClientInfo();
		sql = null;
		byteCount = 0;
		serverLogicalName = "unknown";
		calLogFrequency = ENABLE_FULL_CAL; // -1 is for full cal enabled. 0 for disabled. 
		
		try {
			// TODO: (some of) these should come as system properties to not have CAL dependency 
			clientInfo.pid = this.computePid();
			clientInfo.cmdLine = ManagementFactory.getRuntimeMXBean().getName();
			clientInfo.hostName = java.net.InetAddress.getLocalHost().getHostName();
			clientInfo.poolName = CalClientConfigMXBeanImpl.getInstance().getPoolname();
			CalPoolStackInfo stackInfo = CalPoolStackInfo.getCalPoolStackInfo();
			if (stackInfo != null)
				clientInfo.poolStack = "PoolStack: " + stackInfo.getPoolStack();
			
		} catch (Exception e) {
			LOGGER.info("Could not get client info, ex: " + e.getMessage());
		}
		columnNamesEnabled = _columnNamesEnabled;
		columnInfoEnabled = _columnInfoEnabled;
	}
	
	private NetStringObj getResponse(String _cmd) throws HeraIOException, HeraProtocolException {
		try {
			response = is.parse();
		} catch (IOException e) {
			throw new HeraIOException(e,getConnectionMetaInfo());
		}
		if (!response.hasNext())
			throw new HeraProtocolException("Invalid response for " + _cmd);
		return response.next();
	}

	public void setServerLogicalName(String serverName){
		serverLogicalName = serverName;
	}

	public void setCalLogOption(String isCalEnabled){
		if(isCalEnabled != null) {
			calLogFrequency = isCalEnabled;
		}
	}
	
	public void prepare(String _sql) throws HeraIOException{
		sendCalCorrId();
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::prepare(" + _sql + ") {}", conn.getConnectionId());
		os.add(HeraConstants.HERA_PREPARE_V2, _sql.getBytes());
		sql = _sql;
	}

    private CalTransaction startCalExecTransaction() {
    	CalTransaction transaction;
    	if(!calLogFrequency.equals(DISABLE_CAL)) {
            // make sure to write sql statement to CAL before starting a new transaction
            lastStmtId = ClsLogOutputHelper.writeSQLStmt(this.sql);

            transaction = CalTransactionFactory.create("EXEC");
            transaction.setName(Long.toString(lastStmtId));
            transaction.addData("HOST", serverLogicalName);
    	} else {
    		// cal disabled return nulcaltransaction
    		transaction = CalStreamUtils.getInstance().getDefaultCalStream().transaction("EXEC");
    	}
        return transaction;
    }

	
	public boolean execute(int _num_rows, boolean _add_commit) throws HeraIOException, HeraTimeoutException, HeraClientException, HeraProtocolException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::execute(" + _num_rows + ") {}", conn.getConnectionId());

        CalTransaction execCalTxn = startCalExecTransaction();
        execCalTxn.setStatus("0");

		os.add(HeraConstants.HERA_EXECUTE);
	
		// flush the accumulated commands
		try {
	        os.flush();
			NetStringObj obj = read_response();
			if (obj.getCommand() == HeraConstants.HERA_VALUE) 
				columns = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			obj = read_response();
			if (obj.getCommand() == HeraConstants.HERA_VALUE) 
				rows = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("HeraClient::execQuery() returned cols=" + columns + ",rows=" + rows +
						" connId:" +  conn.getConnectionId());

			 
			if (columns > 0) {
				//non-DML(select) like executeQuery 	
				if (columnInfoEnabled) {
					os.add(HeraConstants.HERA_COLS_INFO);
					os.flush();
				} else {
					if (columnNamesEnabled) {
						os.add(HeraConstants.HERA_COLS);
						os.flush();
					}
				}	
				state = State.FETCH_CMD_SENT;
				os.add(HeraConstants.HERA_FETCH, _num_rows);
				os.flush();
				
				colMetaData = iterateColumns();

				return true;
				
			} else {
					//DML(insert, update, delete) like executeUpdate
					if (_add_commit) {
						os.add(HeraConstants.HERA_COMMIT);
						os.flush();
						NetStringObj resp = getResponse( "HERA_AUTO_COMMIT");
		                if (resp.getCommand() != HeraConstants.HERA_OK) {
		                	HeraClientException heraEx = new HeraClientException("commit: Error " + Integer.toString((int)resp.getCommand()));
		                	handleException(heraEx, execCalTxn);
		                    throw heraEx;
		                }
					} 
					return false;
			}
			
		} catch (IOException e) {
			HeraIOException heraEx = new HeraIOException(e, getConnectionMetaInfo());
			handleException(heraEx, execCalTxn);
			throw heraEx;
		} catch (HeraTimeoutException | HeraClientException | HeraProtocolException e) {
			handleException(e, execCalTxn);
			throw e;
		}finally {
			execCalTxn.completed();
		}
	}
		
	public ArrayList<HeraColumnMeta> execQuery(int _num_rows, boolean _column_meta) throws HeraIOException, HeraTimeoutException, HeraClientException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::execQuery(" + _num_rows + ") connId: " + conn.getConnectionId());

		CalTransaction execCalTxn = startCalExecTransaction();
		execCalTxn.setStatus("0");

		os.add(HeraConstants.HERA_EXECUTE);
		if (_column_meta) {
			if (columnInfoEnabled)
				os.add(HeraConstants.HERA_COLS_INFO);
			else
				if (columnNamesEnabled)
					os.add(HeraConstants.HERA_COLS);
		}
		state = State.FETCH_CMD_SENT;
		os.add(HeraConstants.HERA_FETCH, _num_rows);
		// flush the accumulated commands
		try {
	        os.flush();
			NetStringObj obj = read_response();
			if (obj.getCommand() == HeraConstants.HERA_VALUE) 
				columns = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			obj = read_response();
			if (obj.getCommand() == HeraConstants.HERA_VALUE) 
				rows = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("HeraClient::execQuery() returned cols=" + columns + ",rows=" + rows +
						" connId: " + conn.getConnectionId());
			ArrayList<HeraColumnMeta> columnMeta = null;
			if (_column_meta && (columnNamesEnabled || columnInfoEnabled)) {
				columnMeta = new ArrayList<HeraColumnMeta>(); 
				// column names 
				obj = read_response();
				if (obj.getCommand() == HeraConstants.HERA_VALUE) 
					columns = Integer.parseInt(new String(obj.getData(), "UTF-8"));
				else
					check_error(obj);
				for (int i = 0; i < columns; i++) {
					HeraColumnMeta meta = new HeraColumnMeta();
					// name
					obj = read_response();
					if (obj.getCommand() == HeraConstants.HERA_VALUE) {
						meta.setName(new String(obj.getData(), "UTF-8"));
					}
					else
						check_error(obj);
					if (columnInfoEnabled) {
						// type
						obj = read_response();
						if (obj.getCommand() == HeraConstants.HERA_VALUE) {
							meta.setType(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						}
						else
							check_error(obj);
						// width
						obj = read_response();
						if (obj.getCommand() == HeraConstants.HERA_VALUE) {
							meta.setWidth(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						}
						else
							check_error(obj);
						// precision
						obj = read_response();
						if (obj.getCommand() == HeraConstants.HERA_VALUE) {
							meta.setPrecision(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						}
						else
							check_error(obj);
						// scale
						obj = read_response();
						if (obj.getCommand() == HeraConstants.HERA_VALUE) {
							meta.setScale(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						}
						else
							check_error(obj);
					}
					
					columnMeta.add(meta);
				}
			}
			return columnMeta;
		} catch (IOException e) {
			HeraIOException heraEx = new HeraIOException(e);
			handleException(heraEx, execCalTxn);
			throw heraEx;
		}catch (HeraClientException | HeraIOException | HeraTimeoutException e) {
			handleException(e, execCalTxn);
			throw e;
		}finally {
			execCalTxn.completed();
		}
	}

	public ArrayList<HeraColumnMeta> iterateColumns() throws HeraTimeoutException, HeraIOException, HeraClientException {
		NetStringObj obj;
		ArrayList<HeraColumnMeta> columnMeta = null;
		if (columnNamesEnabled || columnInfoEnabled) {
			columnMeta = new ArrayList<HeraColumnMeta>(); 
			// column names 
			obj = read_response();
			if (obj.getCommand() == HeraConstants.HERA_VALUE)
				try {
					columns = Integer.parseInt(new String(obj.getData(), "UTF-8"));
				} catch (NumberFormatException|UnsupportedEncodingException e) {
					throw new HeraClientException("Exception:", e);
				}
			else
				check_error(obj);
			for (int i = 0; i < columns; i++) {
				HeraColumnMeta meta = new HeraColumnMeta();
				// name
				obj = read_response();
				if (obj.getCommand() == HeraConstants.HERA_VALUE) {
					try {
						meta.setName(new String(obj.getData(), "UTF-8"));
					} catch (UnsupportedEncodingException e) {
						throw new HeraClientException("Exception:", e);
					}
				}
				else
					check_error(obj);
				if (columnInfoEnabled) {
					// type
					obj = read_response();
					if (obj.getCommand() == HeraConstants.HERA_VALUE) {
						try {
							meta.setType(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						} catch (NumberFormatException | UnsupportedEncodingException e) {
							throw new HeraClientException("Exception:", e);
						}
					}
					else
						check_error(obj);
					// width
					obj = read_response();
					if (obj.getCommand() == HeraConstants.HERA_VALUE) {
						try {
							meta.setWidth(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						} catch (NumberFormatException | UnsupportedEncodingException e) {
							throw new HeraClientException("Exception:", e);
						}
					}
					else
						check_error(obj);
					// precision
					obj = read_response();
					if (obj.getCommand() == HeraConstants.HERA_VALUE) {
						try {
							meta.setPrecision(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						} catch (NumberFormatException | UnsupportedEncodingException e) {
							throw new HeraClientException("Exception:", e);
						}
					}
					else
						check_error(obj);
					// scale
					obj = read_response();
					if (obj.getCommand() == HeraConstants.HERA_VALUE) {
						try {
							meta.setScale(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						} catch (NumberFormatException | UnsupportedEncodingException e) {
							throw new HeraClientException("Exception:", e);
						}
					}
					else
						check_error(obj);
				}
				
				columnMeta.add(meta);
			}
		}
		return columnMeta;
	} 	

	public boolean packetHasMoreData() {
		return response.hasNext();
	}
	
	public void execDML(boolean _add_commit) throws SQLException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::execDML(" + _add_commit + ") " +
					"connId: " + conn.getConnectionId() + " SQL: " + this.sql);
		if (readOnly) {
			String msg = "DML Operation called on ReadOnly Connection";
			LOGGER.error("HeraClient::execDML " + msg);
			throw new SQLException(msg);
		}

		CalTransaction execCalTxn = startCalExecTransaction();
		execCalTxn.setStatus("0");

		os.add(HeraConstants.HERA_EXECUTE);
		if (_add_commit)
			os.add(HeraConstants.HERA_COMMIT);
		boolean do_commit = _add_commit;
		// flush the accumulated commands
		try {
			os.flush();
			NetStringObj obj = read_response();
			if (obj.getCommand() == HeraConstants.HERA_VALUE) 
				columns = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			obj = read_response();
			if (obj.getCommand() == HeraConstants.HERA_VALUE) 
				rows = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("HeraClient::execDML() returned cols=" + columns + ",rows=" + rows
				+ " connId: " + conn.getConnectionId());
		} catch (IOException e) {
			HeraIOException heraEx = new HeraIOException(e);
			handleException(heraEx, execCalTxn);
			do_commit = false;
			throw heraEx;
		} catch (HeraTimeoutException e) {
			handleException(e, execCalTxn);
			do_commit = false;
			throw e;
		} catch (SQLException e) {
			handleException(e, execCalTxn);
			//for SQLException do not make it false. It is used in finally block to consume HERA_COMMIT response
			//do_commit = false;  
			throw e;
		} finally {
			try {
				handlecommit(do_commit,execCalTxn);
			} finally {
				execCalTxn.completed();
			}
		}
	}


	 private void handlecommit(boolean do_commit, CalTransaction execCalTxn) throws HeraIOException, HeraProtocolException, HeraClientException {
		 if (do_commit) {
			 NetStringObj resp = getResponse("HERA_AUTO_COMMIT");
			 if (resp.getCommand() != HeraConstants.HERA_OK) {
				 HeraClientException heraEx = new HeraClientException("commit: Error " + Integer.toString((int) resp.getCommand()));
				 handleException(heraEx, execCalTxn);
				 execCalTxn.completed();
				 throw heraEx;
			 }
		 }
	}


	private String getThrowableName(Throwable e) {
		if (e == null) {
			return "log";
		}
		String name = e.getClass().getName();
		int index = name.lastIndexOf('.');
		if (index >= 0) {
			return name.substring(index + 1);
		} else {
			return name;
		}
	}

	private void handleException(SQLException e,
			CalTransaction transaction) {
		if (HeraJdbcDriverConstants.getInstance().shouldLogInCal(e)) {
			String errorCode = "";
			String errorMsg = "";
			if (e != null) {
				errorCode += e.getErrorCode();
				errorMsg += e.getMessage();
			}
			String status = getThrowableName(e);
			status += "." + errorCode;
			transaction.setStatus(status);

			if (errorMsg.length() > 0) {
				if (errorMsg.endsWith("\n")) {
					errorMsg = errorMsg.substring(0, errorMsg.length() - 1);
				}
				transaction.addData("ExceptionMsg ", errorMsg);
			}
			if (e != null) {
				transaction.addData("st", StackTrace.getStackTrace(e));
			}
		} else {
			transaction.setStatus("0");
		}
	}

	public ArrayList<ArrayList<byte[]> > fetch(int _num_rows) throws HeraIOException, HeraClientException, HeraTimeoutException, HeraInternalErrorException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::fetch(" + _num_rows + ") connId: " + conn.getConnectionId());

		CalTransaction fetchCalTxn;
		if(!calLogFrequency.equals(DISABLE_CAL)) {
			fetchCalTxn = CalTransactionFactory.create("FETCH");
		} else {
			// cal disabled return nulcaltransaction
			fetchCalTxn = CalStreamUtils.getInstance().getDefaultCalStream().transaction("FETCH");
		}
		fetchCalTxn.setName(Long.toString(lastStmtId));
		fetchCalTxn.addData("HOST", serverLogicalName);

		if (state == State.FETCH_DONE)
			return new ArrayList<ArrayList<byte[]> >();
		if (state == State.FETCH_CMD_NEEDED) {
			os.add(HeraConstants.HERA_FETCH, _num_rows);
			try {
				os.flush();
				state = State.FETCH_CMD_SENT;
			} catch (IOException e) {
				HeraIOException heraEx = new HeraIOException(e,getConnectionMetaInfo());
				handleException(heraEx, fetchCalTxn);
				fetchCalTxn.completed();
				throw heraEx;
			}
		}
		ArrayList<ArrayList<byte[]> > result = load_results(Integer.MAX_VALUE);

		fetchCalTxn.addData("bytes", String.valueOf(byteCount) );
		fetchCalTxn.addData("rows", String.valueOf(result.size()) );
		fetchCalTxn.setStatus("0");
		fetchCalTxn.completed();
		
		return result;
	}
	
	public void bind(String _variable, BindType _type, byte[] _value) throws HeraIOException, HeraSQLException{
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::bind() {} {}", conn.getConnectionId(), _value);
		try {
			os.add(HeraConstants.HERA_BIND_NAME, HeraJdbcConverter.string2hera(_variable));
		} catch (UnsupportedEncodingException e) {
			throw new HeraSQLException("can't encode in variable name", e);
		}
		if (_type != BindType.HERA_TYPE_STRING)
			os.add(HeraConstants.HERA_BIND_TYPE, _type.getValue());
		os.add(HeraConstants.HERA_BIND_VALUE, _value);
	}

	public void bindOut(String _variable) throws HeraIOException, HeraSQLException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::bind_out() {}", conn.getConnectionId());
		try {
			os.add(HeraConstants.HERA_BIND_OUT_NAME, HeraJdbcConverter.string2hera(_variable));
		} catch (UnsupportedEncodingException e) {
			throw new HeraSQLException("can't encode out variable name", e);
		}
	}
	
	@Override
	public void bindArray(String _variable, int _max_sz, BindType _type, ArrayList<byte[]> _values) throws HeraIOException, HeraSQLException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::bindArray() {}", conn.getConnectionId());
		try {
			os.add(HeraConstants.HERA_BIND_NAME, HeraJdbcConverter.string2hera(_variable));
		} catch (UnsupportedEncodingException e) {
			throw new HeraSQLException("can't encode in variable name", e);
		}
		if (_type != BindType.HERA_TYPE_STRING)
			os.add(HeraConstants.HERA_BIND_TYPE, _type.getValue());
		os.add(HeraConstants.HERA_ARRAY_LENGTH, HeraJdbcConverter.int2hera(_values.size()));
		os.add(HeraConstants.HERA_ARRAY_MAX_VALUESZ, HeraJdbcConverter.int2hera(_max_sz));
		for (int i = 0; i < _values.size(); i++) {
			os.add(HeraConstants.HERA_BIND_VALUE, _values.get(i));
		}
	}

	public ArrayList<ArrayList<byte[]> > fetchOutBindVars(int _bind_var_count) throws HeraTimeoutException, HeraIOException, HeraClientException, HeraInternalErrorException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::fetch_out_bind_vars() {}", conn.getConnectionId());
		NetStringObj obj = read_response();
		if (obj.getCommand() != HeraConstants.HERA_VALUE)
			check_error(obj);
		try {
			rows = Integer.parseInt(new String(obj.getData(), "UTF-8"));
		} catch (NumberFormatException | UnsupportedEncodingException e) {
			throw new HeraClientException("Exception:", e);
		}
		columns = _bind_var_count;
		return load_results(rows);
	}	
	
	public ArrayList<ArrayList<byte[]> > load_results(int _rows) throws HeraClientException, HeraTimeoutException, HeraIOException, HeraInternalErrorException {
		ArrayList<ArrayList<byte[]> > ret = new ArrayList<ArrayList<byte[]> >();
		byteCount = 0;
		for (int i = 0; i < _rows; i++) {
			ArrayList<byte[]> row = new ArrayList<byte[]>();
			if (columns <= 0)
				throw new HeraInternalErrorException("For the query '" + sql + "' the number of column is incorrect: " + columns);
			for (int j = 0; j < columns; j++) {
				NetStringObj obj = read_response();
				if (obj.getCommand() == HeraConstants.HERA_NO_MORE_DATA) {
					state = State.FETCH_DONE;
					return ret;
				}
				if (obj.getCommand() == HeraConstants.HERA_OK) {
					state = State.FETCH_CMD_NEEDED;
					return ret;
				}
				if (obj.getCommand() == HeraConstants.HERA_VALUE) {
					row.add(obj.getData());
					// collect number of bytes of resultset to log them to cal later
					byteCount += obj.getData().length;
				}
				else
					check_error(obj);
			}
			ret.add(row);
		}
		return ret;
	}

	public void commit() throws HeraClientException, HeraIOException, HeraProtocolException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::commit() {}", conn.getConnectionId());
		os.add(HeraConstants.HERA_COMMIT);
		try {
			os.flush();
			NetStringObj resp = getResponse("HERA_COMMIT");
			if (resp.getCommand() != HeraConstants.HERA_OK)
				throw new HeraClientException("commit: Error " + Integer.toString((int)resp.getCommand()));
		} catch (IOException e) {
			throw new HeraIOException(e,getConnectionMetaInfo());
		}
	}

	public void rollback() throws HeraIOException, HeraProtocolException, HeraClientException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::rollback() {}", conn.getConnectionId());
		os.add(HeraConstants.HERA_ROLLBACK);
		try {
			os.flush();
			NetStringObj resp = getResponse("HERA_ROLLBACK");
			if (resp.getCommand() != HeraConstants.HERA_OK)
				throw new HeraClientException("rollback: Error " + Integer.toString((int)resp.getCommand()));
		} catch (IOException e) {
			throw new HeraIOException(e,getConnectionMetaInfo());
		}
	}
	
	private void check_error(NetStringObj obj) throws HeraClientException {
		String errorMessage = null;
		try {
			errorMessage = new String(obj.getData(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new HeraClientException("Exception:", e);
		}
		Pair<String, Integer> errInfo = HeraJdbcUtil.ErrorToSqlStateAndVendorCodeConverter(errorMessage);
			switch ((int)obj.getCommand()) {
			case HeraConstants.HERA_SQL_ERROR:
				throw new HeraClientException(Consts.HERA_SQL_ERROR_PREFIX + errorMessage, errInfo.getFirst(), errInfo.getSecond());
			case HeraConstants.HERA_ERROR:
				throw new HeraClientException("Hera error: " + errorMessage + getConnectionMetaInfo().toString() , errInfo.getFirst());
			case HeraConstants.HERA_MARKDOWN:
				throw new HeraClientException("Hera markdown: " + errorMessage, errInfo.getFirst());
			default:
				throw new HeraClientException("Unknown error: cmd=" + obj.getCommand() + ", data=" + errorMessage);
			}
	}
	
	private NetStringObj read_response() throws HeraTimeoutException, HeraIOException {
		try {
			long start = System.currentTimeMillis();
			while (true) {
				if ((response == null) || (!response.hasNext()))
					response =is.parse();
				NetStringObj obj = response.next();
				if (obj.getCommand() == HeraConstants.HERA_STILL_EXECUTING) {
					if (LOGGER.isInfoEnabled())
						LOGGER.info("Still executing ...");
					long now = System.currentTimeMillis();
					if (now - start > connTimeout)
						throw new HeraTimeoutException("Timeout waiting for response");
				} else {
					return obj;
				}
			}
		} catch (IOException e) {

			throw new HeraIOException(e,getConnectionMetaInfo());
		}
	}

	protected String getCorrelationId() {
		CalTransaction topTransaction = CalTransactionHelper.getTopTransaction();
		if (topTransaction == null) {
			return "NotSet";
		}
		return topTransaction.getCorrelationId();
	}
	
	@Override
	public void sendCalCorrId() throws HeraIOException
	{
		String buffer;
		buffer = "CorrId=" + getCorrelationId();
		CalPoolStackInfo stackInfo = CalPoolStackInfo.getCalPoolStackInfo();
		if (stackInfo != null)
			buffer += "&PoolStack: " + stackInfo.getPoolStack();
		os.add(HeraConstants.CLIENT_CAL_CORRELATION_ID, buffer.getBytes());
	}
	
	@Override
	public String sendClientInfo(String info, String name)
			throws HeraExceptionBase {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::sendClientInfo() {}", conn.getConnectionId());
		String buffer;
		if (name.isEmpty())
			buffer  = "PID: " + clientInfo.pid + ",HOST: " + clientInfo.hostName + ", EXEC: " + clientInfo.cmdLine + 
				", Poolname: " + clientInfo.poolName + ", Command: " + info + ", " + clientInfo.poolStack + ", Name: " + name;
		else
			buffer  = "PID: " + clientInfo.pid + ",HOST: " + clientInfo.hostName + ", EXEC: " + clientInfo.cmdLine + 
				", Poolname: " + clientInfo.poolName + ", Command: " + info + ", " + clientInfo.poolStack;
		os.add(HeraConstants.HERA_CLIENT_INFO, buffer.getBytes());
		try {
			os.flush();
			NetStringObj resp = getResponse("HERA_CLIENT_INFO");
			if (resp.getCommand() != HeraConstants.HERA_OK)
				throw new HeraClientException("HERA_CLIENT_INFO: Error " + Integer.toString((int)resp.getCommand()));
			return new String(resp.getData(), "UTF-8");
		} catch (IOException e) {
			throw new HeraIOException(e,getConnectionMetaInfo());
		}
	}

	public int getRows() {
		return rows;
	}

	@Override
	public void reset() {
		os.reset();		
	}

	@Override
	public void close() throws HeraIOException {
		conn.close();		
	}
	/**
	 * This really should be in a kernel jar somewhere
	 * but not sure where.
	 * @return
	 */
	private long computePid() {
		java.lang.management.RuntimeMXBean mx = ManagementFactory
				.getRuntimeMXBean();
		String[] mxNameTable = mx.getName().split("@");
		long pid = Thread.currentThread().getId();
		if (mxNameTable.length == 2) {
			try {
				pid = Long.parseLong(mxNameTable[0]);
			} catch (NumberFormatException nfe) {
				LOGGER.debug("caught NumberFormatException : " + mxNameTable[0]);
			}
		}
		return pid;
    }
	@Override
	public ArrayList<HeraColumnMeta> getColumnMeta() throws HeraIOException {
		return colMetaData;		
	}

	@Override
	public void shardKey(byte[] _data) throws HeraIOException {
		os.add(HeraConstants.HERA_SHARD_KEY, _data);
	}

	@Override
	public int getNumShards() throws HeraIOException, HeraProtocolException, HeraClientException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::getNumShards() {}", conn.getConnectionId());
		os.add(HeraConstants.HERA_GET_NUM_SHARDS);
		try {
			os.flush();
			NetStringObj resp = getResponse("HERA_GET_NUM_SHARDS");
			if (resp.getCommand() != HeraConstants.HERA_OK)
				throw new HeraClientException("commit: Error " + Integer.toString((int)resp.getCommand()));
			return Integer.parseInt(new String(resp.getData(), "UTF-8"));
		} catch (IOException e) {
			throw new HeraIOException(e,getConnectionMetaInfo());
		}
	}

	@Override
	public void setShard(int _shard_id) throws HeraIOException, HeraProtocolException, HeraClientException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::setShard() {}", conn.getConnectionId());
		os.add(HeraConstants.HERA_SET_SHARD_ID, _shard_id);
		try {
			os.flush();
			NetStringObj resp = getResponse("HERA_SET_SHARD_ID");
			if (resp.getCommand() != HeraConstants.HERA_OK)
				throw new HeraClientException("setShard(" + _shard_id + ") error code: " + Integer.toString((int)resp.getCommand()) + ": " + new String(resp.getData(), "UTF-8"));
		} catch (IOException e) {
			throw new HeraIOException(e,getConnectionMetaInfo());
		}
	}

	@Override
	public void ping(int tmo) throws HeraExceptionBase {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("HeraClient::sendPing() {}", conn.getConnectionId());
		os.add(HeraConstants.SERVER_PING_COMMAND, "".getBytes());
		try {
			os.flush();
			int oldTmo = 0;
			if (tmo > 0) {
				oldTmo = conn.getSoTimeout();
				conn.setSoTimeout(tmo);
			}
			NetStringObj resp = getResponse("HERA_CLIENT_PING");
			// restore the timeout
			if (tmo > 0) {
				conn.setSoTimeout(oldTmo);
			}
			if (resp.getCommand() != HeraConstants.SERVER_ALIVE){
				throw new HeraClientException("HeraClient::sendPing(): Error " + resp.getCommand());
			}
		} catch (IOException e) {
			throw new HeraIOException(e,getConnectionMetaInfo());
		}
	}

	@Override
	public void setHeraHostName(String heraBoxName){
		this.heraHostName = heraBoxName;
	}


	ConnectionMetaInfo getConnectionMetaInfo(){
		ConnectionMetaInfo connectionMetaInfo = new ConnectionMetaInfo();
		connectionMetaInfo.setServerBoxName(heraHostName);
		return connectionMetaInfo;
	}

	@Override
	public void setFirstSQL(boolean isFirstSQL) {
		this.isFirstSQL = isFirstSQL;
	}

	@Override
	public void setSOTimeout(int timeoutInMS) throws SocketException {
		conn.setSoTimeout(timeoutInMS);
	}

	@Override
	public int getSOTimeout() throws SocketException {
		return conn.getSoTimeout();
	}

	@Override
	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	@Override
	public boolean isReadOnly() {
		return this.readOnly;
	}

	@Override
	public String getHeraClientConnID(){
		return conn.getConnectionId();
	}

}
