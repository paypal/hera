package com.paypal.jmux.client;

import com.paypal.jmux.conn.OccClientConnection;
import com.paypal.jmux.constants.BindType;
import com.paypal.jmux.constants.Consts;
import com.paypal.jmux.constants.OccConstants;
import com.paypal.jmux.constants.OccJdbcDriverConstants;
import com.paypal.jmux.ex.OccClientException;
import com.paypal.jmux.ex.OccTimeoutException;
import com.paypal.jmux.ex.OccProtocolException;
import com.paypal.jmux.ex.OccIOException;
import com.paypal.jmux.ex.OccExceptionBase;
import com.paypal.jmux.ex.OccSQLException;
import com.paypal.jmux.ex.OccInternalErrorException;
import com.paypal.jmux.util.*;
import com.paypal.jmux.util.NetStringObj;
import com.paypal.jmux.cal.CalClientConfigMXBeanImpl;
import com.paypal.jmux.cal.CalPoolStackInfo;
import com.paypal.jmux.cal.CalStreamUtils;
import com.paypal.jmux.cal.CalTransaction;
import com.paypal.jmux.cal.CalTransactionFactory;
import com.paypal.jmux.cal.CalTransactionHelper;
import com.paypal.jmux.cal.ClsLogOutputHelper;
import com.paypal.jmux.cal.StackTrace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

@SuppressWarnings("deprecation")
public class OccClientImpl implements OccClient{
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
	
	static final Logger LOGGER = LoggerFactory.getLogger(OccClientImpl.class);
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
	private OccClientConnection conn;
	ArrayList<OccColumnMeta> colMetaData;
	private String sql;
	private long lastStmtId;
	private int byteCount;
	private String serverLogicalName;
	private String calLogFrequency;
	private String occBoxName;

	public OccClientImpl(OccClientConnection _conn, int _connTimeout, boolean _columnNamesEnabled, boolean _columnInfoEnabled) throws OccExceptionBase{
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
	
	private NetStringObj getResponse(String _cmd) throws OccIOException, OccProtocolException {
		try {
			response = is.parse();
		} catch (IOException e) {
			throw new OccIOException(e,getConnectionMetaInfo());
		}
		if (!response.hasNext())
			throw new OccProtocolException("Invalid response for " + _cmd);
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
	
	public void prepare(String _sql) throws OccIOException{
		sendCalCorrId();
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::prepare(" + _sql + ")");
		os.add(OccConstants.OCC_PREPARE_V2, _sql.getBytes());
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

	
	public boolean execute(int _num_rows, boolean _add_commit) throws OccIOException, OccTimeoutException, OccClientException, OccProtocolException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::execute(" + _num_rows + ")");

        CalTransaction execCalTxn = startCalExecTransaction();
        execCalTxn.setStatus("0");

		os.add(OccConstants.OCC_EXECUTE);
	
		// flush the accumulated commands
		try {
	        os.flush();
			NetStringObj obj = read_response();
			if (obj.getCommand() == OccConstants.OCC_VALUE) 
				columns = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			obj = read_response();
			if (obj.getCommand() == OccConstants.OCC_VALUE) 
				rows = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("OCCClient::execQuery() returned cols=" + columns + ",rows=" + rows);

			 
			if (columns > 0) {
				//non-DML(select) like executeQuery 	
				if (columnInfoEnabled) {
					os.add(OccConstants.OCC_COLS_INFO);
					os.flush();
				} else {
					if (columnNamesEnabled) {
						os.add(OccConstants.OCC_COLS);
						os.flush();
					}
				}	
				state = State.FETCH_CMD_SENT;
				os.add(OccConstants.OCC_FETCH, _num_rows);
				os.flush();
				
				colMetaData = iterateColumns();

				return true;
				
			} else {
					//DML(insert, update, delete) like executeUpdate
					if (_add_commit) {
						os.add(OccConstants.OCC_COMMIT);
						os.flush();
						NetStringObj resp = getResponse( "OCC_AUTO_COMMIT");
		                if (resp.getCommand() != OccConstants.OCC_OK) {
		                	OccClientException occEx = new OccClientException("commit: Error " + Integer.toString((int)resp.getCommand()));
		                	handleException(occEx, execCalTxn);
		                    throw occEx;
		                }
					} 
					return false;
			}
			
		} catch (IOException e) {
			OccIOException occEx = new OccIOException(e, getConnectionMetaInfo());
			handleException(occEx, execCalTxn);
			throw occEx;
		} catch (OccTimeoutException | OccClientException | OccProtocolException e) {
			handleException(e, execCalTxn);
			throw e;
		}finally {
			execCalTxn.completed();
		}
	}
		
	public ArrayList<OccColumnMeta> execQuery(int _num_rows, boolean _column_meta) throws OccIOException, OccTimeoutException, OccClientException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::execQuery(" + _num_rows + ")");

		CalTransaction execCalTxn = startCalExecTransaction();
		execCalTxn.setStatus("0");

		os.add(OccConstants.OCC_EXECUTE);
		if (_column_meta) {
			if (columnInfoEnabled)
				os.add(OccConstants.OCC_COLS_INFO);
			else
				if (columnNamesEnabled)
					os.add(OccConstants.OCC_COLS);
		}
		state = State.FETCH_CMD_SENT;
		os.add(OccConstants.OCC_FETCH, _num_rows);
		// flush the accumulated commands
		try {
	        os.flush();
			NetStringObj obj = read_response();
			if (obj.getCommand() == OccConstants.OCC_VALUE) 
				columns = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			obj = read_response();
			if (obj.getCommand() == OccConstants.OCC_VALUE) 
				rows = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("OCCClient::execQuery() returned cols=" + columns + ",rows=" + rows);
			ArrayList<OccColumnMeta> columnMeta = null;
			if (_column_meta && (columnNamesEnabled || columnInfoEnabled)) {
				columnMeta = new ArrayList<OccColumnMeta>(); 
				// column names 
				obj = read_response();
				if (obj.getCommand() == OccConstants.OCC_VALUE) 
					columns = Integer.parseInt(new String(obj.getData(), "UTF-8"));
				else
					check_error(obj);
				for (int i = 0; i < columns; i++) {
					OccColumnMeta meta = new OccColumnMeta();
					// name
					obj = read_response();
					if (obj.getCommand() == OccConstants.OCC_VALUE) {
						meta.setName(new String(obj.getData(), "UTF-8"));
					}
					else
						check_error(obj);
					if (columnInfoEnabled) {
						// type
						obj = read_response();
						if (obj.getCommand() == OccConstants.OCC_VALUE) {
							meta.setType(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						}
						else
							check_error(obj);
						// width
						obj = read_response();
						if (obj.getCommand() == OccConstants.OCC_VALUE) {
							meta.setWidth(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						}
						else
							check_error(obj);
						// precision
						obj = read_response();
						if (obj.getCommand() == OccConstants.OCC_VALUE) {
							meta.setPrecision(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						}
						else
							check_error(obj);
						// scale
						obj = read_response();
						if (obj.getCommand() == OccConstants.OCC_VALUE) {
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
			OccIOException occEx = new OccIOException(e);
			handleException(occEx, execCalTxn);
			throw occEx;
		}catch (OccClientException | OccIOException | OccTimeoutException e) {
			handleException(e, execCalTxn);
			throw e;
		}finally {
			execCalTxn.completed();
		}
	}

	public ArrayList<OccColumnMeta> iterateColumns() throws OccTimeoutException, OccIOException, OccClientException {
		NetStringObj obj;
		ArrayList<OccColumnMeta> columnMeta = null;
		if (columnNamesEnabled || columnInfoEnabled) {
			columnMeta = new ArrayList<OccColumnMeta>(); 
			// column names 
			obj = read_response();
			if (obj.getCommand() == OccConstants.OCC_VALUE)
				try {
					columns = Integer.parseInt(new String(obj.getData(), "UTF-8"));
				} catch (NumberFormatException|UnsupportedEncodingException e) {
					throw new OccClientException("Exception:", e);
				}
			else
				check_error(obj);
			for (int i = 0; i < columns; i++) {
				OccColumnMeta meta = new OccColumnMeta();
				// name
				obj = read_response();
				if (obj.getCommand() == OccConstants.OCC_VALUE) {
					try {
						meta.setName(new String(obj.getData(), "UTF-8"));
					} catch (UnsupportedEncodingException e) {
						throw new OccClientException("Exception:", e);
					}
				}
				else
					check_error(obj);
				if (columnInfoEnabled) {
					// type
					obj = read_response();
					if (obj.getCommand() == OccConstants.OCC_VALUE) {
						try {
							meta.setType(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						} catch (NumberFormatException | UnsupportedEncodingException e) {
							throw new OccClientException("Exception:", e);
						}
					}
					else
						check_error(obj);
					// width
					obj = read_response();
					if (obj.getCommand() == OccConstants.OCC_VALUE) {
						try {
							meta.setWidth(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						} catch (NumberFormatException | UnsupportedEncodingException e) {
							throw new OccClientException("Exception:", e);
						}
					}
					else
						check_error(obj);
					// precision
					obj = read_response();
					if (obj.getCommand() == OccConstants.OCC_VALUE) {
						try {
							meta.setPrecision(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						} catch (NumberFormatException | UnsupportedEncodingException e) {
							throw new OccClientException("Exception:", e);
						}
					}
					else
						check_error(obj);
					// scale
					obj = read_response();
					if (obj.getCommand() == OccConstants.OCC_VALUE) {
						try {
							meta.setScale(Integer.parseInt(new String(obj.getData(), "UTF-8")));
						} catch (NumberFormatException | UnsupportedEncodingException e) {
							throw new OccClientException("Exception:", e);
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
	
	public void execDML(boolean _add_commit) throws SQLException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::execDML(" + _add_commit + ")");

		CalTransaction execCalTxn = startCalExecTransaction();
		execCalTxn.setStatus("0");

		os.add(OccConstants.OCC_EXECUTE);
		if (_add_commit)
			os.add(OccConstants.OCC_COMMIT);
		boolean do_commit = _add_commit;
		// flush the accumulated commands
		try {
			os.flush();
			NetStringObj obj = read_response();
			if (obj.getCommand() == OccConstants.OCC_VALUE) 
				columns = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			obj = read_response();
			if (obj.getCommand() == OccConstants.OCC_VALUE) 
				rows = Integer.parseInt(new String(obj.getData(), "UTF-8"));
			else
				check_error(obj);
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("OCCClient::execDML() returned cols=" + columns + ",rows=" + rows);
		} catch (IOException e) {
			OccIOException occEx = new OccIOException(e);
			handleException(occEx, execCalTxn);
			do_commit = false;
			throw occEx;
		} catch (OccTimeoutException e) {
			handleException(e, execCalTxn);
			do_commit = false;
			throw e;
		} catch (SQLException e) {
			handleException(e, execCalTxn);
			//for SQLException do not make it false. It is used in finally block to consume OCC_COMMIT response
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


	 private void handlecommit(boolean do_commit, CalTransaction execCalTxn) throws OccIOException, OccProtocolException, OccClientException {
		 if (do_commit) {
			 NetStringObj resp = getResponse("OCC_AUTO_COMMIT");
			 if (resp.getCommand() != OccConstants.OCC_OK) {
				 OccClientException occEx = new OccClientException("commit: Error " + Integer.toString((int) resp.getCommand()));
				 handleException(occEx, execCalTxn);
				 execCalTxn.completed();
				 throw occEx;
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
		if (OccJdbcDriverConstants.getInstance().shouldLogInCal(e)) {
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

	public ArrayList<ArrayList<byte[]> > fetch(int _num_rows) throws OccIOException, OccClientException, OccTimeoutException, OccInternalErrorException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::fetch(" + _num_rows + ")");

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
			os.add(OccConstants.OCC_FETCH, _num_rows);
			try {
				os.flush();
				state = State.FETCH_CMD_SENT;
			} catch (IOException e) {
				OccIOException occEx = new OccIOException(e,getConnectionMetaInfo());
				handleException(occEx, fetchCalTxn);
				fetchCalTxn.completed();
				throw occEx;
			}
		}
		ArrayList<ArrayList<byte[]> > result = load_results(Integer.MAX_VALUE);

		fetchCalTxn.addData("bytes", String.valueOf(byteCount) );
		fetchCalTxn.addData("rows", String.valueOf(result.size()) );
		fetchCalTxn.setStatus("0");
		fetchCalTxn.completed();
		
		return result;
	}
	
	public void bind(String _variable, BindType _type, byte[] _value) throws OccIOException, OccSQLException{
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::bind()");
		try {
			os.add(OccConstants.OCC_BIND_NAME, OccJdbcConverter.string2occ(_variable));
		} catch (UnsupportedEncodingException e) {
			throw new OccSQLException("can't encode in variable name", e);
		}
		if (_type != BindType.OCC_TYPE_STRING)
			os.add(OccConstants.OCC_BIND_TYPE, _type.getValue());
		os.add(OccConstants.OCC_BIND_VALUE, _value);
	}

	public void bindOut(String _variable) throws OccIOException, OccSQLException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::bind_out()");
		try {
			os.add(OccConstants.OCC_BIND_OUT_NAME, OccJdbcConverter.string2occ(_variable));
		} catch (UnsupportedEncodingException e) {
			throw new OccSQLException("can't encode out variable name", e);
		}
	}
	
	@Override
	public void bindArray(String _variable, int _max_sz, BindType _type, ArrayList<byte[]> _values) throws OccIOException, OccSQLException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::bindArray()");
		try {
			os.add(OccConstants.OCC_BIND_NAME, OccJdbcConverter.string2occ(_variable));
		} catch (UnsupportedEncodingException e) {
			throw new OccSQLException("can't encode in variable name", e);
		}
		if (_type != BindType.OCC_TYPE_STRING)
			os.add(OccConstants.OCC_BIND_TYPE, _type.getValue());
		os.add(OccConstants.OCC_ARRAY_LENGTH, OccJdbcConverter.int2occ(_values.size()));
		os.add(OccConstants.OCC_ARRAY_MAX_VALUESZ, OccJdbcConverter.int2occ(_max_sz));
		for (int i = 0; i < _values.size(); i++) {
			os.add(OccConstants.OCC_BIND_VALUE, _values.get(i));
		}
	}

	public ArrayList<ArrayList<byte[]> > fetchOutBindVars(int _bind_var_count) throws OccTimeoutException, OccIOException, OccClientException, OccInternalErrorException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::fetch_out_bind_vars()");
		NetStringObj obj = read_response();
		if (obj.getCommand() != OccConstants.OCC_VALUE)
			check_error(obj);
		try {
			rows = Integer.parseInt(new String(obj.getData(), "UTF-8"));
		} catch (NumberFormatException | UnsupportedEncodingException e) {
			throw new OccClientException("Exception:", e);
		}
		columns = _bind_var_count;
		return load_results(rows);
	}	
	
	public ArrayList<ArrayList<byte[]> > load_results(int _rows) throws OccClientException, OccTimeoutException, OccIOException, OccInternalErrorException {
		ArrayList<ArrayList<byte[]> > ret = new ArrayList<ArrayList<byte[]> >();
		byteCount = 0;
		for (int i = 0; i < _rows; i++) {
			ArrayList<byte[]> row = new ArrayList<byte[]>();
			if (columns <= 0)
				throw new OccInternalErrorException("For the query '" + sql + "' the number of column is incorrect: " + columns);
			for (int j = 0; j < columns; j++) {
				NetStringObj obj = read_response();
				if (obj.getCommand() == OccConstants.OCC_NO_MORE_DATA) {
					state = State.FETCH_DONE;
					return ret;
				}
				if (obj.getCommand() == OccConstants.OCC_OK) {
					state = State.FETCH_CMD_NEEDED;
					return ret;
				}
				if (obj.getCommand() == OccConstants.OCC_VALUE) {
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

	public void commit() throws OccClientException, OccIOException, OccProtocolException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::commit()");
		os.add(OccConstants.OCC_COMMIT);
		try {
			os.flush();
			NetStringObj resp = getResponse("OCC_COMMIT");
			if (resp.getCommand() != OccConstants.OCC_OK)
				throw new OccClientException("commit: Error " + Integer.toString((int)resp.getCommand()));
		} catch (IOException e) {
			throw new OccIOException(e,getConnectionMetaInfo());
		}
	}

	public void rollback() throws OccIOException, OccProtocolException, OccClientException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::rollback()");
		os.add(OccConstants.OCC_ROLLBACK);
		try {
			os.flush();
			NetStringObj resp = getResponse("OCC_ROLLBACK");
			if (resp.getCommand() != OccConstants.OCC_OK)
				throw new OccClientException("rollback: Error " + Integer.toString((int)resp.getCommand()));
		} catch (IOException e) {
			throw new OccIOException(e,getConnectionMetaInfo());
		}
	}
	
	private void check_error(NetStringObj obj) throws OccClientException {
		String errorMessage = null;
		try {
			errorMessage = new String(obj.getData(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new OccClientException("Exception:", e);
		}
		Pair<String, Integer> errInfo = OccJdbcUtil.ErrorToSqlStateAndVendorCodeConverter(errorMessage);
			switch ((int)obj.getCommand()) {
			case OccConstants.OCC_SQL_ERROR:
				throw new OccClientException(Consts.OCC_SQL_ERROR_PREFIX + errorMessage, errInfo.getFirst(), errInfo.getSecond());
			case OccConstants.OCC_ERROR:
				throw new OccClientException("OCC error: " + errorMessage + getConnectionMetaInfo().toString() , errInfo.getFirst());
			case OccConstants.OCC_MARKDOWN:
				throw new OccClientException("OCC markdown: " + errorMessage, errInfo.getFirst());
			default:
				throw new OccClientException("Unknown error: cmd=" + obj.getCommand() + ", data=" + errorMessage);
			}
	}
	
	private NetStringObj read_response() throws OccTimeoutException, OccIOException {
		try {
			long start = System.currentTimeMillis();
			while (true) {
				if ((response == null) || (!response.hasNext()))
					response =is.parse();
				NetStringObj obj = response.next();
				if (obj.getCommand() == OccConstants.OCC_STILL_EXECUTING) {
					if (LOGGER.isInfoEnabled())
						LOGGER.info("Still executing ...");
					long now = System.currentTimeMillis();
					if (now - start > connTimeout)
						throw new OccTimeoutException("Timeout waiting for response");
				} else {
					return obj;
				}
			}
		} catch (IOException e) {

			throw new OccIOException(e,getConnectionMetaInfo());
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
	public void sendCalCorrId() throws OccIOException
	{
		String buffer;
		buffer = "CorrId=" + getCorrelationId();
		CalPoolStackInfo stackInfo = CalPoolStackInfo.getCalPoolStackInfo();
		if (stackInfo != null)
			buffer += "&PoolStack: " + stackInfo.getPoolStack();
		os.add(OccConstants.CLIENT_CAL_CORRELATION_ID, buffer.getBytes());
	}
	
	@Override
	public String sendClientInfo(String info, String name)
			throws OccExceptionBase {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::sendClientInfo()");
		String buffer;
		if (name.isEmpty())
			buffer  = "PID: " + clientInfo.pid + ",HOST: " + clientInfo.hostName + ", EXEC: " + clientInfo.cmdLine + 
				", Poolname: " + clientInfo.poolName + ", Command: " + info + ", " + clientInfo.poolStack + ", Name: " + name;
		else
			buffer  = "PID: " + clientInfo.pid + ",HOST: " + clientInfo.hostName + ", EXEC: " + clientInfo.cmdLine + 
				", Poolname: " + clientInfo.poolName + ", Command: " + info + ", " + clientInfo.poolStack;
		os.add(OccConstants.OCC_CLIENT_INFO, buffer.getBytes());
		try {
			os.flush();
			NetStringObj resp = getResponse("OCC_CLIENT_INFO");
			if (resp.getCommand() != OccConstants.OCC_OK)
				throw new OccClientException("OCC_CLIENT_INFO: Error " + Integer.toString((int)resp.getCommand()));
			return new String(resp.getData(), "UTF-8");
		} catch (IOException e) {
			throw new OccIOException(e,getConnectionMetaInfo());
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
	public void close() throws OccIOException {
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
	public ArrayList<OccColumnMeta> getColumnMeta() throws OccIOException {
		return colMetaData;		
	}

	@Override
	public void shardKey(byte[] _data) throws OccIOException {
		os.add(OccConstants.OCC_SHARD_KEY, _data);
	}

	@Override
	public int getNumShards() throws OccIOException, OccProtocolException, OccClientException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClientImpl::getNumShards()");
		os.add(OccConstants.OCC_GET_NUM_SHARDS);
		try {
			os.flush();
			NetStringObj resp = getResponse("OCC_GET_NUM_SHARDS");
			if (resp.getCommand() != OccConstants.OCC_OK)
				throw new OccClientException("commit: Error " + Integer.toString((int)resp.getCommand()));
			return Integer.parseInt(new String(resp.getData(), "UTF-8"));
		} catch (IOException e) {
			throw new OccIOException(e,getConnectionMetaInfo());
		}
	}

	@Override
	public void setShard(int _shard_id) throws OccIOException, OccProtocolException, OccClientException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClientImpl::setShard()");
		os.add(OccConstants.OCC_SET_SHARD_ID, _shard_id);
		try {
			os.flush();
			NetStringObj resp = getResponse("OCC_SET_SHARD_ID");
			if (resp.getCommand() != OccConstants.OCC_OK)
				throw new OccClientException("setShard(" + _shard_id + ") error code: " + Integer.toString((int)resp.getCommand()) + ": " + new String(resp.getData(), "UTF-8"));
		} catch (IOException e) {
			throw new OccIOException(e,getConnectionMetaInfo());
		}
	}

	@Override
	public void ping() throws OccExceptionBase {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("OCCClient::sendPing()");
		os.add(OccConstants.SERVER_PING_COMMAND, "".getBytes());
		try {
			os.flush();
			NetStringObj resp = getResponse("OCC_CLIENT_PING");
			if (resp.getCommand() != OccConstants.SERVER_ALIVE){
				throw new OccClientException("OCCClient::sendPing(): Error " + resp.getCommand());
			}
		} catch (IOException e) {
			throw new OccIOException(e,getConnectionMetaInfo());
		}
	}

	@Override
	public void setOccBoxName(String occBoxName){
		this.occBoxName = occBoxName;
	}


	ConnectionMetaInfo getConnectionMetaInfo(){
		ConnectionMetaInfo connectionMetaInfo = new ConnectionMetaInfo();
		connectionMetaInfo.setServerBoxName(occBoxName);
		return connectionMetaInfo;
	}

}
