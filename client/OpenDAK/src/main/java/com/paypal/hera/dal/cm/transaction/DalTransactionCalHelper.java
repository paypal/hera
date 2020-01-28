package com.paypal.hera.dal.cm.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ebay.kernel.calwrapper.CalTransaction;
import com.ebay.kernel.calwrapper.CalTransactionFactory;
import com.ebay.kernel.logger.LogLevel;
import com.ebay.kernel.logger.Logger;

/**
 * 
 * Description: 	DalTransaction Helper for CAL logging
 *
 */
public class DalTransactionCalHelper
{
	static private final ConcurrentMap<DalTransaction, CalTransaction> 
		s_trans = new ConcurrentHashMap<DalTransaction, CalTransaction>();
	
	private static Logger s_logger =  Logger.getInstance(DalTransactionCalHelper.class);;
	private static void logError(String msg) {
		s_logger.log(LogLevel.ERROR, msg);
	}
	
	static void logCALTransaction(DalTransaction dalTrans, 
			String calTransName, String status, CharSequence data)
	{
		verifyThread(dalTrans);
		CalTransaction calTrans = s_trans.get(dalTrans);
		if (calTrans == null) {
			calTrans = CalTransactionFactory.create("DalTransaction");
		}
		calTrans.setName(calTransName);
		calTrans.setStatus(status);
		calTrans.addData(data);	
		s_trans.put(dalTrans, calTrans);
	}
	
	// complete CAL transaction
	static void endCALTransaction(DalTransaction dalTrans)
	{
		verifyThread(dalTrans);
		CalTransaction calTrans = s_trans.get(dalTrans);
		if (calTrans != null) {
			calTrans.completed();
		}
		s_trans.remove(dalTrans);
	}
	
	// CalTransaction is not safe-thread, so don't touch it if the thread is different
	private static synchronized void verifyThread(DalTransaction dalTrans)
	{
		if (!s_trans.containsKey(dalTrans)) {
			return;
		}
		
		// verify that it's the same thread, log error otherwise and release CalTransaction
		Long curThreadId = Long.valueOf(Thread.currentThread().getId());
		if (null == DalTransactionManagerImpl.getInstance().getDalTransaction(curThreadId)) {
			// DALTransaction was created by a different thread than this
			logError("Attempt to access CalTransaction from a different thread,"
				+ " possibly because DalTransaction was focefully terminated!");
			// remove this CalTransaction to prevent its usage from different thread:
			s_trans.remove(dalTrans);
		}
	}
	
}
