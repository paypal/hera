package com.paypal.integ.odak;

import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import javax.naming.NamingException;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kernel.cal.CalClientConfigFactory;
import com.ebay.kernel.cal.CalServiceFactory;
import com.ebay.kernel.cal.java.file.FileCalService;
import com.ebay.kernel.cal.mxbean.CalClientConfigMXBean;
import com.paypal.dal.callable.EmployeeDO;
import com.paypal.dal.ocp.test.DataManager;
import com.paypal.hera.dal.DalInit;
import com.paypal.hera.dal.dao.CreateException;
import com.paypal.hera.dal.dao.FinderException;
import com.paypal.hera.dal.dao.RemoveException;
import com.paypal.hera.dal.map.ConnectionManager;
import com.paypal.hera.dal.map.SQLOperationEnum;
import com.paypal.integ.odak.OdakPoolManager;
import com.paypal.platform.security.PayPalSSLHelper;

/**
 * During this test,pool should not be registered with pool maintainer or keep
 * the idle/orphan/etc timeouts very high. Otherwise, conn will be removed.
 * 
 *
 */
public class OCPStressTest {
	
	public static void init() throws Exception {
		System.out.println("===> init");
		initCAL();
		PayPalSSLHelper.initializeSecurity();
		DalInit.init();
	}

	public static void initCAL() throws Exception {
		URL urlCal = OCPStressTest.class.getResource("/config/calconfiguration.properties");
		URI uri = new File(".").getAbsoluteFile().toURI();
		String urlCalStr = URLDecoder.decode(urlCal.getPath(), "UTF-8");
		Properties pcal = new Properties();
		pcal.load(new FileReader(urlCalStr));
		CalClientConfigMXBean cccb = CalClientConfigFactory.create(urlCal);
		FileCalService fcs = new FileCalService(cccb);
		com.ebay.kernel.cal.CalServiceFactory.setCalService(fcs);
	}

	public static void cleanSetup() throws Exception {
		CalServiceFactory.reset();
	}

	private static void doWork() {
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static void doExtraWork() {
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static void testSelfAdjustablePoolWithExecutor() {
		// CountDownLatch latch = new CountDownLatch(20);
		// ThreadPoolExecutor executor = (ThreadPoolExecutor)
		// Executors.newFixedThreadPool(20);
		// for (int i = 0; i < 20; i++) {
		// executor.execute(new DBTask(latch, 700, 50));
		// }
		// try {
		// System.out.println("waiting for threads-batch-1 to finish");
		// latch.await();
		// System.out.println("Threads-batch-1 finished");
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// execTasks(5, 200, 300);
		// execTasks(10, 200, 300);
		// execTasks(20, 200, 300);
		execTasks(5, 200, 3000000);
		execTasks(20, 200, 100);
		// execTasks(10, 200, 100);
		execTasks(5, 200, 100);
		// execTasks(40, 200, 300);
		// execTasks(10, 200, 300);

		// execTasks(40, 200, 3000);
	}

	private static void testSelfAdjustablePool() {

		for (int i = 0; i < 20; i++) {
			(new Thread() {
				public void run() {
					for (int i = 0; i < 50; i++) {
						try {
							Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", false,
									"xe", SQLOperationEnum.SQL_OP_SELECT, null, 0L, 0L);
							Thread.sleep(700);
							conn.close();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (SQLException e) {
							e.printStackTrace();
						} catch (NamingException e) {
							e.printStackTrace();
						}
					}
				}
			}).start();
		}

		for (int i = 0; i < 10; i++) {
			(new Thread() {

				public void run() {
					for (int i = 0; i < 50; i++) {
						try {
							Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", false,
									"xe", SQLOperationEnum.SQL_OP_SELECT, null, 0L, 0L);
							Thread.sleep(500);
							conn.close();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (SQLException e) {
							e.printStackTrace();
						} catch (NamingException e) {
							e.printStackTrace();
						}
					}
				}
			}).start();
		}

		for (

		int i = 0; i < 10; i++)

		{
			(new Thread() {
				public void run() {
					for (int i = 0; i < 100; i++) {
						try {
							Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", false,
									"xe", SQLOperationEnum.SQL_OP_SELECT, null, 0L, 0L);
							Thread.sleep(1);
							conn.close();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (NamingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}).start();
		}

	}
	
	private static void createData() throws SQLException, NamingException{
		OCPTests tests = new OCPTests();
		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		conn.setAutoCommit(true);
		tests.deleteEmp(conn, 59);
		tests.createEmp(conn, 59, "John", "Engineer", 10000);
	}

	
	private static void runDCPStress() {
		PerfCounter counters = new PerfCounter();
		int threadCount = 8;
		
		CountDownLatch latch = new CountDownLatch(threadCount);
		long startTime = System.nanoTime();
		DcpPoolStress dcpPoolStress = new DcpPoolStress(latch, counters);
//		try {
//			createData();
//		} catch (SQLException | NamingException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
		for (int i = 0; i < threadCount; i++) {
			Thread dcpThread = new Thread(dcpPoolStress);
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			dcpThread.start();
		}
	
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		long endTime = System.nanoTime();
		long timeTaken = (endTime - startTime) / 1000000000;
		System.out.println("Time taken in sec - " + timeTaken);
		System.out.println("requests - " + counters.reqCount);
		System.out.println("Tx - " +  counters.reqCount.get() / timeTaken);
		counters.print99Percentile();
	}

	private static void runOCPStress() {
		OccPoolStress occPoolStress = new OccPoolStress();
		for (int i = 0; i < 100; i++) {
			Thread occThread = new Thread(occPoolStress);
			occThread.start();
		}
	}

	public static void main(String[] args) throws Exception {
		 init();
		// testSelfAdjustablePool();
		testSelfAdjustablePoolWithExecutor();
		//runDCPStress(); //--- latency test and OCP comparision
	}

	private static void execTasks(int threads, long sleepTime, int iterations) {
		// try {
		// CountDownLatch latch = new CountDownLatch(threads);
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads);
		for (int i = 0; i < threads; i++) {
			// Thread.sleep(1500); //to add randomness in aging
			executor.execute(new DBTask(null, sleepTime, iterations));
		}
		// latch.await();
		try {
			// cleanSetup();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
	}

	private static class DBTask implements Runnable {
		private CountDownLatch latch;
		private long sleepTime;
		private int iteration;

		DBTask(CountDownLatch latch, long sleepTime, int iteration) {
			this.latch = latch;
			this.sleepTime = sleepTime;
			this.iteration = iteration;
		}

		public void run() {
			for (int i = 0; i < iteration; i++) {
				try {
					Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", false, "xe",
							SQLOperationEnum.SQL_OP_SELECT, null, 0L, 0L);
					Thread.sleep(sleepTime);
					conn.close();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (SQLException e) {
					e.printStackTrace();
				} catch (NamingException e) {
					e.printStackTrace();
				}
			}
			// latch.countDown();
		}
	}

}

class DcpPoolStress implements Runnable {
	private final static Logger logger = LoggerFactory.getLogger(DcpPoolStress.class);
	private CountDownLatch latch;
	PerfCounter counters;
	
	public DcpPoolStress(CountDownLatch latch, PerfCounter counters) {
		this.latch = latch;
		this.counters = counters;
	}
	
//	private void execQuery() throws SQLException, NamingException{
//		OCPTests tests = new OCPTests();
//		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
//				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
//		conn.setAutoCommit(true); // in-transaction behavior
//		EmpDO empDo = tests.readEmp(conn, 59);
//	}
	
	public void execCrud() {
		DataManager dm = new DataManager();
		try {
			CalServiceFactory.reset();
			int threadId = (int)Thread.currentThread().getId();
			dm.deleteEmployee(threadId);
			dm.createEmployee(threadId, "Emp1", "job1", 50000, 30);
			EmployeeDO emp = dm.readEmployee(threadId);
			Assert.assertEquals(emp.getEmpno(), threadId);
			dm.deleteEmployee(threadId);
		} catch (CreateException | FinderException e) {
			e.printStackTrace();
			Assert.fail();
		} catch (RemoveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

	
	@Override
	public void run() {
		//Queue<Long> data = new ConcurrentLinkedQueue<Long>();
//		try {
			//20000000
			for (int i = 0; i < 2000000000; i++) {	
				long startTime = System.nanoTime();
				//DCP
//				Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", false, "xe",
//						SQLOperationEnum.SQL_OP_SELECT, null, 0L, 0L);
//				Thread.sleep(1);
//				conn.close();
				
				//OCP
//				OCPPoolableConnection conn = OCPConnectionPoolManager.getInstance().getPool("xe").getPooledConnection();
//				Thread.sleep(5);
//				conn.close();
				
				execCrud();
				long endTime = System.nanoTime();
				long timeTaken = endTime - startTime;
				// data.add(new Long(1));
				// data.poll();
				counters.reqCount.incrementAndGet();
				//Concurrent but not using lock. It may happen some will overwrite other, but ok to loose some data.
				if(i % 10 == 0){
					counters.latencies.add(timeTaken);
				}
				try {
					CalServiceFactory.reset();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}			
//		} 
//		catch (SQLException e) {
//			e.printStackTrace();
//		} 
//		catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		catch (NamingException e) {
//			e.printStackTrace();
//		}
		latch.countDown();
	}	
}

class OccPoolStress implements Runnable {
	@Override
	public void run() {
		for (int i = 0; i < 20000; i++) {
			try {
				long startTime = System.nanoTime();
				Connection conn = OdakPoolManager.getInstance().getPool("xe").getPooledConnection();
				conn.close();
				long endTime = System.nanoTime();
				long timeTaken = endTime - startTime;
				// System.out.println("OCP Pool TimeTaken in nano - " +
				// timeTaken);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
