package com.paypal.dal.ocp.test;

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

import com.ebay.kernel.cal.CalClientConfigFactory;
import com.ebay.kernel.cal.CalServiceFactory;
import com.ebay.kernel.cal.java.file.FileCalService;
import com.ebay.kernel.cal.mxbean.CalClientConfigMXBean;
import com.paypal.hera.dal.DalInit;
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
public class OccConnectionPoolTest {

	public static void init() throws Exception{
		System.out.println("===> init");
		PayPalSSLHelper.initializeSecurity();
		DalInit.init();
		initCAL();
		
	}
	
	public static void  initCAL() throws Exception {
		URL urlCal = OccConnectionPoolTest.class.getResource("/config/calconfiguration.properties");
		URI uri = new File(".").getAbsoluteFile().toURI();
		String urlCalStr = URLDecoder.decode(urlCal.getPath(), "UTF-8");
		Properties pcal = new Properties();
		pcal.load(new FileReader(urlCalStr));
		CalClientConfigMXBean cccb = CalClientConfigFactory.create(urlCal);
		FileCalService fcs = new FileCalService(cccb);
		com.ebay.kernel.cal.CalServiceFactory.setCalService(fcs);
	}
	
	public static  void cleanSetup() throws Exception{
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
		// execTasks(20, 200, 100);
		// execTasks(10, 200, 100);
		// execTasks(5, 200, 100);
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

		for (int i = 0; i < 10; i++) {
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

	private static void runDCPStress() {
		DcpPoolStress dcpPoolStress = new DcpPoolStress();
		for (int i = 0; i < 2; i++) {
			Thread dcpThread = new Thread(dcpPoolStress);
			dcpThread.start();
		}
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
		//testSelfAdjustablePoolWithExecutor();  --- WORKS
		runDCPStress();
	}
	
	private static void execTasks(int threads, long sleepTime, int iterations) {
//		try {
			//CountDownLatch latch = new CountDownLatch(threads);
			ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads);
			for (int i = 0; i < threads; i++) {
				// Thread.sleep(1500); //to add randomness in aging
				executor.execute(new DBTask(null, sleepTime, iterations));
			}
			//latch.await();
			try {
				//cleanSetup();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
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
//				try {
//					Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", false, "xe",
//							SQLOperationEnum.SQL_OP_SELECT, null, 0L, 0L);
					//Thread.sleep(sleepTime);
//					conn.close();
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				} catch (NamingException e) {
//					e.printStackTrace();
//				}
			}
			//latch.countDown();
		}
	}

}

// class OcpAdjustablePoolStress implements Runnable {
// @Override
// public void run() {
// for (int i = 0; i < 20000; i++) {
// try {
// long startTime = System.nanoTime();
// // OccPoolableConnection conn =
// ConnectionPoolManager.getInstance().getPool("xe").getPooledConnection(true,
// "sql", true, "xe",
// SQLOperationEnum.SQL_OP_SELECT, null, 0L, 0L);
// // conn.close();
// Connection conn = ConnectionManager.getInstance().getConnection("xe", true,
// "sql", false, "xe",
// SQLOperationEnum.SQL_OP_SELECT, null, 0L, 0L);
// conn.close();
// long endTime = System.nanoTime();
// long timeTaken = endTime - startTime;
// // System.out.println("DCP Pool TimeTaken in mirco - " +
// // timeTaken/1000);
// } catch (SQLException | NamingException e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// }
// }
// }
// }

class DcpPoolStress implements Runnable {
	@Override
	public void run() {
		int sum = 0;
		for (int i = 0; i < 200000000; i++) {
			try {
				long startTime = System.nanoTime();
				// OccPoolableConnection conn =
				// ConnectionPoolManager.getInstance().getPool("xe").getPooledConnection(true,
				// "sql", true, "xe", SQLOperationEnum.SQL_OP_SELECT, null, 0L,
				// 0L);
				// conn.close();
				
				
//				Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", false, "xe",
//						SQLOperationEnum.SQL_OP_SELECT, null, 0L, 0L);
//				conn.close();
				//System.out.println("test");
				sum = sum + 1;
				sum = sum + 1 -1 + 1 + 1;
				sum = sum * 2;
				long endTime = System.nanoTime();
				long timeTaken = endTime - startTime;
				// System.out.println("DCP Pool TimeTaken in mirco - " +
				// timeTaken/1000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("sum is" + sum);
	}
}

class OccPoolStress implements Runnable {
	@Override
	public void run() {
		for (int i = 0; i < 20000; i++) {
			// call create and return connection
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
