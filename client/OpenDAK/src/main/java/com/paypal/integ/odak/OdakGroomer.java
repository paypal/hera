package com.paypal.integ.odak;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.cal.CalEventHelper;

/**
 * 
 * Background connection pool maintenance.
 * 
 */
public class OdakGroomer implements Runnable {
	private final static OdakGroomer INSTANCE = new OdakGroomer();
	private final static Logger logger = LoggerFactory.getLogger(OdakGroomer.class);

	/*
	 * Cached thread pool performed well for OCP's short-lived asynchronous
	 * tasks.
	 */
	// private static ThreadPoolExecutor executor = (ThreadPoolExecutor)
	// Executors.newFixedThreadPool(GroomerConfig.getInstance().getBkgExecutorPoolSize());
	private static ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

	private Map<String, OdakPool> pools = new ConcurrentHashMap<String, OdakPool>();
	private int numRestarts;

	private OdakGroomer() {
	}

	public static OdakGroomer getInstance() {
		return INSTANCE;
	}

	@Override
	public void run() {
		String msg = String.format("OCP Groomer started. Thread Id: %s", Thread.currentThread().getId());
		logger.info("ODAK_INIT - " + msg);
		CalEventHelper.writeLog("ODAK_INIT", "OCPGroomer", msg, "0");

		try {
			while (true) {
				groomConnections();
				Utils.sleep(GroomerConfig.getInstance().getGroomInterval());
			}
		} catch (Throwable t) {
			msg = String.format("Groomer thread [%d] is about to die with an error: %s.",
					Thread.currentThread().getId(), t);
			logger.error(msg, t);
			CalEventHelper.writeException("ODAK_GROOMER_DIED", t, true, msg);
			forceRetry();
		}
	}

	private void forceRetry() {
		if (numRestarts > GroomerConfig.getInstance().getBkgThreadRestartAttempts()) {
			String msg = String.format(
					"Background Groomer thread - [%d] - cannot be restarted. Exceeds no of max restarts: %d. Total restarts so far: %d",
					Thread.currentThread().getId(), GroomerConfig.getInstance().getBkgThreadRestartAttempts(),
					numRestarts);
			logger.error("ODAK_GROOMER_EXCEED_MAX_RESTARTS - {}", msg);
			CalEventHelper.writeLog("ODAK_GROOMER_EXCEED_MAX_RESTARTS", "ALLHOSTS", msg, "1");
			return;
		}
		numRestarts++;
		Thread useTracker = new Thread(OdakGroomer.getInstance());
		useTracker.setDaemon(true);
		useTracker.start();
		String msg = String.format("Groomer background thread retstarted successfully. RetryCount= %d", numRestarts);
		CalEventHelper.writeLog("ODAK_GROOMER_RESTARTED", "ALLHOSTS", msg, "0");
	}

	/**
	 * 
	 * Removes idle and orphan connections. Submits request to create new if
	 * needed. ConcurrentLinkedQueue Memory leak is fixed -
	 * https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8137185
	 */
	private void groomConnections() {
		for (Map.Entry<String, OdakPool> entry : pools.entrySet()) {
			OdakPool pool = entry.getValue();
			pool.removeIdleConnection();
			pool.removeOrphanConnections();
		}
	}

	/**
	 * 
	 * Trade-off is taken to avoid locking on active/free lists which is heavily
	 * used by user requests and gives measurable latency impact during
	 * connection checkout. Chance of race exists while checking if pool is
	 * already at it's size, and can end up one or two more conns for until next
	 * recycle. Chance is extremely rare and never seen any extra connection
	 * under various load tests.
	 * 
	 * 
	 * @param pool
	 * @return
	 */
	boolean addConnectionRequest(OdakPool pool, boolean isRecycled) {
		try {
			// heavy foreground traffic may end up creating conns before we
			// submit background request. Soft/Hard recycle with randomization
			// and higher extra conn capacity helps to minimize foreground conn
			// creation.
			if (pool.getCurrentConnsCount() >= pool.getSize()) {
				String msg = String.format(
						"Background OCP connection request won't be honored. Pool is already at the ideal size. Pool:%s, isRecycled:%b, "
								+ "PoolCurrentSize:%d, PoolIdealSize: %d, state:%s",
						pool.getName(), isRecycled, pool.getCurrentConnsCount(), pool.getSize(),
						StateLogger.getInstance().getState(pool.getName()));
				logger.info("ODAK_BKG_CONNECT_REQUEST_IGNORED - {}", msg);
				CalEventHelper.writeLog("ODAK_BKG_CONNECT_REQUEST_IGNORED", pool.getName(), msg, "0");
				return false;
			}

			if (executor.getQueue().size() > GroomerConfig.getInstance().getMaxExecutorQueuelength()) {
				String msg = String.format(
						"Reached %d pending background connection requests for pool %s. Rejecting this bkg connection request. state:%s",
						GroomerConfig.getInstance().getMaxExecutorQueuelength(), pool.getName(),
						StateLogger.getInstance().getState(pool.getName()));
				logger.info("ODAK_BKG_CONN_CREATION_SLOWDOWN - {}", msg);
				CalEventHelper.writeLog("ODAK_BKG_CONN_CREATION_SLOWDOWN", pool.getName(), msg, "0");
				return false;
			}
			String msg = String.format(
					"pool=%s&isRecycled=%b&"
							+ "poolCurrentSize=%d&poolIdealSize=%d&msg=Adding background OCP connection request",
					pool.getName(), isRecycled, pool.getCurrentConnsCount(), pool.getSize());
			logger.info("ODAK_BKG_CONNECT_REQUEST - {}", msg);
			CalEventHelper.writeLog("ODAK_BKG_CONNECT_REQUEST", pool.getName(), msg, "0");

			if (!isRecycled) {
				pool.incrPoolResizeBkgConnsReqs();
			}
			pool.incrBkgConnsReqs();
			OCPConnectTask connectTask = new OCPConnectTask(pool);
			executor.execute(connectTask);
			return true;
		} catch (Throwable t) {
			// Log, but not fail the thread.
			String errMsg = String.format("OCPConnection request submission failed for data source: %s",
					pool.getName());
			logger.error(errMsg, t);
			CalEventHelper.writeException("ODAK_BKG_CONNECT_REQUEST_FAILED", t, true, errMsg);
		}
		return false;
	}

	public void register(OdakPool pool) {
		pools.put(pool.getName(), pool);
		logger.info("Registered pool {} with PoolGroommer", pool.getName());
	}

	public void unregister(OdakPool pool) {
		pools.remove(pool.getName());
	}

	private static class OCPConnectTask implements Runnable {
		private final OdakPool pool;

		OCPConnectTask(OdakPool pool) {
			this.pool = pool;
		}

		public void run() {
			try {
				pool.processConnectRequest();
			} catch (SQLException | NamingException e) {
				String errMsg = String.format("Groomer - background connection creation failed for the data source: %s",
						pool.getName());
				logger.error("ODAK_GRM_TASK_CONNECT_FAILED - " + errMsg, e);
				CalEventHelper.writeException("ODAK_GRM_TASK_CONNECT_FAILED", e, true, errMsg);
				// Don't retry here as new connection creation is always
				// retried, as well as groomer will submit new requests after
				// groom time.
			}
		}
	}
}
