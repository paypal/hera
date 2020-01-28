package com.paypal.integ.odak;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.cal.CalEventHelper;
import com.paypal.hera.dal.cm.wrapper.CmConnectionCallback;
import com.paypal.hera.dal.jdbc.rt.JdbcDriverAdapter;
import com.paypal.hera.dal.jdbc.rt.JdbcDriverAdapterFactory;
import com.paypal.integ.odak.exp.InitializationException;

/**
 * 
 * Manages all OCP pools.
 *
 */
public class OdakPoolManager {
	private final static OdakPoolManager INSTANCE = new OdakPoolManager();
	private final static Logger logger = LoggerFactory.getLogger(OdakPoolManager.class);

	private ConcurrentHashMap<String, OdakPool> pools = new ConcurrentHashMap<String, OdakPool>();

	private OdakPoolManager() {
	}

	public static OdakPoolManager getInstance() {
		return INSTANCE;
	}

	public boolean isOCPConnectionPool(String host) throws NamingException {
		return OdakConfigManager.getInstance().doesExist(host);
	}

	/**
	 * 
	 * Create OCP pool for a given host and cache it. Multiple threads
	 * concurrently can enter this method during the lazy init, which is
	 * currently not in-use.
	 * 
	 * @param host
	 * @return
	 * @throws NamingException
	 */
	public OdakPool createPool(String host, CmConnectionCallback cmProxyCallbakUplink) throws NamingException {
		//ConnectionPoolConfig existingConfig = ConnectionPoolFactory.getInstance().getPoolConfig(host);
		//JdbcDriverAdapter jdbcdriverAdapter = JdbcDriverAdapterFactory.getAdapter(existingConfig);
		PoolConfig ocpConfig = OdakConfigManager.getInstance().getPoolConfig(host);
		JdbcDriverAdapter jdbcdriverAdapter = JdbcDriverAdapterFactory.getAdapter(host, ocpConfig.getUrl());
		OdakPool pool = new OdakPool(host, ocpConfig, jdbcdriverAdapter, cmProxyCallbakUplink);
		OdakPool existingPool = pools.putIfAbsent(host, pool);
		if (existingPool != null) {
			// other thread has already initialized.
			return existingPool;
		}
		OdakGroomer.getInstance().register(pool);
		OdakSizeAdjuster.getInstance().register(pool);
		StateLogger.getInstance().register(pool);
		return pool;
	}

	public OdakPool getPool(String host) throws InitializationException {
		//TODO:HERA
		// host = LogicalToPhysicalDatasourceMap.normalize(host);
		OdakPool pool = pools.get(host);
		if (pool == null) {
			throw new InitializationException("OCP pool is not initialized.");
		}
		return pool;
	}

	/**
	 * Initializes OCP pool, if present. DALMaintainer registration for OCP
	 * pools will be skipped as part of DcpConnectionPool.create
	 * 
	 * @throws InitializationException
	 */
	public void init() throws InitializationException {
		OdakConfigManager.getInstance().loadConfig();
		if (!OdakConfigManager.getInstance().isOcpPoolConfigured()) {
			String msg = String.format("No OCP pool detected in the dsimport.xml");
			logger.info("ODAK_INIT - {}", msg);
			CalEventHelper.writeLog("ODAK_INIT", "NO_OCP_POOL_DETECTED", msg, "0");
			return;
		}
		Set<String> ocpPoolNames = OdakConfigManager.getInstance().getOcpPoolNames();

		for (String host : ocpPoolNames) {
			try {
				String msg = String.format("OCP pool found: %s. Initializing.", host);
				logger.info("ODAK_INIT - {}", msg);
				CalEventHelper.writeLog("ODAK_INIT", host, msg, "0");
				// TODO:HERA
				// DataSourceStateManager.getInstance().getScorecard(host).setAutoMarkdown(false);
				OdakAdapter ocpAdapter = new OdakAdapter();
				// ConnectionPoolConfig config = ConnectionPoolFactory.getInstance().getPoolConfig(host);
				PoolConfig ocpConfig = OdakConfigManager.getInstance().getPoolConfig(host);
				ocpAdapter.setOCPConfig(ocpConfig);
				// ocpadapter.setInitialConfig(config);
				ocpAdapter.startup();
				//TODO:HERA
				//ConnectionManager.getInstance().registerCustomPoolAdapter(host, ocpAdapter);
			} catch (NamingException e) {
				throw new InitializationException("OCPConnectionPoolManager: OCP initialization failed.", e);
			}
		}

		// Check reachability for each pool before starting any bkg work.
		for (String host : ocpPoolNames) {
			OdakPool pool = getPool(host);
			try {
				pool.processConnectRequest();
			} catch (SQLException | NamingException e) {
				String msg = String.format("Connection creation check failed for the data source: %s", pool.getName());
				logger.error("ODAK_CONNECT_CHECK_FAILED - " + msg, e);
				CalEventHelper.writeException("ODAK_CONNECT_CHECK_FAILED", e, true, msg);
				InitializationException initExp = new InitializationException(msg, e);
				throw initExp;
			}
		}

		Thread poolGroomer = new Thread(OdakGroomer.getInstance());
		poolGroomer.setDaemon(true);
		poolGroomer.start();
		Thread sizeAdjuster = new Thread(OdakSizeAdjuster.getInstance());
		sizeAdjuster.setDaemon(true);
		sizeAdjuster.start();
		Thread stateLog = new Thread(StateLogger.getInstance());
		stateLog.setDaemon(true);
		stateLog.start();
		for (String host : ocpPoolNames) {
			OdakPool pool = getPool(host);
			int timer = 0;
			int maxStartupTime = GroomerConfig.getInstance().getMaxStartupTime() / 1000;
			// stops waiting as soon as pool is filled.
			while (!hasEnouchConns(pool)) {
				String msg = String.format(
						"OCPConnectionPoolManager.init: waiting to get pool %s filled up to its initial size of %d. current conns: %d, time elapsed: %s, max wait time: %s seconds.",
						pool.getName(), getInitialCapacity(pool), pool.getCurrentConnsCount(), timer, maxStartupTime);
				logger.info("ODAK_INIT - {}", msg);
				CalEventHelper.writeLog("ODAK_INIT", pool.getName(), msg, "0");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					logger.error("OCPConnectionPoolManager.init exception", e);
				}
				timer = timer + 1;
				if (timer > maxStartupTime) {
					msg = String.format(
							"OCPConnectionPoolManager.init: OCP initialization failed. could not fill pool %s with its initial estimated size of %d. "
									+ "current conns: %d, time waited: %s seconds",
							pool.getName(), getInitialCapacity(pool), pool.getCurrentConnsCount(), timer);
					InitializationException e = new InitializationException(msg);
					CalEventHelper.writeException("ODAK_INIT_FAILED", e, true, msg);
					logger.error("ODAK_INIT_FAILED - " + msg, e);
					throw e;
				}
			}
		}
	}

	private boolean hasEnouchConns(OdakPool pool) {
		if (pool.getCurrentConnsCount() < pool.getConfig().getPoolExtraCapacity()
				|| pool.getCurrentConnsCount() < pool.getConfig().getMinConnections()) {
			return false;
		}
		return true;
	}

	private int getInitialCapacity(OdakPool pool) {
		if (pool.getConfig().getPoolExtraCapacity() >= pool.getConfig().getMinConnections()) {
			return pool.getConfig().getPoolExtraCapacity();
		} else {
			return pool.getConfig().getMinConnections();
		}
	}

}
