package com.paypal.integ.odak;

import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.cal.CalEventHelper;
import com.paypal.integ.odak.exp.OdakConfigException;

/**
 * Holds pool config per and across pools. Expose as little option as possible
 * to the consumer.
 * 
 */
public class PoolConfig {
	private String host;
	private int poolExtraCapacity = 5;
	private int poolExtraCapacityForAging;

	// low water mark
	private int minConnections = 0;
	// high water mark.
	// pool size auto-adjusts between low and high water marks.
	private int maxConnections = 100;

	// millis
	private long idleTimeout = 20000;
	private long softRecycle = 45000;
	private long hardRecycle = 55000; // plus up to rPaddingEnd
	private int rPaddingStart = 0;
	private int rPaddingEnd = 5000;
	private int orphanTimeout = 600000;
	private int orphanReport = 60000;

	private static final String USER = "user";
	private static final String PASSWORD = "password";

	private String driverClazz;
	private String url;
	private String username;
	private String password;
	private Properties connectionProperties;
	private final static Logger logger = LoggerFactory.getLogger(PoolConfig.class);

	private void buildConnectionProperties() {
		connectionProperties = new Properties();
		connectionProperties.put(USER, username);
		connectionProperties.put(PASSWORD, password);
	}

	public long getHardRecycle() {
		return hardRecycle + getRandom(rPaddingStart, rPaddingEnd);
	}

	public void setForceRecycle(long forceRecycle, int rPaddingStart, int rPaddingEnd) {
		this.hardRecycle = forceRecycle;
		this.rPaddingStart = rPaddingStart;
		this.rPaddingEnd = rPaddingEnd;
	}

	public static int getRandom(int min, int max) {
		Random rand = new Random();
		return rand.nextInt((max - min) + 1) + min;
	}

	// not in use today. currently relying on existing properties for occ-jdbc.
	public Properties getConnectionProperties() {
		Properties result = new Properties();
		result.putAll(connectionProperties);
		return result;
	}

	public void validate() throws OdakConfigException {
		if (Utils.isEmpty(url) || Utils.isEmpty(username) || Utils.isEmpty(password)) {
			throw new OdakConfigException("Validation error: missing data store connect info in the dsimport.");
		}
		if (idleTimeout <= 0) {
			throw new OdakConfigException("Validation error: idleTimeout should not be 0");
		}
		if (orphanTimeout <= 0) {
			throw new OdakConfigException("Validation error: orphanTimeout should not be 0");
		}
		if (orphanTimeout <= orphanReport) {
			orphanReport = orphanTimeout / 4;
		}
		if (hardRecycle <= softRecycle) {
			throw new OdakConfigException("Validation error: hardRecycle interval should be more than hardRecycle");
		}
		if (rPaddingStart >= rPaddingEnd) {
			throw new OdakConfigException(
					"Validation error: Connection recycle padding start interval shound be less than padding end");
		}
		if (poolExtraCapacityForAging <= 0) {
			if (poolExtraCapacity > 0) {
				poolExtraCapacityForAging = (int) Math.ceil((double) poolExtraCapacity / (double) 2);
				String msg = String.format("Computed poolExtraCapacityForAging %d from the poolExtraCapacity of %d.",
						poolExtraCapacityForAging, poolExtraCapacity);
				logger.info(msg);
			} else {
				String msg = String.format(
						"poolExtraCapacity is set to 0 for pool:%s. It can create latency impact during connection recycle.",
						host);
				logger.info(msg);
				CalEventHelper.writeLog("ODAK_CONFIG", host, msg, "0");
				poolExtraCapacityForAging = 0;
			}
		}
		if (poolExtraCapacityForAging > poolExtraCapacity) {
			throw new OdakConfigException(
					"Validation error: poolExtraCapacityForAging should be less than poolExtraCapacity");
		}
		buildConnectionProperties();
	}

	public void dump() {
		String configStr = String.format(
				"pool=%s&idleTimeout=%d&softRecycle=%d&"
						+ "hardRecycle=%d&rPaddingStart=%d&rPaddingEnd=%d&orphanReport=%d&orphanTimeout=%d&minConnections=%d&maxConnections=%d&"
						+ "poolExtraCapacity=%d&poolExtraCapacityForAging=%d&msg=Not allowed to be altered by Users",
				host, idleTimeout, softRecycle, hardRecycle, rPaddingStart, rPaddingEnd, orphanReport, orphanTimeout,
				minConnections, maxConnections, poolExtraCapacity, poolExtraCapacityForAging);
		logger.info("ODAK_INTERNAL_SETTING - {} ", configStr);
		CalEventHelper.writeLog("ODAK_INTERNAL_SETTING", host, configStr, "0");
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public long getSoftRecycle() {
		return softRecycle;
	}

	public void setSoftRecycle(long softRecycle) {
		this.softRecycle = softRecycle;
	}

	public int getrPaddingStart() {
		return rPaddingStart;
	}

	public void setrPaddingStart(int rPaddingStart) {
		this.rPaddingStart = rPaddingStart;
	}

	public int getrPaddingEnd() {
		return rPaddingEnd;
	}

	public void setrPaddingEnd(int rPaddingEnd) {
		this.rPaddingEnd = rPaddingEnd;
	}

	public int getOrphanTimeout() {
		return orphanTimeout;
	}

	public void setOrphanTimeout(int orphanTimeout) {
		this.orphanTimeout = orphanTimeout;
	}

	public int getMinConnections() {
		return minConnections;
	}

	public void setMinConnections(int minConnections) {
		this.minConnections = minConnections;
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	public void setMaxConnections(int maxConnections) {
		this.maxConnections = maxConnections;
	}

	public String getDriverClazz() {
		return driverClazz;
	}

	public void setDriverClazz(String driverClazz) {
		this.driverClazz = driverClazz;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setHardRecycle(long hardRecycle) {
		this.hardRecycle = hardRecycle;
	}

	public void setConnectionProperties(Properties connectionProperties) {
		this.connectionProperties = connectionProperties;
	}

	public int getPoolExtraCapacity() {
		return poolExtraCapacity;
	}

	public void setPoolExtraCapacity(int poolExtraCapacity) {
		this.poolExtraCapacity = poolExtraCapacity;
	}

	public int getPoolExtraCapacityForAging() {
		return poolExtraCapacityForAging;
	}

	public void setPoolExtraCapacityForAging(int poolExtraCapacityForAging) {
		this.poolExtraCapacityForAging = poolExtraCapacityForAging;
	}

	public int getOrphanReport() {
		return orphanReport;
	}

	public void setOrphanReport(int orphanReport) {
		this.orphanReport = orphanReport;
	}

}
