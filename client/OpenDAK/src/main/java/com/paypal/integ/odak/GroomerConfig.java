package com.paypal.integ.odak;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.cal.CalEventHelper;
import com.paypal.integ.odak.exp.OdakConfigException;

public class GroomerConfig {
	private int bkgExecutorPoolSize = 15;
	private int maxExecutorQueuelength = 100;
	private String stateLogFilePath = ".";
	private int stateLogFrequency = 1000;
	private boolean isStateLogEnable = true;
	private int bkgThreadRestartAttempts = 10; // keep higher

	// millis
	private long poolSizeTrackingInterval = 50;
	private long poolResizeInterval = 300000;
	private long poolUpwardResizeInterval = 10000;
	private long groomInterval = 1000;
	private int maxStartupTime = 60000;
	private int spikeAdjInterval = 5000;

	private final static Logger logger = LoggerFactory.getLogger(GroomerConfig.class);
	private final static GroomerConfig INSTANCE = new GroomerConfig();

	private GroomerConfig() {
	}

	public static GroomerConfig getInstance() {
		return INSTANCE;
	}

	public int getBkgExecutorPoolSize() {
		return bkgExecutorPoolSize;
	}

	public void setBkgExecutorPoolSize(int bkgExecutorPoolSize) {
		this.bkgExecutorPoolSize = bkgExecutorPoolSize;
	}

	public String getStateLogFilePath() {
		return stateLogFilePath;
	}

	public void setStateLogFilePath(String stateLogFilePath) {
		this.stateLogFilePath = stateLogFilePath;
	}

	public long getPoolSizeTrackingInterval() {
		return poolSizeTrackingInterval;
	}
	
	public long getSpikeAdjInterval() {
		return spikeAdjInterval;
	}
	
	public void setPoolSizeTrackingInterval(long poolSizeTrackingInterval) {
		this.poolSizeTrackingInterval = poolSizeTrackingInterval;
	}

	public long getPoolResizeInterval() {
		return poolResizeInterval;
	}

	public void setPoolResizeInterval(long poolResizeInterval) {
		this.poolResizeInterval = poolResizeInterval;
	}

	public long getPoolUpwardResizeInterval() {
		return poolUpwardResizeInterval;
	}

	public void setPoolUpwardResizeInterval(long poolUpwardResizeInterval) {
		this.poolUpwardResizeInterval = poolUpwardResizeInterval;
	}

	public int getStateLogFrequency() {
		return stateLogFrequency;
	}

	public void setStateLogFrequency(int stateLogFrequency) {
		this.stateLogFrequency = stateLogFrequency;
	}

	public boolean isStateLogEnable() {
		return isStateLogEnable;
	}

	public void setStateLogEnable(boolean isStateLogEnable) {
		this.isStateLogEnable = isStateLogEnable;
	}

	public int getBkgThreadRestartAttempts() {
		return bkgThreadRestartAttempts;
	}

	public void setBkgThreadRestartAttempts(int bkgThreadRestartAttempts) {
		this.bkgThreadRestartAttempts = bkgThreadRestartAttempts;
	}

	public long getGroomInterval() {
		return groomInterval;
	}

	public void setGroomInterval(long groomInterval) {
		this.groomInterval = groomInterval;
	}

	public int getMaxExecutorQueuelength() {
		return maxExecutorQueuelength;
	}

	public void setMaxExecutorQueuelength(int maxExecutorQueuelength) {
		this.maxExecutorQueuelength = maxExecutorQueuelength;
	}

	public int getMaxStartupTime() {
		return maxStartupTime;
	}

	public void setMaxStartupTime(int maxStartupTime) {
		this.maxStartupTime = maxStartupTime;
	}

	public void validate() throws OdakConfigException {
		if (poolResizeInterval % poolSizeTrackingInterval != 0) {
			throw new OdakConfigException(
					"Validation error: poolResizeInterval should be multiple of poolSizeTrackingInterval");
		}
	}

	public void dump() {
		String configStr = String.format(
				"bkgExecutorPoolSize=%d&maxExecutorQueuelength=%d&stateLogFilePath=%s&"
						+ "poolSizeTrackingInterval=%d&poolResizeInterval=%d&poolUpwardResizeInterval=%d&stateLogFrequency=%d&"
						+ "isStateLogEnable=%b&bkgThreadRestartAttempts=%d&groomInterval=%d&maxStartupTime=%d&msg=Not allowed to be altered by Users",
				bkgExecutorPoolSize, maxExecutorQueuelength, stateLogFilePath, poolSizeTrackingInterval,
				poolResizeInterval, poolUpwardResizeInterval, stateLogFrequency, isStateLogEnable,
				bkgThreadRestartAttempts, groomInterval, maxStartupTime);
		logger.info("ODAK_GROOMER_INTERNAL_SETTING - {}", configStr);
		CalEventHelper.writeLog("ODAK_GROOMER_INTERNAL_SETTING", "ALLHOSTS", configStr, "0");
	}
}
