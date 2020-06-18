package com.paypal.integ.odak;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.cal.CalEventHelper;

/**
 * 
 * Adjusts pool size every X minutes based on the model used. Uses checkedOut
 * connection as a traffic measure instead of the current pool-size as pool size
 * includes both idle + checkedout connections.
 * 
 * Options for Models: 1) Simple Avg model 2) Weighted Avg model 3) Split upward
 * and downward setting.
 * 
 * 
 */
public class OdakSizeAdjuster implements Runnable {
	private final static OdakSizeAdjuster INSTANCE = new OdakSizeAdjuster();
	private final static Logger logger = LoggerFactory.getLogger(OdakSizeAdjuster.class);
	private int numRestarts;

	private Map<String, OdakPool> pools = new ConcurrentHashMap<String, OdakPool>();
	private long sumTrkIntervalUpward = 0;

	private Map<String, PoolTrackingData> trackingData = new ConcurrentHashMap<String, PoolTrackingData>();

	public static OdakSizeAdjuster getInstance() {
		return INSTANCE;
	}

	private OdakSizeAdjuster() {
	}

	@Override
	public void run() {
		String msg = String.format("OCC Pool Usage Tracker started. Thread Id: %s", Thread.currentThread().getId());
		logger.info("ODAK_INIT - " + msg);
		CalEventHelper.writeLog("ODAK_INIT", "OCPSizeAdjuster", msg, "0");

		long trkInterval = GroomerConfig.getInstance().getPoolSizeTrackingInterval();
		long resizeInterval = GroomerConfig.getInstance().getPoolResizeInterval();
		long upwardResizeInterval = GroomerConfig.getInstance().getPoolUpwardResizeInterval();

		// force first upward adjustment
		sumTrkIntervalUpward = upwardResizeInterval;

		try {
			while (true) {
				Utils.sleep(trkInterval);
				sumTrkIntervalUpward = sumTrkIntervalUpward + trkInterval;
				trackUsage(trkInterval);
				if (sumTrkIntervalUpward >= upwardResizeInterval) {
					adjustPoolsUpward();
					sumTrkIntervalUpward = 0;
				}
				for (Map.Entry<String, OdakPool> entry : pools.entrySet()) {
					OdakPool pool = entry.getValue();
					PoolTrackingData tData = trackingData.get(pool.getName());
					if (tData.getSumTrkInterval() >= resizeInterval) {
						adjust(pool);
						tData.setSumTrkInterval(0);
					}
				}
			}
		} catch (Throwable t) {
			msg = String.format("PoolSizeAdjuster thread [%d] is about to die with an error: %s.",
					Thread.currentThread().getId(), t.getMessage());
			logger.error("ODAK_SIZEADJUSTER_DIED - " + msg, t);
			CalEventHelper.writeException("ODAK_SIZEADJUSTER_DIED", t, true, msg);
			forceRetry();
		}
	}

	private void forceRetry() {
		if (numRestarts > GroomerConfig.getInstance().getBkgThreadRestartAttempts()) {
			String msg = String.format(
					"Background PoolSizeAdjuster thread - [%d] - cannot be restarted. Exceeds num of max restarts: %d. Total restarts so far: %d",
					Thread.currentThread().getId(), GroomerConfig.getInstance().getBkgThreadRestartAttempts(),
					numRestarts);
			logger.error("ODAK_SIZEADJUSTER_EXCEED_MAX_RESTARTS - {}", msg);
			CalEventHelper.writeLog("ODAK_SIZEADJUSTER_EXCEED_MAX_RESTARTS", "ALLHOSTS", msg, "1");
			return;
		}
		numRestarts++;
		Thread useTracker = new Thread(OdakSizeAdjuster.getInstance());
		useTracker.setDaemon(true);
		useTracker.start();
		String msg = String.format("Pool size adjuster background thread retstarted successfully. RetryCount= %d",
				numRestarts);
		logger.info("ODAK_SIZEADJUSTER_RESTARTED - {}", msg);
		CalEventHelper.writeLog("ODAK_SIZEADJUSTER_RESTARTED", "ALLHOSTS", msg, "0");
	}

	private void trackUsage(long trkInterval) {
		for (Map.Entry<String, OdakPool> entry : pools.entrySet()) {
			OdakPool pool = entry.getValue();
			PoolTrackingData tData = trackingData.get(pool.getName());
			if (tData == null) {
				tData = new PoolTrackingData();
				trackingData.put(pool.getName(), tData);
			}
			tData.setSumTrkInterval(tData.getSumTrkInterval() + trkInterval);
			tData.setSumCheckoutConnUpward(tData.getSumCheckoutConnUpward() + pool.getActiveConnsCount());
			tData.setSamplesUpward(tData.getSamplesUpward() + 1);
			tData.setSumCheckoutConn(tData.getSumCheckoutConn() + pool.getActiveConnsCount());
			tData.setSamples(tData.getSamples() + 1);
		}
	}

	private int computeNewPoolSize(long sum, long samples, OdakPool pool) {
		int newPoolSize;
		int modeledSize;
		int cnt = 1;
		for(int spikeVal : pool.getSpikes()) {
		    sum = sum + spikeVal;
		    samples = samples + cnt;
		    cnt++;
		}
		if (sum != 0 && samples != 0) {
			modeledSize = (int) Math.ceil((double) sum / (double) samples);
			newPoolSize = modeledSize + pool.getConfig().getPoolExtraCapacity();
		} else {
			newPoolSize = pool.getConfig().getPoolExtraCapacity();
		}

		if (newPoolSize < pool.getConfig().getMinConnections()) {
			String msg = String.format(
					"Pool:%s - estimated pool size:%d is lower than the configured minConnections:%d. Using configured minConnections.",
					pool.getName(), newPoolSize, pool.getConfig().getMinConnections());
			logger.info("ODAK_SET_MIN - {}", msg);
			CalEventHelper.writeLog("ODAK_SET_MIN", pool.getName(), msg, "0");

			newPoolSize = pool.getConfig().getMinConnections();
		}
		return newPoolSize;
	}

	private void adjustPoolsUpward() {
		for (Map.Entry<String, OdakPool> entry : pools.entrySet()) {
			OdakPool pool = entry.getValue();
			PoolTrackingData tData = trackingData.get(pool.getName());
			int currPoolSize = pool.getSize();
			int newPoolSize = computeNewPoolSize(tData.getSumCheckoutConnUpward(), tData.getSamplesUpward(), pool);
			String msg = String.format(
					"Pool size adjustment - pool:%s, newSize:%d, currSize:%d, sumActiveConn:%d, sampleCount:%d, spikes:%s",
					pool.getName(), newPoolSize, currPoolSize, tData.getSumCheckoutConnUpward(),
					tData.getSamplesUpward(), Arrays.toString(pool.getSpikes()));
			if (newPoolSize > currPoolSize) {
				logger.info("ODAK_ADJ_SHORT_TERM_HIGH - {}", msg);
				CalEventHelper.writeLog("ODAK_ADJ_SHORT_TERM_HIGH", pool.getName(), msg, "0");

				pool.setSize(newPoolSize);
				// Reset overall tracking as we see peak again during the
				// last interval. Don't want to reduce pool size when traffic is
				// increased or getting multiple and very frequent peaks.
				tData.resetSamples();
			} else if (newPoolSize == currPoolSize) {
				// Don't look at past results as traffic is sustaining.
				tData.resetSamples();
				logger.info("ODAK_ADJ_SHORT_TERM_CONSTANT - {}", msg);
				CalEventHelper.writeLog("ODAK_ADJ_SHORT_TERM_CONSTANT", pool.getName(), msg, "0");
			}
			pool.resetSpikes();
			tData.resetSamplesUpward();
			addCapacity(pool, newPoolSize, false);
		}
	}

	void addCapacity(OdakPool pool, int newPoolSize, boolean isSpikeAdj) {
		int currConns = pool.getCurrentConnsCount();
		if (currConns < newPoolSize) {
			// In the extreme case, may get one or two extra submitted as free
			// & active lists are not locked. Adjustment will happen
			// during next recycle interval.
			// by default ensureMin effect is always enabled.
			String eventType = isSpikeAdj ? "ODAK_SPIKE_BKG_CONNECT_REQUEST" : "ODAK_RESIZE_BKG_CONNECT_REQUEST";
			for (int i = 0; i < newPoolSize - currConns; i++) {
				String msg = String.format(
						"Adding background OCP connection request as part of pool resize. pool:%s, "
								+ "currConns:%d, newPoolSize: %d, isSpikeAdj: %b",
						pool.getName(), currConns, newPoolSize, isSpikeAdj);
				logger.info("{} - {}", eventType, msg);
				CalEventHelper.writeLog(eventType, pool.getName(), msg, "0");
				OdakGroomer.getInstance().addConnectionRequest(pool, false);
			}
		}
	}

	/**
	 * Overall long term adjustment. Can be upward or downward based on the
	 * traffic pattern. Frequent and high spikes may end up overall upward
	 * adjustment. Reduction in overall traffic, less frequent and/or low spikes
	 * may cause overall downward adjustment.
	 * 
	 * @param pool
	 */
	private void adjust(OdakPool pool) {
		PoolTrackingData tData = trackingData.get(pool.getName());
		int currPoolSize = pool.getSize();
		int newPoolSize = computeNewPoolSize(tData.getSumCheckoutConn(), tData.getSamples(), pool);
		String msg = String.format(
				"Pool size adjustment - pool:%s, newSize:%d, currSize:%d, sumActiveConn:%d, sampleCount:%d, spikes:%s",
				pool.getName(), newPoolSize, currPoolSize, tData.getSumCheckoutConn(), tData.getSamples(), Arrays.toString(pool.getSpikes()));
		if (newPoolSize > currPoolSize) {
			logger.info("ODAK_ADJ_LONG_TERM_HIGH - {}", msg);
			CalEventHelper.writeLog("ODAK_ADJ_LONG_TERM_HIGH", pool.getName(), msg, "0");
			pool.setSize(newPoolSize);
		} else if (newPoolSize < currPoolSize) {
			logger.info("ODAK_ADJ_LONG_TERM_LOW - {}", msg);
			CalEventHelper.writeLog("ODAK_ADJ_LONG_TERM_LOW", pool.getName(), msg, "0");
			pool.setSize(newPoolSize);
		} else {
			logger.info("ODAK_ADJ_LONG_TERM_CONSTANT - No adjustment needed. {}", msg);
			CalEventHelper.writeLog("ODAK_ADJ_LONG_TERM_CONSTANT", pool.getName(), msg, "0");
		}
		pool.resetSpikes();
		tData.resetSamples();
		addCapacity(pool, newPoolSize, false);
	}

	void register(OdakPool pool) {
		pools.put(pool.getName(), pool);
	}

	void unregister(OdakPool pool) {
		pools.remove(pool.getName());
	}

	private static class PoolTrackingData {
		private long sumCheckoutConn;
		private long sumCheckoutConnUpward;
		private long samples;
		private long samplesUpward;
		private long sumTrkInterval;

		PoolTrackingData() {
		}

		public long getSumCheckoutConn() {
			return sumCheckoutConn;
		}

		public void setSumCheckoutConn(long sumCheckoutConn) {
			this.sumCheckoutConn = sumCheckoutConn;
		}

		public long getSumCheckoutConnUpward() {
			return sumCheckoutConnUpward;
		}

		public void setSumCheckoutConnUpward(long sumCheckoutConnUpward) {
			this.sumCheckoutConnUpward = sumCheckoutConnUpward;
		}

		public void resetSamplesUpward() {
			setSumCheckoutConnUpward(0);
			setSamplesUpward(0);
		}

		public long getSamples() {
			return samples;
		}

		public void setSamples(long samples) {
			this.samples = samples;
		}

		public long getSamplesUpward() {
			return samplesUpward;
		}

		public void setSamplesUpward(long samplesUpward) {
			this.samplesUpward = samplesUpward;
		}

		public void resetSamples() {
			setSumCheckoutConn(0);
			setSamples(0);
			setSumTrkInterval(0);
		}

		public long getSumTrkInterval() {
			return sumTrkInterval;
		}

		public void setSumTrkInterval(long sumTrkInterval) {
			this.sumTrkInterval = sumTrkInterval;
		}
	}
}
