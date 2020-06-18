package com.paypal.integ.odak;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.cal.CalEventHelper;

/**
 * State log for all the OCP connection pools. Each connection pool will have
 * it's own state log written to separate file. Don't do avg connection or any
 * costly computation in the state log as it can impact the throughput.
 * 
 * Keep this isolated from Groomer and PoolSizeAdjuster as this has a different
 * level of criticality, as well as deals with the file i/o.
 * 
 */
public class StateLogger implements Runnable {
	private final static StateLogger INSTANCE = new StateLogger();
	private final static Logger logger = LoggerFactory.getLogger(StateLogger.class);

	private Map<String, PoolStateWriter> poolWriters = new ConcurrentHashMap<>();

	public static StateLogger getInstance() {
		return INSTANCE;
	}

	private StateLogger() {
	}

	public void register(OdakPool pool) {
		poolWriters.put(pool.getName(), new PoolStateWriter(pool, GroomerConfig.getInstance().getStateLogFilePath()));
		logger.info("Registered pool {} with StateLogger", pool.getName());

	}

	public void unregister(OdakPool pool) {
		poolWriters.remove(pool.getName());
	}
	
	public String getState(String poolName){
		return poolWriters.get(poolName).getState();
	}

	@Override
	public void run() {
		if (!GroomerConfig.getInstance().isStateLogEnable()) {
			logger.info("State logger is not enabled");
			return;
		}
		String msg = String.format("OCP State logger started. Thread Id: %s", Thread.currentThread().getId());
		logger.info("ODAK_INIT - " + msg);
		CalEventHelper.writeLog("ODAK_INIT", "OCPStateLogger", msg, "0");
		try {
			log();
		} catch (Throwable t) {
			msg = "Dal state logger thread is about to die. It will not be auto-restarted to avoid any negative impact.";
			CalEventHelper.writeException("ODAK_STATE_WRITER_DIED", t, true, msg);
			logger.error("ODAK_STATE_WRITER_DIED" + msg, t);
		}
	}

	public static class PoolStateWriter {
		private File file;
		private FileWriter fw;
		private BufferedWriter bw;
		private long headerCnt;
		private long calFlushCnt;
		private long detailStateCnt;
		private OdakPool pool;
		private DateFormat df;
		private String header;
		private int maxStateDelta = 15;
		private long maxOrphanConnDelta = 5;
		private ODAKState prevState;
		private ODAKState currState;
		
		private class ODAKState {
				public long idled;
				public long aged;
				public long orphaned;
				public long fgCreated;
				public long bkgCreated;
				public long closed;
				public long destroyed;
		}

		public PoolStateWriter(OdakPool pool, String filePath) {
			this.pool = pool;
			this.prevState = new ODAKState();
			this.currState = new ODAKState();
			// Keep only CAL as single source of truth, instead of using
			// both local file and CAL.
			//createLocalFile(filePath);
		}
		
		public String getState(){
			String ocpState = String.format(
					"poolNm=%s&estPoolSz=%d&active=%d&free=%d&opncls=%d&idled=%d&aged=%d&orphaned=%d&fgOpn=%d"
							+ "&bkgReqs=%d&szBkgReqs=%d&bkgOpn=%d&closed=%d&agedClz=%d&idleClz=%d&activeClz=%d&destroyed=%d",
					pool.getName(), pool.getSize(), pool.getActiveConnsCount(), pool.getFreeConnsCount(),
					pool.getAllConnsCountByCreatedDestroyed(), pool.getIdleConnsCount(), pool.getAgedConnsCount(),
					pool.getOrphanConnsCount(), pool.getFgConnsCreated(), pool.getBkgConnsReqs(),
					pool.getPoolResizeBkgConnsReqs(), pool.getBkgConnsCreated(),
					pool.getClosedConns(), pool.getAgedClosed(), pool.getIdleClosed(), pool.getActiveClosed(),
					pool.getDestroyedConns());
			return ocpState;
		}
		
		public String getMinimalState() {
			currState.idled = pool.getIdleConnsCount();
			currState.aged = pool.getAgedConnsCount();
			currState.orphaned = pool.getOrphanConnsCount();
			currState.fgCreated = pool.getFgConnsCreated();
			currState.bkgCreated = pool.getBkgConnsCreated();
			currState.closed = pool.getClosedConns();
			currState.destroyed = pool.getDestroyedConns();

			String ocpState = String.format(
					"poolNm=%s&estPoolSz=%d&active=%d&free=%d&idled=%d&aged=%d&orphaned=%d&fgOpn=%d"
							+ "&bkgOpn=%d&closed=%d&destroyed=%d",
					pool.getName(), pool.getSize(), pool.getActiveConnsCount(), pool.getFreeConnsCount(),
					currState.idled - prevState.idled, currState.aged - prevState.aged, currState.orphaned - prevState.orphaned,
					currState.fgCreated - prevState.fgCreated, currState.bkgCreated - prevState.bkgCreated,
					currState.closed - prevState.closed, currState.destroyed - prevState.destroyed);

			prevState.idled = currState.idled;
			prevState.aged = currState.aged;
			prevState.orphaned = currState.orphaned;
			prevState.fgCreated = currState.fgCreated;
			prevState.bkgCreated = currState.bkgCreated;
			prevState.closed = currState.closed;
			prevState.destroyed = currState.destroyed;
			return ocpState;
		}

		/**
		 * If traffic remains steady and we've free connections, aged
		 * connections will be recreated by background. If traffic spikes up and
		 * if we run out of free connections, aged connections will be recreated
		 * by background as well as by foreground. Idea is to keep connection
		 * pool size little higher than active connections to give room for aged
		 * connections to recycle in the background.
		 * 
		 */
		public void write() {
			if(detailStateCnt == 5){
				CalEventHelper.writeLog("ODAK_STATE_DETAIL", pool.getName(), getState(), "0");
				detailStateCnt = 0;
			}
			CalEventHelper.writeLog("ODAK_STATE", pool.getName(), getMinimalState(), "0");
			int currConns = pool.getActiveConnsCount() + pool.getFreeConnsCount();
			int connCounter = pool.getAllConnsCountByCreatedDestroyed();
			int connDelta = Math.abs(currConns-connCounter);
			if (connDelta > maxStateDelta) {
				String msg = String.format(
						"StateLogger: Potential warning sign as opncls (running counter of open less close conns) of %d "
								+ "does not match with active plus free connections of %d. state:%s. "
								+ "Report this to DAL team if received this warning frequently",
						connCounter, currConns, getState());
				logger.warn("ODAK_STATELOG_ALERT - {}", msg);
				CalEventHelper.writeLog("ODAK_STATELOG_ALERT", pool.getName(), msg, "3");
				maxStateDelta = connDelta + 1;
			}
			long orphanConns = pool.getOrphanConnsCount();
			if (orphanConns > maxOrphanConnDelta) {
				String msg = String.format(
						"StateLogger: Orphan connection count is increasing. "
								+ "This indicates application is using long running transactions or keeping connection checked out for a long time. state: %s",
						getState());
				logger.warn("ODAK_STATELOG_ORPHAN_ALERT - {}", msg);
				CalEventHelper.writeLog("ODAK_STATELOG_ORPHAN_ALERT", pool.getName(), msg, "3");
				maxOrphanConnDelta = orphanConns + 1;
			}
			if (calFlushCnt == 3000) {
				pool.getConfig().dump();
				GroomerConfig.getInstance().dump();
				calFlushCnt = 0;
			}
			headerCnt++;
			calFlushCnt++;
			detailStateCnt++;
		    //writeLocalFile();
		}

		private void writeLocalFile() {
			try {
				if (headerCnt == 10) {
					bw.write(header);
					bw.flush();
					headerCnt = 0;
				}
				Date dateTime = Calendar.getInstance().getTime();
				String dateTimeFmt = df.format(dateTime);
				String logData = String.format(
						"%20s %8s %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d\n", dateTimeFmt,
						pool.getName(), pool.getSize(), pool.getActiveConnsCount(), pool.getFreeConnsCount(),
						pool.getAllConnsCountByCreatedDestroyed(), pool.getIdleConnsCount(), pool.getAgedConnsCount(),
						pool.getOrphanConnsCount(), pool.getFgConnsCreated(), pool.getBkgConnsReqs(),
						pool.getPoolResizeBkgConnsReqs(), pool.getBkgConnsCreated(),
						pool.getClosedConns(), pool.getAgedClosed(), pool.getIdleClosed(), pool.getActiveClosed(),
						pool.getDestroyedConns());
				bw.write(logData);
				bw.flush();
			} catch (IOException e) {
				String msg = String.format("Can not emit state logs for pool: %s", pool.getName());
				CalEventHelper.writeException("ODAK_STATE_WRITER_FAILED", e, true, msg);
				logger.error("ODAK_STATE_WRITER_FAILED" + msg, e);
			}
		}

		private void createLocalFile(String filePath) {
			header = String.format("%20s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s %8s\n",
					"daytime", "poolNm", "estPoolSz", "active", "free", "opncls", "idled", "aged", "orphaned", "fgOpn",
					"bkgReqs", "szBkgReqs", "bkgOpn", "closed", "agedClz", "idledClz", "activeClz",
					"destroyed");
			df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
			file = new File(filePath + "/state-" + pool.getName() + ".log");

			try {
				if (!file.exists()) {
					file.createNewFile();
				}
				fw = new FileWriter(file.getAbsoluteFile(), true);
				bw = new BufferedWriter(fw);
			} catch (IOException e) {
				String msg = String.format("Can not create pool state writer for the pool: %s", pool.getName());
				CalEventHelper.writeException("ODAK_STATE_WRITER_FAILED", e, true, msg);
				logger.error("ODAK_STATE_WRITER_FAILED" + msg, e);
			}
		}

		public void close() {
			try {
				bw.close();
			} catch (IOException e) {
				String msg = String.format("Can not close state write for pool: %s", pool.getName());
				CalEventHelper.writeException("ODAK_STATE_WRITER_CLOSE_FAILED", e, true, msg);
				logger.error("ODAK_STATE_WRITER_CLOSE_FAILED" + msg, e);
			}
		}
	}

	private void log() {
		while (true) {
			Utils.sleep(GroomerConfig.getInstance().getStateLogFrequency());
			for (PoolStateWriter poolWriter : poolWriters.values()) {
				poolWriter.write();
			}
		}
	}

	// TODO: Call from DAL shutdown sequence.
	// Local file not enabled in prod. Rely on CAL as a single source.
	public void shutdown() {
		for (PoolStateWriter poolWriter : poolWriters.values()) {
			poolWriter.close();
		}
	}

}
