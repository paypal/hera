package com.paypal.integ.odak;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Utils {

	private final static Logger logger = LoggerFactory.getLogger(Utils.class);

	private Utils() {
	}

	public static void sleep(long sleepTime) {
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			logger.error("Thread Interrupted during sleep.", e);
		}
	}

	public static boolean isEmpty(String data) {
		return data == null || data.isEmpty();
	}
}
