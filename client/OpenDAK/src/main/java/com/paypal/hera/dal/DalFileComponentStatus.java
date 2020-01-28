package com.paypal.hera.dal;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.ebay.kernel.component.ComponentStatus;
import com.ebay.kernel.component.Registration;
import com.ebay.kernel.logger.LogLevel;
import com.ebay.kernel.logger.Logger;

public class DalFileComponentStatus implements ComponentStatus {

	private static final int MAX_FILE_SIZE = 200 * 1024;

	private static DalFileComponentStatus s_instance = null;

	private Set<String> m_dalFiles = new TreeSet<String>();

	private static Logger s_logger = Logger.getInstance(DalFileComponentStatus.class);

	public static synchronized DalFileComponentStatus getInstance() {
		if (s_instance == null) {
			s_instance = new DalFileComponentStatus();
			Registration.registerComponent(s_instance);
		}
		return s_instance;
	}

	@Override
	public String getName() {
		return ("DALFilesOnDisk");
	}

	@Override
	public String getAlias() {
		return ("DALFilesOnDisk");
	}

	@Override
	public String getStatus() {
		return ("Loaded");
	}

	/**
	 * Return the properties that will be displayed on the component status page
	 */
	@Override
	public List<Map<String, String>> getProperties() {
		synchronized (this) {
			List<Map<String, String>> propList = new ArrayList<Map<String, String>>();
			Map<String, String> prop = new HashMap<String, String>();
			if (m_dalFiles.size() > 0) {
				Iterator<String> iter = m_dalFiles.iterator();
				while (iter.hasNext()) {
					String filename = iter.next();
					String contents = getFileOnDisk(filename);
					if (contents != null)
						contents = contents.replaceAll("\\{\\{.*=", "********");
					prop.put(filename, contents);
				}
			}
			propList.add(prop);
			return propList;
		}
	}

	/**
	 * Register the file as one that should show up on the page
	 * @param fileURL the URL for the file (usually obtained from a ResourceUtil.getResource call)
	 */
	public synchronized void registerFile(URL fileURL) {
		m_dalFiles.add(fileURL.getFile());
	}

	/**
	 * Retrieve the actual contents of the file as a String.  This method is expecting that all DAL files are less
	 * than 200K bytes long.
	 * 
	 * @param filename the name of the file to retrieve
	 * @return a string containing the contents of the file
	 */
	private String getFileOnDisk(String filename) {
		String retStr = "";
		if ((filename == null) || (filename.trim().length() == 0)) {
			return (retStr);
		}

		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			char[] c = new char[MAX_FILE_SIZE];
			int bytesRead = br.read(c);
			retStr = new String(c, 0, bytesRead);
		}
		catch (IOException e) {
			retStr = "*** Encountered an exception while reading the config file: " + e
					+ "  Please refer to ebay.log for additional information. ***";
			getLogger().log(LogLevel.ERROR, "Encountered while reading DAL config file at: " + filename, e);
		}
		finally {
			if (br != null) {
				try {
					br.close();
				}
				catch (Exception e) {
					// should not happen but log it anyway
					getLogger().log(LogLevel.ERROR, "Encountered while closing DAL config file at: " + filename, e);
				}
			}
		}

		return (retStr);
	}

	private static Logger getLogger() {
		return (s_logger);
	}
}
