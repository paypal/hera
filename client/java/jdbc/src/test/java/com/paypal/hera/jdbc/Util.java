package com.paypal.hera.jdbc;

import java.net.*;
import java.nio.file.*;
import java.io.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.client.HeraClientImpl;
import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.ex.HeraClientException;
import com.paypal.hera.jdbc.HeraConnection;
import com.paypal.hera.util.MurmurHash3;
import com.paypal.hera.util.HeraJdbcConverter;
import com.paypal.hera.util.HeraJdbcUtil;

/** Helper functions for testing. */
public class Util {

	/** Returns a new HeraConnection.  Connects to server in 
	 System.getProperty("SERVER_URL") and defaults to localhost:11111 */
	public static HeraConnection makeDbConn() {
		try {
			String host = System.getProperty("SERVER_URL", "1:127.0.0.1:11111");
			HeraClientConfigHolder.clear();
			Properties props = new Properties();
			props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
			props.setProperty(HeraClientConfigHolder.SUPPORT_RS_METADATA_PROPERTY, "true");
			props.setProperty(HeraClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY, "true");
			props.setProperty(HeraClientConfigHolder.ENABLE_SHARDING_PROPERTY, "true");
			Class.forName("com.paypal.hera.jdbc.HeraDriver");
			return (HeraConnection)DriverManager.getConnection("jdbc:hera:" + host, props);
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}


	public static void runDml(Connection dbConn, String sql) {
		try {
			PreparedStatement pst = dbConn.prepareStatement(sql);
			pst.executeUpdate();
			dbConn.commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/** Compiles and starts Hera server and a docker Mysql. Cleans up
	 old Hera and Mysql before it remakes new ones. Uses GOROOT to find
	 go compiler and GOPATH to find the binaries and make a directory
	 with config and logs for the test hera server. */
	public static void makeAndStartHeraMux(HashMap<String,String> cfg) {
		try {
			makeAndStartHeraMuxInternal(cfg);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	static boolean checkMySqlIsUp() {
		boolean didConn = false;
		for (int i = 0; i < 10; i++) {
			Socket clientSocket = new Socket();
			try {
				Thread.sleep(1222);
				clientSocket.connect(new InetSocketAddress("127.0.0.1", 3306), 2000);
				didConn = true;
				clientSocket.close();
				break;
			} catch (ConnectException e) {
				continue;
			} catch (SocketTimeoutException e) {
				continue;
			} catch (IOException e) {
				continue;
			} catch (InterruptedException e) {
				continue;
			}
		}
		return didConn;
	}
	static boolean checkDockerLogs(String cmd) {
		for(int i = 0; i < 10; i++){
			try{
				Thread.sleep(1222);
				ProcessBuilder builder = new ProcessBuilder("bash", "-c", cmd);
				builder.redirectErrorStream(true);
				Process process = builder.start();
				InputStream is = process.getInputStream();
				BufferedReader reader = new BufferedReader(new InputStreamReader(is));
				if (reader.readLine() != null){
					return true;
				}
			}
			catch (IOException ex){
				ex.printStackTrace();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		return false;
	}
	static void startMySqlContainer() throws IOException, InterruptedException {
		if(checkMySqlIsUp()) return;

		String dockerName = "mysql55";
		Runtime.getRuntime().exec("docker stop "+dockerName).waitFor();
		Runtime.getRuntime().exec("docker rm "+dockerName).waitFor();
		// ensure that localhost's port is mapped to container port,
		// else hera worker can not reach mysql
		Runtime.getRuntime()
				.exec("docker run -p 3306:3306 --name "
						+dockerName+
						" -e TZ=America/Los_Angeles " +
						"-e MYSQL_ROOT_PASSWORD=1-testDb " +
						"-e MYSQL_DATABASE=heratestdb " +
						"-d mysql:8.0.31").waitFor();

		String cmd = "grep -i \"ready for start up\" <(docker logs mysql55 2>&1)";
		if (!checkDockerLogs(cmd)){
			throw new RuntimeException("mysql docker didn't start");
		}

		if (!checkMySqlIsUp()) {
			Runtime.getRuntime().exec("docker stop "+dockerName).waitFor();
			Runtime.getRuntime().exec("docker rm "+dockerName).waitFor();
			throw new RuntimeException("mysql docker did not come up");
		}
	}

	public static void stopMySqlContainer() throws IOException, InterruptedException {
		if(checkMySqlIsUp()){
			String dockerName = "mysql55";
			Runtime.getRuntime().exec("docker stop "+dockerName).waitFor();
			Runtime.getRuntime().exec("docker rm "+dockerName).waitFor();
		}
	}

	static void makeAndStartHeraMuxInternal(HashMap<String,String> cfg) throws IOException, InterruptedException {
		startMySqlContainer();
		if (cfg == null) {
			cfg = new HashMap<String,String>();
		}

		Runtime.getRuntime().exec("go install github.com/paypal/hera/mux github.com/paypal/hera/worker/mysqlworker").waitFor();
		Runtime.getRuntime().exec("killall -ILL mux mysqlworker").waitFor();

		String gopath = System.getenv().get("GOPATH");
		String basedir = gopath+"/srv/";
		File basedirF = new File(basedir);
		basedirF.mkdir();


		File symLinkTarget;
		symLinkTarget = new File(basedir+"mux");
		if (!symLinkTarget.exists()) {
			Files.createSymbolicLink(
					symLinkTarget.toPath(),
					(new File(gopath+"/bin/" + symLinkTarget.getName())).toPath());
		}
		symLinkTarget = new File(basedir+"mysqlworker");
		if (!symLinkTarget.exists()) {
			Files.createSymbolicLink(
					symLinkTarget.toPath(),
					(new File(gopath+"/bin/" + symLinkTarget.getName())).toPath());
		}

		BufferedWriter writer;
		writer = new BufferedWriter(new FileWriter(basedir+"cal_client.txt"));
		writer.write("enable_cal=true\n");
		writer.write("cal_handler=file\n");
		writer.write("cal_pool_name=stage_hera\n");
		writer.write("cal_log_file=./cal.log\n");
		writer.write("cal_pool_stack_enable=true\n");
		writer.close();

		cfg.putIfAbsent("bind_ip", "127.0.0.1");
		cfg.putIfAbsent("bind_port", "11111");
		cfg.putIfAbsent("random_start_ms", "1");
		cfg.putIfAbsent("opscfg.hera.server.max_connections","4");
		cfg.putIfAbsent("log_level","5");
		cfg.putIfAbsent("rac_sql_interval","0");
		cfg.putIfAbsent("database_type","mysql");
//		cfg.putIfAbsent("cert_chain_file","srvChain.crt");
//		cfg.putIfAbsent("key_file","srv2.key");
		writer = new BufferedWriter(new FileWriter(basedir+"hera.txt"));
		for (String key : cfg.keySet()) {
			writer.write(key + "=" + cfg.get(key) + "\n");
		}
		writer.close();


		ProcessBuilder pb = new ProcessBuilder("./mux", "--name", "hera-test");
		pb.directory(basedirF);
		Map<String,String> env = pb.environment();
		// mysql connectivity works with localhost and not with docker container IP
		env.put("TWO_TASK","tcp(127.0.0.1:3306)/heratestdb");
		env.put("username","root");
		env.put("password","1-testDb");
//		env.put("certdir", basedir);
		s_hera = pb.start();

		boolean didConn = false;
		for (int i = 0; i < 111; i++) {
			Thread.sleep(1222);
			Socket clientSocket = new Socket();
			try {
				clientSocket.connect(new InetSocketAddress("localhost", 11111), 2000);
			} catch (ConnectException e) {
				continue;
			} catch (SocketTimeoutException e) {
				continue;
			}
			clientSocket.close();
			didConn = true;
			break;
		}
		if (!didConn) {
			throw new RuntimeException("hera srv did not come up");
		}
	}
	static Process s_hera, o_hera;
		
	public static void startOracleContainer(String dockerName, String userName, String password) {
		File sqlPath = new File("src/test/resources");
		String absPath = sqlPath.getAbsolutePath();
        Process proc = null;
        try {
                proc = Runtime.getRuntime()
                    .exec("docker run -d --name " + dockerName +
							" -p 1521:1521 -e ORACLE_RANDOM_PASSWORD=\"y\" " +
							"-e ORACLE_DATABASE=poolname " +
							"-v "+absPath+":/container-entrypoint-initdb.d " +
							"-e APP_USER=" + userName +
							" -e APP_USER_PASSWORD=" +password+
							" gvenzl/oracle-xe:21-slim-faststart");

			
			BufferedReader stdInput = new BufferedReader(new
					InputStreamReader(proc.getInputStream()));

			BufferedReader stdError = new BufferedReader(new
					InputStreamReader(proc.getErrorStream()));

			proc.waitFor();
			if(0!= proc.exitValue()){
				System.out.println("Error Occurred Starting oracle contaier:\n");
                        	String s = null;
                        	while ((s = stdError.readLine()) != null) {
                                	System.out.println(s);
                        	}
			}
			//takes over a minute to complete setup and start listening on the port
			Thread.sleep(70000);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
}

static void makeAndStartMuxOracleWorker(HashMap<String,String> cfg) throws IOException, InterruptedException {
		File cdbmakePath = new File("src/test/resources");
		String gopath = System.getenv().get("GOPATH");//basedir.getParent().toString();
		if (cfg == null) {
			cfg = new HashMap<String,String>();
		}

		Runtime.getRuntime().exec("go install github.com/paypal/hera/mux github.com/paypal/hera/worker/oracleworker").waitFor();
		Runtime.getRuntime().exec("killall -ILL mux oracleworker").waitFor();

		ProcessBuilder pb = new ProcessBuilder("./mux", "--name", "hera-test");
		String basedirFPath = gopath+"/srv/";
		File basedirF = new File(basedirFPath);
		basedirF.mkdir();
		pb.directory(basedirF);
		
		File symLinkTarget;
		symLinkTarget = new File(basedirFPath+"/mux");
		if (!symLinkTarget.exists()) {
			Files.createSymbolicLink(
					symLinkTarget.toPath(),
					(new File(gopath+"/bin/" + symLinkTarget.getName())).toPath());
		}
		symLinkTarget = new File(basedirFPath+"/oracleworker");
		if (!symLinkTarget.exists()) {
			Files.createSymbolicLink(
					symLinkTarget.toPath(),
					(new File(gopath+"/bin/" + symLinkTarget.getName())).toPath());
		}

		/**
		 * create cal_client.cdb
		 */
		BufferedWriter writer;
		writer = new BufferedWriter(new FileWriter(basedirFPath+"/cal_client.txt"));
		writer.write("enable_cal=true\n");
		writer.write("cal_handler=file\n");
		writer.write("cal_pool_name=stage_hera\n");
		writer.write("cal_log_file=./cal.log\n");
		writer.write("cal_pool_stack_enable=true\n");
		writer.close();
		String[] cmdCalClient = {
				"/bin/sh",
				"-c",
				"python3 "+ cdbmakePath.getAbsolutePath()+"/cdbmake.py "+ basedirFPath +"/cal_client.txt | cdbmake "+ basedirFPath +"/cal_client.cdb "+ basedirFPath +"/cal_client.cdb.tmp"
		};
		Runtime.getRuntime().exec(cmdCalClient).waitFor();

		/**
		 * create occ.cdb
		 */
		cfg.putIfAbsent("bind_ip", "127.0.0.1");
		cfg.putIfAbsent("bind_port", "11111");
		cfg.putIfAbsent("opscfg.hera.server.max_connections","4");
		cfg.putIfAbsent("database_type","oracle");
		cfg.putIfAbsent("log_level","5");
		cfg.putIfAbsent("enable_client_info_to_worker","true");
		cfg.putIfAbsent("rac_sql_interval","0");
		cfg.putIfAbsent("child.executable","oracleworker");
		writer = new BufferedWriter(new FileWriter(basedirFPath+"/occ.txt"));
		for (String key : cfg.keySet()) {
			writer.write(key + "=" + cfg.get(key) + "\n");
		}
		writer.close();

		writer = new BufferedWriter(new FileWriter(basedirFPath+"/hera.txt"));
		for (String key : cfg.keySet()) {
			writer.write(key + "=" + cfg.get(key) + "\n");
		}
		writer.close();

		String[] cmdOcc = {
				"/bin/sh",
				"-c",
				"python3 "+ cdbmakePath.getAbsolutePath()+"/cdbmake.py "+ basedirFPath +"/occ.txt | cdbmake "+ basedirFPath +"/occ.cdb "+ basedirFPath +"/occ.cdb.tmp"
		};
		Runtime.getRuntime().exec(cmdOcc).waitFor();

		/**
		 * create version.cdb
		 */
		writer = new BufferedWriter(new FileWriter(basedirFPath+"/version.txt"));
		writer.close();
		String[] cmdVersion = {
				"/bin/sh",
				"-c",
				"python3 "+ cdbmakePath.getAbsolutePath()+"/cdbmake.py "+ basedirFPath +"/version.txt | cdbmake "+ basedirFPath +"/version.cdb "+ basedirFPath +"/version.cdb.tmp"
		};
		Runtime.getRuntime().exec(cmdVersion).waitFor();
		Map<String,String> env = pb.environment();
		
		env.put("TWO_TASK","(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1521)) (CONNECT_DATA = (SERVER=DEDICATED)(SERVICE_NAME = poolname)) (FAILOVER_MODE = (TYPE=SESSION)(METHOD=BASIC)(RETRIES=10)(DELAY=5)))");
		env.put("TWO_TASK_0","(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1521)) (CONNECT_DATA = (SERVER=DEDICATED)(SERVICE_NAME = poolname)) (FAILOVER_MODE = (TYPE=SESSION)(METHOD=BASIC)(RETRIES=10)(DELAY=5)))");
		env.put("username","heraapp");
		env.put("password","heraappstg");
		
		o_hera = pb.start();
		BufferedReader stdInput = new BufferedReader(new
                                        InputStreamReader(o_hera.getInputStream()));

                        BufferedReader stdError = new BufferedReader(new
                                        InputStreamReader(o_hera.getErrorStream()));
                        String s = null;
                        while ((s = stdInput.readLine()) != null) {
                                System.out.println(s);
                        }
		String sError = null;
                        while ((s = stdError.readLine()) != null) {
                                System.out.println(sError);
                        }

		boolean didConn = false;
		for (int i = 0; i < 111; i++) {
			Thread.sleep(1222);
			Socket clientSocket = new Socket();
			try {
				clientSocket.connect(new InetSocketAddress("localhost", 11111), 2000);
			} catch (ConnectException e) {
				continue;
			} catch (SocketTimeoutException e) {
				continue;
			}
			clientSocket.close();
			didConn = true;
			break;
		}
		if (!didConn) {
			throw new RuntimeException("hera srv did not come up");
		}
	}

}
