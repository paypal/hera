package com.paypal.hera.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Map;

public class UtilHeraBox {

    private static String HERA_OSS = "hera-oss";
    private static String HERA_MOCK = "hera-mock";
    private static int MOCK_PORT = 13916;
    private static int HERA_BOX_PORT = 10101;
    private static int HERA_MYSQL_PORT = 3306;
    private static String HOSTNAME = "127.0.0.1";
    private static String GO_PATH = System.getenv().get("GOPATH");
    static final Logger LOGGER = LoggerFactory.getLogger(UtilHeraBox.class);

    static boolean checkImageBuilt(String imageName, String version){
        for(int i = 0; i < 200; i++){
            try{
                Thread.sleep(1222);
                String cmd = "docker image inspect " + imageName + ":" + version;
                ProcessBuilder builder = new ProcessBuilder("bash", "-c", cmd);
                builder.redirectErrorStream(true);
                Process process = builder.start();
                InputStream is = process.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                if (!reader.readLine().equals("[]")){
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
    public static void buildHeraBoxImageWithMock() throws IOException, InterruptedException {
        if(!checkImageBuilt(HERA_OSS, "latest") || !checkImageBuilt(HERA_MOCK, "latest")){
            ProcessBuilder pb = new ProcessBuilder( "bash", "-c", "./build.sh");
            pb.redirectErrorStream(true);
            Map<String, String> env = pb.environment();
            env.put("BUILD_SAMPLE_APP", "false");
            String currentPath = System.getProperty("user.dir");
            File curr = new File(currentPath);
            String base = curr.getParentFile().getParentFile().getParent();
            String dir = base + "/docker_build_and_run";
            pb.directory(new File(dir));
            Process process = pb.start();
            printOutput(process);
            if(checkImageBuilt(HERA_OSS, "latest") && checkImageBuilt(HERA_MOCK, "latest")){
                return;
            }
            else{
                throw new RuntimeException("hera-mock and/or hera-oss image not built");
            }
        }

    }

    static boolean checkConnectionUp(String hostname, int port) {
        boolean didConn = false;
        for (int i = 0; i < 10; i++) {
            Socket clientSocket = new Socket();
            try {
                Thread.sleep(1222);
                clientSocket.connect(new InetSocketAddress(hostname, port), 2000);
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

    static void printOutput(Process process) throws IOException {
        InputStream is = process.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while ((line = reader.readLine()) != null)
            LOGGER.debug(line);
    }

     static void startHeraBox() throws IOException {
        if (checkConnectionUp(HOSTNAME, MOCK_PORT) && checkConnectionUp(HOSTNAME, HERA_BOX_PORT) &&
                checkConnectionUp(HOSTNAME, HERA_MYSQL_PORT)) return;
        ProcessBuilder pb = new ProcessBuilder( "bash", "-c", "./start.sh");
        pb.redirectErrorStream(true);
        Map<String, String> env = pb.environment();
        env.put("START_HERA_SAMPLE_APP", "false");
        env.put("HERA_DISABLE_SSL", "true");
        String currentPath = System.getProperty("user.dir");
        File curr = new File(currentPath);
        String base = curr.getParentFile().getParentFile().getParent();
        String dir = base + "/docker_build_and_run";
        pb.directory(new File(dir));
        Process process = pb.start();
        printOutput(process);

        if (!checkConnectionUp(HOSTNAME, MOCK_PORT) && !checkConnectionUp(HOSTNAME, HERA_BOX_PORT) &&
                !checkConnectionUp(HOSTNAME, HERA_MYSQL_PORT)){
            throw new RuntimeException("hera docker containers did not come up");
        }

    }

    public static void makeAndStartHeraBox() throws IOException, InterruptedException {
        buildHeraBoxImageWithMock();
        startHeraBox();
    }

    public static void stopHeraBox() throws IOException {
        ProcessBuilder pb = new ProcessBuilder( "bash", "-c", "./stop.sh");
        pb.redirectErrorStream(true);
        String currentPath = System.getProperty("user.dir");
        File curr = new File(currentPath);
        String base = curr.getParentFile().getParentFile().getParent();
        String dir = base + "/docker_build_and_run";
        pb.directory(new File(dir));
        Process process = pb.start();
        printOutput(process);
    }
}
