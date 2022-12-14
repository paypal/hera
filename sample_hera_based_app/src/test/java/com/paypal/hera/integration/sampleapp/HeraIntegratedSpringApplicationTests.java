package com.paypal.hera.integration.sampleapp;

import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.conn.HeraTLSConnection;
import com.paypal.hera.conn.HeraTLSConnectionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.Assert;

import java.sql.*;
import java.util.Properties;

@SpringBootTest
class HeraIntegratedSpringApplicationTests {

    @Test
    void testConnection() throws SQLException {
        String message = "";
        try {
            Boolean disableSSL = false;
            String sslEnv = System.getenv("HERA_DISABLE_SSL");
            if (sslEnv != null && sslEnv.equalsIgnoreCase("true"))
                disableSSL = true;
            String host = "1:127.0.0.1:10102";
            Properties props = new Properties();
            System.setProperty("javax.net.ssl.trustStore", "src/main/resources/cert/hera.jks");
            System.setProperty("javax.net.ssl.trustStorePassword", "herabox");

            if(!disableSSL)
                props.setProperty(HeraClientConfigHolder.CONNECTION_FACTORY_PROPERTY, HeraTLSConnectionFactory.class.getCanonicalName());
            props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
            Connection dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);

            // do standard JDBC
            PreparedStatement pst = dbConn.prepareStatement("select * from employee");
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                message += "testRead : " + rs.getString(1);
            }
        }catch (Exception e) {
            e.printStackTrace();
            message += "Exception: " + e.getMessage();
            System.out.println(message);
            throw e;
        }
        System.out.println(message);
    }

}
