package com.paypal.hera.integration.sampleapp;

import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.conn.HeraTLSConnectionFactory;
import com.paypal.hera.jdbc.HeraDriver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
@ComponentScan("com.paypal.hera.integration")
public class SpringJDBCconfig {
    @Bean
    public DataSource mysqlDataSource() {
        Boolean disableSSL = false;
        String sslEnv = System.getenv("HERA_DISABLE_SSL");
        if (sslEnv != null && sslEnv.equalsIgnoreCase("true"))
            disableSSL = true;
        String host = "1:127.0.0.1:10102";
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(HeraDriver.class.getName());
        dataSource.setUrl("jdbc:hera:" + host);

        Properties props = new Properties();
        System.setProperty("javax.net.ssl.trustStore", "src/main/resources/cert/hera.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "herabox");

        if(!disableSSL)
            props.setProperty(HeraClientConfigHolder.CONNECTION_FACTORY_PROPERTY, HeraTLSConnectionFactory.class.getCanonicalName());
        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
        dataSource.setConnectionProperties(props);

        return dataSource;
    }
}
