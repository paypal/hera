package com.paypal.hera.integration.sampleapp;

import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.conn.HeraTLSConnectionFactory;
import com.paypal.hera.jdbc.HeraDriver;
import com.paypal.integ.odak.OdakConfigManager;
import com.paypal.integ.odak.OdakDataSource;
import com.paypal.integ.odak.PoolConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
    public static DataSource heraDataSourceWithNoConnPool() {
        boolean disableSSL = false;
        String sslEnv = System.getenv("HERA_DISABLE_SSL");
        if (sslEnv != null && sslEnv.equalsIgnoreCase("true"))
            disableSSL = true;
        String host = "1:127.0.0.1:10102";
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(HeraDriver.class.getName());
        dataSource.setUrl("jdbc:hera:" + host);

        Properties props = new Properties();
        System.setProperty("javax.net.ssl.trustStore", "src/main/resources/cert/hera.jks");
        System.setProperty("avax.net.ssl.trustStorePassword", "herabox");

        if(!disableSSL)
            props.setProperty(HeraClientConfigHolder.CONNECTION_FACTORY_PROPERTY, HeraTLSConnectionFactory.class.getCanonicalName());
        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
        dataSource.setConnectionProperties(props);

        return dataSource;
    }

    @Bean
    public static DataSource heraDataSourceWithHikari() {
        boolean disableSSL = false;
        String sslEnv = System.getenv("HERA_DISABLE_SSL");
        if (sslEnv != null && sslEnv.equalsIgnoreCase("true"))
            disableSSL = true;
        String host = "1:127.0.0.1:10102";

        HikariConfig config = new HikariConfig();
        config.setDriverClassName(HeraDriver.class.getName());
        config.setJdbcUrl("jdbc:hera:" + host);
        config.setMaximumPoolSize(1);

        Properties props = new Properties();
        System.setProperty("javax.net.ssl.trustStore", "src/main/resources/cert/hera.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "herabox");

        if(!disableSSL)
            props.setProperty(HeraClientConfigHolder.CONNECTION_FACTORY_PROPERTY, HeraTLSConnectionFactory.class.getCanonicalName());
        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
        config.setDataSourceProperties(props);

        return new HikariDataSource(config);
    }

    @Bean("heraDataSourceWithOpenDAK")
    public static DataSource heraDataSourceWithOpenDAK() {
        boolean enableSSL = true;
        String sslEnv = System.getenv("HERA_DISABLE_SSL");
        if (sslEnv != null && sslEnv.equalsIgnoreCase("true"))
            enableSSL = false;
        String host = "1:127.0.0.1:10102";

        PoolConfig poolConfig = new PoolConfig();
        poolConfig.setDriverClazz(HeraDriver.class.getName());
        poolConfig.setUrl("jdbc:hera:" + host);
        poolConfig.setHost(host);
        poolConfig.setUseSSLConnection(enableSSL); // by default SSL is enabled


        Properties props = new Properties();
        System.setProperty("javax.net.ssl.trustStore", "src/main/resources/cert/hera.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "herabox");

        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
        poolConfig.setConnectionProperties(props);

        return new OdakDataSource(poolConfig);
    }
}
