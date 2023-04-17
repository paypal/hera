package com.paypal.hera.jdbc;

import com.paypal.hera.conf.HeraClientConfigHolder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class HeraDatabaseMetadataTest {

    private static HeraConnection dbConn;
    private static String host = System.getProperty("SERVER_URL", "1:127.0.0.1:11111");
    private static String table = System.getProperty("TABLE_NAME", "jdbc_hera_test");
    @Test
    public void test_oracle_sqlEscaping() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException, IOException, InterruptedException {
        Util.makeAndStartHeraMuxInternal(null);
        Properties props = new Properties();
        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
        props.setProperty(HeraClientConfigHolder.SUPPORT_RS_METADATA_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.ENABLE_SHARDING_PROPERTY, "true");
        dbConn = (HeraConnection) DriverManager.getConnection("jdbc:hera:" + host, props);
        testMySqlDBMetadata(props);

        testOracleDBMetadata(props);

        testUnknownDBMetadata(props);
        Util.stopMySqlContainer();

    }

    public static void testMySqlDBMetadata(Properties props) throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        HeraClientConfigHolder config = new HeraClientConfigHolder(props);
        HeraDatabaseMetadata metadata = new HeraDatabaseMetadata(dbConn);

        Assert.assertEquals(String.valueOf(config.getDataSourceType()),
                HeraClientConfigHolder.E_DATASOURCE_TYPE.MYSQL, config.getDataSourceType());


        Assert.assertEquals(String.valueOf(metadata.getDatabaseProductName()),
                HeraClientConfigHolder.E_DATASOURCE_TYPE.MYSQL.name(), metadata.getDatabaseProductName());

        Assert.assertEquals(String.valueOf(metadata.getDatabaseProductVersion()),
                "8.0.31", metadata.getDatabaseProductVersion());

        props.setProperty(HeraClientConfigHolder.DATASOURCE_TYPE, "mysql");
        config = new HeraClientConfigHolder(props);

        Assert.assertEquals(String.valueOf(config.getDataSourceType()),
                HeraClientConfigHolder.E_DATASOURCE_TYPE.MYSQL, config.getDataSourceType());

        dbConn = (HeraConnection) DriverManager.getConnection("jdbc:hera:" + host, props);
        metadata = new HeraDatabaseMetadata(dbConn);

        Assert.assertEquals(String.valueOf(metadata.getDatabaseProductName()),
                HeraClientConfigHolder.E_DATASOURCE_TYPE.MYSQL.name(), metadata.getDatabaseProductName());

    }

    public static void testOracleDBMetadata(Properties props) throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        HeraClientConfigHolder config = new HeraClientConfigHolder(props);
        HeraDatabaseMetadata metadata = new HeraDatabaseMetadata(dbConn);

        props.setProperty(HeraClientConfigHolder.DATASOURCE_TYPE, "oracle");
        config = new HeraClientConfigHolder(props);
        dbConn = (HeraConnection) DriverManager.getConnection("jdbc:hera:" + host, props);
        metadata = new HeraDatabaseMetadata(dbConn);

        Assert.assertEquals(String.valueOf(config.getDataSourceType()),
                HeraClientConfigHolder.E_DATASOURCE_TYPE.ORACLE, config.getDataSourceType());
        // since getDatabaseProductVersion() require db connection,
        // call below will fail as only mysqlworker is running
        HeraDatabaseMetadata finalMetadata = metadata;
        Assert.assertThrows(com.paypal.hera.ex.HeraClientException.class,
                () -> finalMetadata.getDatabaseProductVersion());
    }

    public static void testUnknownDBMetadata(Properties props) throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        HeraClientConfigHolder config = new HeraClientConfigHolder(props);
        HeraDatabaseMetadata metadata = new HeraDatabaseMetadata(dbConn);

        // for unknow db type, it'll default to mysql
        props.setProperty(HeraClientConfigHolder.DATASOURCE_TYPE, "unknown");
        config = new HeraClientConfigHolder(props);

        Assert.assertEquals(String.valueOf(config.getDataSourceType()),
                HeraClientConfigHolder.E_DATASOURCE_TYPE.MYSQL, config.getDataSourceType());

        dbConn = (HeraConnection) DriverManager.getConnection("jdbc:hera:" + host, props);
        metadata = new HeraDatabaseMetadata(dbConn);

        Assert.assertEquals(String.valueOf(metadata.getDatabaseProductName()),
                HeraClientConfigHolder.E_DATASOURCE_TYPE.MYSQL.name(), metadata.getDatabaseProductName());

        Assert.assertEquals(String.valueOf(metadata.getDatabaseProductVersion()),
                "8.0.31", metadata.getDatabaseProductVersion());
    }

}
