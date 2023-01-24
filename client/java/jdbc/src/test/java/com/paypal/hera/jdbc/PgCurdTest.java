package com.paypal.hera.jdbc;

import com.paypal.hera.conf.HeraClientConfigHolder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class PgCurdTest {
   static String host = System.getProperty("SERVER_URL", "1:127.0.0.1:11111");
    @BeforeClass
    public static void setup() throws SQLException {
        UtilPostgres.makeAndStartHeraMux(null);

        reset();
        initSetup();
    }


    @AfterClass
    public static void teardown() throws IOException, InterruptedException {
//        UtilPostgres.stopPostgresContainer();
    }


    @Test
    public void testSimpleInsert() throws SQLException {

        Properties props = new Properties();
        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
        props.setProperty(HeraClientConfigHolder.SUPPORT_RS_METADATA_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.ENABLE_SHARDING_PROPERTY, "true");
        Connection dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);

        PreparedStatement preparedStatement =	dbConn.prepareStatement("INSERT INTO herapgcurd (ID,INT_VAL,STR_VAL ) values (?, ?, ?)");
        preparedStatement.setInt(1, 123);
        preparedStatement.setInt(2, 123);
        preparedStatement.setString(3, "heracurd");

       int count = preparedStatement.executeUpdate();
       System.out.println("count==>"+ count);
       dbConn.commit();

        PreparedStatement preparedStatement2 =	dbConn.prepareStatement("SELECT ID,INT_VAL,STR_VAL FROM herapgcurd where ID = ?");
        preparedStatement2.setInt(1, 123);

       ResultSet rs = preparedStatement2.executeQuery();

       if (rs.next()){
           System.out.println( rs.getString("ID"));
       }else{
           Assert.fail("Inserted record not found");
       }

       rs.close();
       preparedStatement2.close();
       preparedStatement.close();
       dbConn.close();

    }



    public static void initSetup() throws SQLException {

        HeraClientConfigHolder.clear();
        Properties props = new Properties();
        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
        props.setProperty(HeraClientConfigHolder.SUPPORT_RS_METADATA_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.ENABLE_SHARDING_PROPERTY, "true");
        Connection dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);

        System.out.println( "dbConn==>"+ dbConn);


        PreparedStatement preparedStatement =	dbConn.prepareStatement("SELECT table_name FROM information_schema.tables where table_name = 'herapgcurd'");

        ResultSet rs =	preparedStatement.executeQuery();
        if (rs.next()){
            System.out.println( rs.getString("table_name") );
        }else{
            PreparedStatement preparedStatement2 =   dbConn.prepareStatement("create table herapgcurd (ID INTEGER primary key ,INT_VAL INTEGER,STR_VAL VARCHAR(256),"
                    +"CHAR_VAL CHAR(2), RAW_VAL TEXT, date_val DATE, time_val TIME,"
                    +" timestamp_val TIMESTAMP, timestamp_tz_val TIMESTAMPTZ, time_tz_val TIMETZ)");
            preparedStatement2.execute();
            dbConn.commit();
        }

        rs.close();
        preparedStatement.close();
        dbConn.close();

    }

    public static void reset() throws SQLException {
        HeraClientConfigHolder.clear();
        Properties props = new Properties();
        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
        props.setProperty(HeraClientConfigHolder.SUPPORT_RS_METADATA_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.ENABLE_SHARDING_PROPERTY, "true");
        Connection dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);

        System.out.println( "dbConn==>"+ dbConn);
        PreparedStatement preparedStatement2 =   dbConn.prepareStatement("DROP TABLE IF EXISTS herapgcurd");
        preparedStatement2.execute();
        dbConn.commit();

        preparedStatement2.close();
        dbConn.close();
    }

}
