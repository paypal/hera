package com.paypal.hera.example;

import java.sql.*;
import java.util.Properties;

public class Client {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String host = System.getProperty("SERVER_URL", "1:127.0.0.1:11111");
        Properties props = new Properties();
        props.setProperty("foo", "bar");
        props.setProperty("hera.datasource.type", "postgres");
        Connection dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);
        DatabaseMetaData metaData = dbConn.getMetaData();
        String dbName = metaData.getDatabaseProductName();

        PreparedStatement pst = null;
        if (dbName.equalsIgnoreCase("postgres")) {
            pst = dbConn.prepareStatement("SELECT 1");
        } else if (dbName.equalsIgnoreCase("mysql") ||
                dbName.equalsIgnoreCase("oracle")) { // oracle or mysql
            pst = dbConn.prepareStatement("SELECT 'foo' from dual");
        }
        if (pst != null) {
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                System.out.println("Result: " + rs.getString(1));
            }
        } else {
            System.out.println("Supported DB types are mysql, oracle, postgres. Exiting...");
        }

        String dbVersion = metaData.getDatabaseProductVersion();
        System.out.println(dbName + " DB version is:" + dbVersion);

    }
}
