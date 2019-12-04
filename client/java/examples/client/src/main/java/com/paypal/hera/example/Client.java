package com.paypal.hera.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class Client {

	public static void main(String[] args) throws ClassNotFoundException, SQLException{
		String host = System.getProperty("SERVER_URL", "1:127.0.0.1:11111"); 
		Properties props = new Properties();
		props.setProperty("foo", "bar");
		Connection dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);

		PreparedStatement pst = dbConn.prepareStatement("SELECT 'foo' from dual");
		ResultSet rs = pst.executeQuery();
		if (rs.next()) {
			System.out.println("Result: " + rs.getString(1));
		}
	}
}
