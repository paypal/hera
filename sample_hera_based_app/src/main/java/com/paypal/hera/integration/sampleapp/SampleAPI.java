package com.paypal.hera.integration.sampleapp;

import com.paypal.dal.heramockclient.HERAMockException;
import com.paypal.dal.heramockclient.HERAMockHelper;
import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.conn.HeraTLSConnectionFactory;
import com.paypal.hera.integration.sampleapp.dataaccess.EmployeeRepository;
import com.paypal.hera.integration.sampleapp.dataaccess.entity.EmployeeEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import java.util.Properties;

@RestController
public class SampleAPI {
    @Autowired
    EmployeeRepository employeeRepository;

    private String basicTest() throws SQLException {
        Boolean disableSSL = false;
        String sslEnv = System.getenv("HERA_DISABLE_SSL");
        if (sslEnv != null && sslEnv.equalsIgnoreCase("true"))
            disableSSL = true;
        StringBuilder message = new StringBuilder();
        message.append("Basic Test Result: ");
        String host = "1:127.0.0.1:10101";
        Properties props = new Properties();
        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
        if(!disableSSL)
            props.setProperty(HeraClientConfigHolder.CONNECTION_FACTORY_PROPERTY, HeraTLSConnectionFactory.class.getCanonicalName());

        System.setProperty("javax.net.ssl.trustStore", "src/main/resources/cert/hera.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "herabox");

        // Override any default property
        Connection dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);

        // do standard JDBC
        PreparedStatement pst = dbConn.prepareStatement("select 'abc' from dual");
        ResultSet rs = pst.executeQuery();
        if (rs.next()) {
            message.append("testRead : ").append(rs.getString(1));
        }
        return message.toString();
    }

    private String mockTest() throws HERAMockException {
        EmployeeEntity employee = new EmployeeEntity();
        employee.setId(1);
        employee.setName("mockedResponse");
        employee.setVersion(100);
        HERAMockHelper.addMock("Employee.FIND_BY_ID", employee);
        return employeeRepository.findById(1).toString();
    }

    @GetMapping("/basicTest")
    public String simple() {
        StringBuilder message = new StringBuilder();
        try {
            message.append("Basic Test: ").append(basicTest()).append("\n");
            message.append(basicTest()).append("\n");
        }catch (Exception e) {
            message.append("Exception: ").append(e.getMessage()).append("\n");
        }
        return message.toString();
    }

    @GetMapping("/basicMockTest")
    public String basicMockTest() {
        StringBuilder message = new StringBuilder();
        try {
            message.append("Mock Test: ").append(mockTest()).append("\n");
            message.append(basicTest()).append("\n");
        }catch (Exception e) {
            message.append("Exception: ").append(e.getMessage()).append("\n");
        }
        return message.toString();
    }

    @GetMapping("/springJdbcTemplate")
    public String springJdbcTemplate() {
        StringBuilder message = new StringBuilder();
        try {
            message.append("Spring JDBC Template Test result: ").append(employeeRepository.findById(1)).append("\n");
        }catch (Exception e) {
            message.append("Exception: ").append(e.getMessage()).append("\n");
        }
        return message.toString();
    }
}
