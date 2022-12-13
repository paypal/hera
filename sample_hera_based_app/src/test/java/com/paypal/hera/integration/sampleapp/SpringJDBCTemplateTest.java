package com.paypal.hera.integration.sampleapp;

import com.paypal.hera.integration.sampleapp.dataaccess.EmployeeRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class SpringJDBCTemplateTest {
    @Autowired
    EmployeeRepository employeeRepository;

    @Test
    public void springJdbcTemplate() {
        StringBuilder message = new StringBuilder();
        try {
            message.append("Spring JDBC Template Test result: ").append(employeeRepository.findById(1, false)).append("\n");
        }catch (Exception e) {
            message.append("Exception: ").append(e.getMessage()).append("\n");
        }
        System.out.println(message);
    }

    @Configuration
    @ComponentScan("com.paypal.hera.integration.sampleapp")
    public static class SpringConfig {

    }
}
