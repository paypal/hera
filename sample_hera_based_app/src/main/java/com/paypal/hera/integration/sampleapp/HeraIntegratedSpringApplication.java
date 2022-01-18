package com.paypal.hera.integration.sampleapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.http.HttpMessageConvertersAutoConfiguration;

@SpringBootApplication (exclude = {HttpMessageConvertersAutoConfiguration.class},
        scanBasePackages = "com.paypal.hera.integration")
public class HeraIntegratedSpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(HeraIntegratedSpringApplication.class, args);
    }

}
