package com.demo.testing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.retry.annotation.EnableRetry;

@SpringBootApplication
@EnableConfigurationProperties
@EnableRetry
@ComponentScan(basePackages = {
"com.demo.config",
"com.demo.controller" , 
"com.demo.dto",
"com.demo.exception",
"com.demo.service",
"com.demo.testing",
"com.demo.util",
"com.demo.kafka"

})
public class TestingApplication {

	public static void main(String[] args) {
		SpringApplication.run(TestingApplication.class, args);
	}

}
//web socket + stomp code 