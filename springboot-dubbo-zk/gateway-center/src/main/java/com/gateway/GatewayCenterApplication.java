package com.gateway;

import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableDubbo(scanBasePackages="com.gateway")
public class GatewayCenterApplication {
    public static void main(String[] args) {
        SpringApplication.run(GatewayCenterApplication.class, args);
    }
}
