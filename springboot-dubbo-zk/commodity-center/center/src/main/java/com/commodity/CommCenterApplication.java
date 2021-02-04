package com.commodity;

import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableDubbo(scanBasePackages="com.commodity")
public class CommCenterApplication {
    public static void main(String[] args) {
        SpringApplication.run(CommCenterApplication.class, args);
    }
}
