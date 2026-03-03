package com.sats;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan("com.sats.config")
@EnableScheduling
public class SatsApplication {

    public static void main(String[] args) {
        SpringApplication.run(SatsApplication.class, args);
    }
}
