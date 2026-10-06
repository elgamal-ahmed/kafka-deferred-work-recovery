package com.elgamal.redecide;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RedecideApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedecideApplication.class, args);
    }
}
