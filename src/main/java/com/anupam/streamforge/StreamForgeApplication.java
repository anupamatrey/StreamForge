package com.anupam.streamforge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class StreamForgeApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamForgeApplication.class, args);
    }

}
