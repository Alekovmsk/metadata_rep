package com.example.flinkreplication;
// Главный класс
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FlinkReplicationApplication {
    public static void main(String[] args) {
        SpringApplication.run(FlinkReplicationApplication.class, args);
    }
}