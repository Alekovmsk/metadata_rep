package com.example.flinkreplication.properties;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
@Configuration
@ConfigurationProperties(prefix = "logs-database")
@RequiredArgsConstructor
@Data
public class LogsDatabaseProperties {
    private boolean enabled;
}

