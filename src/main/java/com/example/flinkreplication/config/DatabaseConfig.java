package com.example.flinkreplication.config;
//Конфиг целевой БД (HikariCP)
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
@ConfigurationProperties(prefix = "spring.datasource.target")
public class DatabaseConfig {
    private String url;
    private String username;
    private String password;
    private int maximumPoolSize;
    private long connectionTimeout;

    // Геттеры и сеттеры

    @Bean
    public DataSource targetDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maximumPoolSize);
        config.setConnectionTimeout(connectionTimeout);
        return new HikariDataSource(config);
    }
}