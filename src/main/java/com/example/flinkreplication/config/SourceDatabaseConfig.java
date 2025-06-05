package com.example.flinkreplication.config;
//Конфиг источников (DBCP2)
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "replication")
public class SourceDatabaseConfig {
    private List<SourceDatabaseProperties> sourceDatabases;

    public static class SourceDatabaseProperties {
        private String name;
        private String url;
        private String username;
        private String password;
        private boolean active;
        private ConnectionPool pool;

        // Геттеры и сеттеры
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public ConnectionPool getPool() {
            return pool;
        }

        public void setPool(ConnectionPool pool) {
            this.pool = pool;
        }
    }

    public static class ConnectionPool {
        private int maxTotal;
        private int maxIdle;
        private int minIdle;
        private long maxWaitMillis;

        // Геттеры и сеттеры
        public int getMaxTotal() {
            return maxTotal;
        }

        public void setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
        }

        public int getMaxIdle() {
            return maxIdle;
        }

        public void setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
        }

        public int getMinIdle() {
            return minIdle;
        }

        public void setMinIdle(int minIdle) {
            this.minIdle = minIdle;
        }

        public long getMaxWaitMillis() {
            return maxWaitMillis;
        }

        public void setMaxWaitMillis(long maxWaitMillis) {
            this.maxWaitMillis = maxWaitMillis;
        }
    }

    // Геттеры и сеттеры для основного класса
    public List<SourceDatabaseProperties> getSourceDatabases() {
        return sourceDatabases;
    }

    public void setSourceDatabases(List<SourceDatabaseProperties> sourceDatabases) {
        this.sourceDatabases = sourceDatabases;
    }

    @Bean
    public Map<String, DataSource> sourceDataSources() {
        Map<String, DataSource> dataSources = new HashMap<>();
        if (sourceDatabases != null) {
            for (SourceDatabaseProperties db : sourceDatabases) {
                if (db.isActive()) {
                    BasicDataSource ds = new BasicDataSource();
                    ds.setUrl(db.getUrl());
                    ds.setUsername(db.getUsername());
                    ds.setPassword(db.getPassword());
                    ConnectionPool pool = db.getPool();
                    if (pool != null) {
                        ds.setMaxTotal(pool.getMaxTotal());
                        ds.setMaxIdle(pool.getMaxIdle());
                        ds.setMinIdle(pool.getMinIdle());
                        ds.setMaxWaitMillis(pool.getMaxWaitMillis());
                    }
                    dataSources.put(db.getName(), ds);
                }
            }
        }
        return dataSources;
    }

    @Bean
    public Map<String, JdbcTemplate> sourceJdbcTemplates(Map<String, DataSource> sourceDataSources) {
        Map<String, JdbcTemplate> templates = new HashMap<>();
        sourceDataSources.forEach((name, ds) -> templates.put(name, new JdbcTemplate(ds)));
        return templates;
    }
}