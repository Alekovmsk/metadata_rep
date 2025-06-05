package com.example.flinkreplication.config;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class PostgresConfig {

    @Value("${target.db.url}")
    private String targetDbUrl;

    @Value("${target.db.username}")
    private String targetDbUsername;

    @Value("${target.db.password}")
    private String targetDbPassword;

    @Value("${source.db.prefix}")
    private String sourceDbPrefix;

    @Value("${source.db.count}")
    private int sourceDbCount;

    @Value("${source.db.url.pattern}")
    private String sourceDbUrlPattern;

    @Value("${source.db.username}")
    private String sourceDbUsername;

    @Value("${source.db.password}")
    private String sourceDbPassword;

    @Value("${db.connection.max.total:20}")
    private int maxTotalConnections;

    @Value("${db.connection.max.idle:10}")
    private int maxIdleConnections;

    @Value("${db.connection.min.idle:5}")
    private int minIdleConnections;

    @Value("${db.connection.max.wait.millis:30000}")
    private long maxWaitMillis;

    @Bean
    public DataSource targetDataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(targetDbUrl);
        dataSource.setUsername(targetDbUsername);
        dataSource.setPassword(targetDbPassword);
        configureConnectionPool(dataSource);
        return dataSource;
    }

    @Bean
    public JdbcTemplate targetJdbcTemplate(DataSource targetDataSource) {
        return new JdbcTemplate(targetDataSource);
    }

    @Bean
    public Map<String, DataSource> sourceDataSources() {
        Map<String, DataSource> dataSources = new HashMap<>();

        for (int i = 1; i <= sourceDbCount; i++) {
            String dbName = sourceDbPrefix + i;
            String dbUrl = sourceDbUrlPattern.replace("{db}", dbName);

            BasicDataSource dataSource = new BasicDataSource();
            dataSource.setUrl(dbUrl);
            dataSource.setUsername(sourceDbUsername);
            dataSource.setPassword(sourceDbPassword);
            configureConnectionPool(dataSource);

            dataSources.put(dbName, dataSource);
        }

        return dataSources;
    }

    @Bean
    public Map<String, JdbcTemplate> sourceJdbcTemplates(Map<String, DataSource> sourceDataSources) {
        Map<String, JdbcTemplate> jdbcTemplates = new HashMap<>();
        sourceDataSources.forEach((dbName, dataSource) -> {
            jdbcTemplates.put(dbName, new JdbcTemplate(dataSource));
        });
        return jdbcTemplates;
    }

    private void configureConnectionPool(BasicDataSource dataSource) {
        dataSource.setMaxTotal(maxTotalConnections);
        dataSource.setMaxIdle(maxIdleConnections);
        dataSource.setMinIdle(minIdleConnections);
        dataSource.setMaxWaitMillis(maxWaitMillis);
        dataSource.setTestOnBorrow(true);
        dataSource.setValidationQuery("SELECT 1");
    }

    // Геттеры и сеттеры для всех полей

    public String getTargetDbUrl() {
        return targetDbUrl;
    }

    public void setTargetDbUrl(String targetDbUrl) {
        this.targetDbUrl = targetDbUrl;
    }

    public String getTargetDbUsername() {
        return targetDbUsername;
    }

    public void setTargetDbUsername(String targetDbUsername) {
        this.targetDbUsername = targetDbUsername;
    }

    public String getTargetDbPassword() {
        return targetDbPassword;
    }

    public void setTargetDbPassword(String targetDbPassword) {
        this.targetDbPassword = targetDbPassword;
    }

    public String getSourceDbPrefix() {
        return sourceDbPrefix;
    }

    public void setSourceDbPrefix(String sourceDbPrefix) {
        this.sourceDbPrefix = sourceDbPrefix;
    }

    public int getSourceDbCount() {
        return sourceDbCount;
    }

    public void setSourceDbCount(int sourceDbCount) {
        this.sourceDbCount = sourceDbCount;
    }

    public String getSourceDbUrlPattern() {
        return sourceDbUrlPattern;
    }

    public void setSourceDbUrlPattern(String sourceDbUrlPattern) {
        this.sourceDbUrlPattern = sourceDbUrlPattern;
    }

    public String getSourceDbUsername() {
        return sourceDbUsername;
    }

    public void setSourceDbUsername(String sourceDbUsername) {
        this.sourceDbUsername = sourceDbUsername;
    }

    public String getSourceDbPassword() {
        return sourceDbPassword;
    }

    public void setSourceDbPassword(String sourceDbPassword) {
        this.sourceDbPassword = sourceDbPassword;
    }

    public int getMaxTotalConnections() {
        return maxTotalConnections;
    }

    public void setMaxTotalConnections(int maxTotalConnections) {
        this.maxTotalConnections = maxTotalConnections;
    }

    public int getMaxIdleConnections() {
        return maxIdleConnections;
    }

    public void setMaxIdleConnections(int maxIdleConnections) {
        this.maxIdleConnections = maxIdleConnections;
    }

    public int getMinIdleConnections() {
        return minIdleConnections;
    }

    public void setMinIdleConnections(int minIdleConnections) {
        this.minIdleConnections = minIdleConnections;
    }

    public long getMaxWaitMillis() {
        return maxWaitMillis;
    }

    public void setMaxWaitMillis(long maxWaitMillis) {
        this.maxWaitMillis = maxWaitMillis;
    }
}