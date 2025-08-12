package com.example.flinkreplication.config;

import jakarta.persistence.EntityManagerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableJpaRepositories(
        basePackages = "com.example.flinkreplication.logrepository",
        entityManagerFactoryRef = "logsEntityManagerFactory",
        transactionManagerRef = "logsTransactionManager"
)
public class LogsDbConfig {

    @Bean
    @ConfigurationProperties(prefix = "logs-database.datasource")
    public DataSource logsDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean logsEntityManagerFactory(
            @Qualifier("logsDataSource") DataSource dataSource,
            EntityManagerFactoryBuilder builder) {
        return builder
                .dataSource(dataSource)
                .packages("com.example.flinkreplication.model")
                .persistenceUnit("logs")
                .build();
    }

    @Bean
    public PlatformTransactionManager logsTransactionManager(
            @Qualifier("logsEntityManagerFactory") EntityManagerFactory logsEntityManagerFactory) {
        return new JpaTransactionManager(logsEntityManagerFactory);
    }
}
