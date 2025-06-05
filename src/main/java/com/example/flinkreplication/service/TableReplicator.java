package com.example.flinkreplication.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class TableReplicator {
    private static final Logger logger = LoggerFactory.getLogger(TableReplicator.class);

    private final ReplicationService replicationService;
    private final ApplicationContext applicationContext;
    private int retryCount = 0;

    @Value("${replication.max-retries:3}")
    private int maxRetries;

    @Value("${replication.retry-delay-ms:5000}")
    private long retryDelayMs;

    @Autowired
    public TableReplicator(ReplicationService replicationService,
                           ApplicationContext applicationContext) {
        this.replicationService = replicationService;
        this.applicationContext = applicationContext;
    }

    @Scheduled(fixedDelayString = "${replication.interval:60000}")
    public void replicateTables() {
        try {
            replicationService.startReplication();
            retryCount = 0;
        } catch (Exception e) {
            handleReplicationError(e);
        }
    }

    private void handleReplicationError(Exception e) {
        retryCount++;
        logger.error("Replication failed (attempt {}/{}): {}",
                retryCount, maxRetries, e.getMessage());

        if (retryCount >= maxRetries) {
            logger.error("Max retries reached. Restarting application...");
            restartApplication();
        } else {
            waitAndRetry();
        }
    }

    private void waitAndRetry() {
        try {
            Thread.sleep(retryDelayMs);
            replicateTables();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private void restartApplication() {
        // Безопасный перезапуск через Spring Boot
        int exitCode = SpringApplication.exit(applicationContext, () -> 1);
        System.exit(exitCode);
    }

    public int getRetryCount() {
        return retryCount;
    }
}