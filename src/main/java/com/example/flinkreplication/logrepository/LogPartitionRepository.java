package com.example.flinkreplication.logrepository;

import com.example.flinkreplication.properties.CleanDatabaseLogsProperties;
import com.example.flinkreplication.properties.LogsDatabaseProperties;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Slf4j
@Repository
@RequiredArgsConstructor
public class LogPartitionRepository {

    @PersistenceContext(unitName = "logs")
    private EntityManager entityManager;

    private final CleanDatabaseLogsProperties cleanDatabaseLogs;
    private final LogsDatabaseProperties logsDatabaseProperties;

    @Transactional("logsTransactionManager")
    public void createTodayPartition() {
        if (!logsDatabaseProperties.isEnabled())
            return;

        LocalDate currentDate = LocalDate.now();
        String partitionName = String.format("dashboard_kafka_log_%s", currentDate.format(DateTimeFormatter.ofPattern("yyyy_MM_dd")));
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s PARTITION OF dashboard_kafka_log FOR VALUES FROM ('%s 00:00:00') TO ('%s 00:00:00')",
                partitionName,
                currentDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                currentDate.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Transactional("logsTransactionManager")
    public void dropOldPartitions() {
        if (!logsDatabaseProperties.isEnabled())
            return;

        LocalDate dateToDelete = LocalDate.now().minusYears(cleanDatabaseLogs.getCleanPeriod());
        String partitionName = String.format("dashboard_kafka_log_%d_%02d_%02d", dateToDelete.getYear(), dateToDelete.getMonthValue(), dateToDelete.getDayOfMonth());
        String sql = String.format("DROP TABLE IF EXISTS %s", partitionName);
        entityManager.createNativeQuery(sql).executeUpdate();
    }
}
