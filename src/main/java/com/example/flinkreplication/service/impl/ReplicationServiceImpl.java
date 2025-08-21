package com.example.flinkreplication.service.impl;

import com.example.flinkreplication.enums.ReplicationJobStatus;
import com.example.flinkreplication.dto.SourceDbConnections;
import com.example.flinkreplication.flink.MetadataExtractorByDatabase;
import com.example.flinkreplication.model.ReplicationJob;
import com.example.flinkreplication.properties.*;
import com.example.flinkreplication.repository.ReplicationJobRepository;
import com.example.flinkreplication.service.DbSourcesService;
import com.example.flinkreplication.service.ReplicationService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplicationServiceImpl implements ReplicationService {

    private final ExecutionEnvironment env;
    private final DataSourceProperties dataSourceProperties;
    private final PostgresTablesProperties postgresProperties;
    private final FlinkProperty flinkProperty;
    private final ReplicationJobRepository jobRepository;
    private final DbSourcesService dbSourcesService;

    @Scheduled(fixedDelay = 60000)
    @Transactional
    public void scheduledReplication() {
        log.info("Запуск репликации...");
        try {
            startReplication();
        } catch (Exception e) {
            log.error("Ошибка при выполнении репликации", e);
        }
    }

    public void startReplication() {
        env.setParallelism(flinkProperty.getParallelism() > 0 ? flinkProperty.getParallelism() : 1);
        // список которые поидут на репликацию
        List<SourceDbConnections> sourceDbCForReplication = new ArrayList<>();
        // список которые не были найденны в источниках для репликации
        List<SourceDbConnections> sourceDbNotFound = new ArrayList<>();

        Map<String, SourceDbConnections> dbConnectionsMap = dbSourcesService.getDbConnections()
                .stream()
                .collect(Collectors.toMap(
                        SourceDbConnections::getName,
                        Function.identity()
                ));

        List<ReplicationJob> pendingJobs = jobRepository.findByStatusOrderByCreatedAt(ReplicationJobStatus.PENDING);

        pendingJobs
                .forEach(job -> {
                    if (dbConnectionsMap.containsKey(job.getDbName())) {
                        sourceDbCForReplication.add(dbConnectionsMap.get(job.getDbName()));
                        job.setStatus(ReplicationJobStatus.RUNNING);
                    } else {
                        sourceDbNotFound.add(dbConnectionsMap.get(job.getDbName()));
                        job.setStatus(ReplicationJobStatus.FAILED);
                    }
                });

        // список системных таблиц для репликации
        List<String> tablesToReplicate = postgresProperties.getTables();

        if (!sourceDbCForReplication.isEmpty()) {
            log.info(String.format("Полученно из очереди %s источников", sourceDbCForReplication.size()));
        }
        if (!sourceDbNotFound.isEmpty()) {
            log.info(String.format("Не найденно данных по %s источникам для репликации", sourceDbCForReplication.size()));
        }
        if (tablesToReplicate.isEmpty()) {
            log.info("Не получен список системных таблиц");
        }

        jobRepository.saveAll(pendingJobs);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Timestamp.class)
        );


        if (!sourceDbCForReplication.isEmpty()) {
            run(rowTypeInfo, tablesToReplicate, sourceDbCForReplication);

            log.info(String.format(
                    "Репликация завершена успешно для источников : %s",
                    sourceDbCForReplication.stream()
                            .map(SourceDbConnections::getName)
                            .collect(Collectors.joining(", "))
            ));
        }
    }

    public void run(RowTypeInfo rowTypeInfo, List<String> tablesToReplicate, List<SourceDbConnections> activeSources) {

        var rowsDS = env.fromCollection(activeSources)
                .flatMap(new MetadataExtractorByDatabase(tablesToReplicate, flinkProperty.getMaxRetries(), flinkProperty.getRetryDelayMs()))
                .returns(rowTypeInfo);

        rowsDS.output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername("org.postgresql.Driver")
                        .setDBUrl(dataSourceProperties.getUrl())
                        .setUsername(dataSourceProperties.getUsername())
                        .setPassword(dataSourceProperties.getPassword())
                        .setQuery("INSERT INTO metadata (data_source, table_name, data, updated_at) VALUES (?, ?, ?::jsonb, ?)")
                        .setSqlTypes(new int[]{
                                Types.VARCHAR,   // источник бд
                                Types.VARCHAR,   // имя системной таблицы
                                Types.VARCHAR,   // строка из таблицы в виде json
                                Types.TIMESTAMP  // дата последнего обновления
                        })
                        .finish()
        );

        try {
            env.execute("Репликация метаданных в единую таблицу метаданных");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}