package com.example.flinkreplication.service.impl;

import com.example.flinkreplication.dto.SourceDbConnections;
import com.example.flinkreplication.flink.MetadataExtractorByDatabase;
import com.example.flinkreplication.model.ReplicationJob;
import com.example.flinkreplication.properties.*;
import com.example.flinkreplication.repository.ReplicationJobRepository;
import com.example.flinkreplication.service.DbSourcesService;
import com.example.flinkreplication.service.ReplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplicationServiceImpl implements ReplicationService {

    private final ExecutionEnvironment env;
    private final TargetDateBaseProperty targetDateBaseProperty;
    private final PostgresTablesProperties postgresProperties;
    private final FlinkProperty flinkProperty;
    private final ReplicationJobRepository jobRepository;
    private final DbSourcesService dbSourcesService;

    @Scheduled(fixedDelay = 60000)
    public void scheduledReplication() {
        log.info("Запуск репликации...");
        try {
            startReplication();
            log.info("Репликация завершена успешно");
        } catch (Exception e) {
            log.error("Ошибка при выполнении репликации", e);
        }
    }

    public void startReplication() {
        env.setParallelism(flinkProperty.getParallelism() > 0 ? flinkProperty.getParallelism() : 1);

        Map<String, ReplicationJob> jobMap = jobRepository.findAll()
                .stream()
                .collect(Collectors.toMap(
                        ReplicationJob::getDbName,
                        Function.identity()
                ));

        List<SourceDbConnections> activeSources = dbSourcesService.getDbConnections()
                .stream()
                .filter(source -> jobMap.containsKey(source.getName()))
                .collect(Collectors.toList());

        List<String> tablesToReplicate = postgresProperties.getTables();

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Timestamp.class)
        );

        run(rowTypeInfo, tablesToReplicate, activeSources);
    }

    public void run(RowTypeInfo rowTypeInfo, List<String> tablesToReplicate, List<SourceDbConnections> activeSources) {

        var rowsDS = env.fromCollection(activeSources)
                .flatMap(new MetadataExtractorByDatabase(tablesToReplicate, flinkProperty.getMaxRetries(), flinkProperty.getRetryDelayMs()))
                .returns(rowTypeInfo);

        rowsDS.output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername("org.postgresql.Driver")
                        .setDBUrl(targetDateBaseProperty.getUrl())
                        .setUsername(targetDateBaseProperty.getUsername())
                        .setPassword(targetDateBaseProperty.getPassword())
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
