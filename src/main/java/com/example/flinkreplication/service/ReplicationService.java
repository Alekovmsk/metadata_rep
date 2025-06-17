package com.example.flinkreplication.service;

import com.example.flinkreplication.flink.MetadataExtractorByDatabase;
import com.example.flinkreplication.properties.*;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ReplicationService {

    private final ExecutionEnvironment env;
    private final TargetDateBaseProperty targetDateBaseProperty;
    private final PostgresTablesProperties postgresProperties;
    private final SourceDatabasesProperties sourceDatabasesProperties;
    private final FlinkProperty flinkProperty;

    public void startReplication() throws Exception {
        env.setParallelism(flinkProperty.getParallelism() > 0 ? flinkProperty.getParallelism() : 1);

        List<String> tablesToReplicate = postgresProperties.getTables();
        List<SourceDbProperties> activeSources = sourceDatabasesProperties.getInfo().stream()
                .filter(SourceDbProperties::isActive).toList();

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Timestamp.class)
        );

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

        env.execute("Replicate metadata to unified metadata table");
    }
}
