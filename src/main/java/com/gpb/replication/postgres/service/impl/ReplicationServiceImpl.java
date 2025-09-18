package com.gpb.replication.postgres.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.replication.postgres.dto.SourceDbConnections;
import com.gpb.replication.postgres.log.SvoiCustomLogger;
import com.gpb.replication.postgres.log.SvoiSeverityEnum;
import com.gpb.replication.postgres.model.DatabaseMetadata;
import com.gpb.replication.postgres.model.EntityId;
import com.gpb.replication.postgres.model.SchemaMetadata;
import com.gpb.replication.postgres.model.TableMetadata;
import com.gpb.replication.postgres.repository.DatabaseMetadataRepository;
import com.gpb.replication.postgres.repository.SchemaMetadataRepository;
import com.gpb.replication.postgres.repository.TableMetadataRepository;
import com.gpb.replication.postgres.service.DbSourcesService;
import com.gpb.replication.postgres.service.ReplicationService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.apache.commons.codec.digest.DigestUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplicationServiceImpl implements ReplicationService {

    private final DbSourcesService dbSourcesService;
    private final SvoiCustomLogger svoiCustomLogger;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final DatabaseMetadataRepository databaseRep;
    private final SchemaMetadataRepository schemaRep;
    private final TableMetadataRepository tableRep;

    @Async
    public void startReplicationAsync(String serviceName) {
        startReplication(serviceName);
    }

    public void startReplication(String serviceName) {
        // Чистим таблицы
        truncateTables(serviceName);

        Map<String, SourceDbConnections> dbConnectionsMap = dbSourcesService.getDbConnections()
                .stream()
                .collect(Collectors.toMap(
                        SourceDbConnections::getName,
                        Function.identity()
                ));

        if (dbConnectionsMap.containsKey(serviceName)) {
            SourceDbConnections source = dbConnectionsMap.get(serviceName);

            // Репликация баз данных
            List<String> databases = databaseReplication(source);

            
            for ( String dbName : databases ) {
                // Репликация схем
                schemaReplication(source, dbName);
                // Репликация таблиц
                tableReplication(source, dbName);
            }

            log.info("Репликация завершена успешно для источника {}", serviceName);
            svoiCustomLogger.send(
                    "replicationJob",
                    "Replication Finished",
                    String.format("Replicated source: [%s];",
                            serviceName),
                    SvoiSeverityEnum.ONE
            );

        } else {
            log.info("Не найденно данных по сервису {} для репликации", serviceName);
        }
    }

    private void truncateTables(String serviceName) {
        databaseRep.deleteByServiceName(serviceName);
        schemaRep.deleteByServiceName(serviceName);
        tableRep.deleteByServiceName(serviceName);
    }

    private List<String> databaseReplication(SourceDbConnections source) {
        String sql = """
            SELECT oid, datname FROM pg_database
            WHERE datistemplate = false AND datallowconn = true AND datname NOT IN ('postgres');
        """;
        List<String> response = new ArrayList<>();
        Long currentTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(source.getUrl(), source.getUsername(), source.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            List<DatabaseMetadata> entities = new ArrayList<>();

            while (rs.next()) {
                DatabaseMetadata entity = new DatabaseMetadata();
                String fqn = getFqn(List.of(source.getServiceName(), rs.getString("datname")));

                EntityId id = new EntityId(rs.getLong("oid"),source.getServiceName());

                entity.setId(id);
                entity.setFqn(fqn);
                entity.setParentFqn(source.getServiceName());
                entity.setName(rs.getString("datname"));
                entity.setCreatedAt(currentTime);
                String hashString = fqn;

                // Подсчет хэш
                String hashData = DigestUtils.md5Hex(hashString);
                entity.setHashData(hashData);

                entities.add(entity);
                response.add(rs.getString("datname"));
            }
            databaseRep.saveAll(entities);

        } catch (SQLException e) {
            log.error("Ошибка при получении списка баз для {}: {}", source.getName(), e.getMessage(), e);
        }
        return response;
    }

    private void schemaReplication(SourceDbConnections source, String dbName) {
        String sql = """
            SELECT n.oid, n.nspname AS schema_name
            FROM pg_namespace n
            WHERE n.nspname not in ('information_schema', 'pg_catalog', 'pg_toast');
        """;
        List<SchemaMetadata> entities = new ArrayList<>();
        String url = buildDbUrl(source.getUrl(), dbName);
        Long currentTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(url, source.getUsername(), source.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                SchemaMetadata entity = new SchemaMetadata();
                String fqn = getFqn(List.of(source.getServiceName(), dbName, rs.getString("schema_name")));
                String parentFqn = fqn.substring(0, fqn.lastIndexOf("."));
                
                EntityId id = new EntityId(rs.getLong("oid"),source.getServiceName());

                entity.setId(id);
                entity.setFqn(fqn);
                entity.setDbName(dbName);
                entity.setName(rs.getString("schema_name"));
                entity.setParentFqn(parentFqn);
                entity.setCreatedAt(currentTime);

                // Подсчет хэш
                String hashString = fqn;
                String hashData = DigestUtils.md5Hex(hashString);
                entity.setHashData(hashData);

                entities.add(entity);
            }
            schemaRep.saveAll(entities);
        } catch (SQLException e) {
            log.error("Ошибка при получении схем для {}: {}", source.getName(), e.getMessage(), e);
        }
    }

    private void tableReplication(SourceDbConnections source, String dbName) {
        String sql = """
            SELECT
                c.oid,
                n.nspname as schema_name,
                c.relname as table_name,
                CASE 
                    WHEN c.relkind = 'r' THEN 'regular'
                    WHEN c.relkind = 'v' THEN 'view'
                    WHEN c.relkind = 'm' THEN 'materialized_view'
                    ELSE 'other'
                END as table_type,
                obj_description(c.oid, 'pg_class') as description,
                jsonb_build_object(
                    'columns', 
                    (SELECT jsonb_agg(
                        jsonb_build_object(
                            'fqn', current_database() || '.' || n.nspname || '.' || c.relname || '.' || a.attname,
                            'name', a.attname,
                            'dtype', split_part(format_type(a.atttypid, a.atttypmod), '(', 1),
                            'dataLength', 
                                CASE 
                                    WHEN a.atttypid IN (1042, 1043, 25) THEN 
                                        CASE WHEN a.atttypmod > 0 THEN a.atttypmod - 4 ELSE NULL END
                                    WHEN a.atttypid IN (1700) THEN 
                                        CASE WHEN a.atttypmod > 0 THEN (a.atttypmod - 4) >> 16 ELSE NULL END
                                    WHEN a.atttypid IN (1083, 1114, 1184, 1266) THEN 
                                        CASE WHEN a.atttypmod > 0 THEN a.atttypmod & 65535 ELSE NULL END
                                    WHEN a.atttypid IN (1560, 1562) THEN 
                                        CASE WHEN a.atttypmod > 0 THEN a.atttypmod - 4 ELSE NULL END
                                    ELSE NULL 
                                END,
                            'is_nullable', NOT a.attnotnull,
                            'description', col_description(a.attrelid, a.attnum)
                        ) ORDER BY a.attnum
                    )
                    FROM pg_attribute a 
                    WHERE a.attrelid = c.oid 
                    AND a.attnum > 0 
                    AND NOT a.attisdropped)
                ) as table_structure
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'v', 'm')
            AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY n.nspname, c.relname;
            """;

        String url = buildDbUrl(source.getUrl(), dbName);
        Long currentTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(url, source.getUsername(), source.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            List<TableMetadata> entities = new ArrayList<>();

            while (rs.next()) {
                try {
                    TableMetadata entity = new TableMetadata();
                    String fqn = getFqn(List.of(source.getServiceName(), dbName, rs.getString("schema_name"), rs.getString("table_name")));
                    String parentFqn = fqn.substring(0, fqn.lastIndexOf("."));

                    EntityId id = new EntityId(rs.getLong("oid"),source.getServiceName());

                    entity.setId(id);
                    entity.setFqn(fqn);
                    entity.setDbName(dbName);
                    entity.setSchemaName(rs.getString("schema_name"));
                    entity.setDescription(rs.getString("description"));
                    entity.setName(rs.getString("table_name"));
                    entity.setParentFqn(parentFqn);
                    entity.setCreatedAt(currentTime);

                    // Собираем data (jsonb)
                    Map<String, Object> dataMap = new HashMap<>();

                    String jsonString = rs.getString("table_structure");
                    JsonNode columnsNode = objectMapper.readTree(jsonString);
                    dataMap.put("columns", columnsNode);

                    // Подсчет хэш
                    String jsonStringForHash = objectMapper.writeValueAsString(dataMap);
                    String hashString = fqn + rs.getString("description");
                    String hashData = DigestUtils.md5Hex(jsonStringForHash + hashString);
                    entity.setHashData(hashData);

                    JsonNode jsonNode = objectMapper.valueToTree(dataMap);
                    entity.setData(jsonNode);

                    entities.add(entity);
                } catch (JsonProcessingException e) {
                    log.error("Ошибка при преобразовании JSON для таблицы {}: {}", 
                            rs.getString("table_name"), e.getMessage(), e);
                }
            }
            tableRep.saveAll(entities);

        } catch (SQLException e) {
            log.error("Ошибка при получении таблиц для {}: {}", source.getName(), e.getMessage(), e);
        }
    }

    private String getFqn(List<String> names) {
        return String.join(".", names);
    }

    private String buildDbUrl(String originalUrl, String dbName) {
        String url = originalUrl.trim();
        if (!url.matches(".*/[^/]+$")) {
            if (!url.endsWith("/")) url += "/";
            url += dbName;
        } else {
            url = url.replaceFirst("/[^/]+$", "/" + dbName);
        }
        return url;
    }
}
