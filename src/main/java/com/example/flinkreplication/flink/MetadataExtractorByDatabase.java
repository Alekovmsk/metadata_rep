package com.example.flinkreplication.flink;

import com.example.flinkreplication.dto.SourceDbConnections;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

@RequiredArgsConstructor
public class MetadataExtractorByDatabase implements FlatMapFunction<SourceDbConnections, Row> {

    private static final Logger log = LoggerFactory.getLogger(MetadataExtractorByDatabase.class);

    private static final int ROW_FIELD_COUNT = 9;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final int maxRetries;
    private final long retryDelayMs;

    @Override
    public void flatMap(SourceDbConnections source, Collector<Row> collector) {
        Timestamp currentTimestamp = Timestamp.valueOf(LocalDateTime.now());
        List<String> userDatabases = getAllUserDatabases(source);

        for (String dbName : userDatabases) {
            String dbUrl = buildDbUrl(source.getUrl(), dbName);
            try (Connection conn = connectWithRetries(dbUrl, source.getUsername(), source.getPassword())) {
                List<String> userTables = getAllUserTables(conn);

                for (String tableFullName : userTables) {
                    String[] parts = tableFullName.split("\\.");
                    String schema = parts[0];
                    String tableName = parts[1];

                    processTableColumns(conn, source, schema, tableName, collector, currentTimestamp, dbName);
                }

            } catch (SQLException | InterruptedException e) {
                log.error("Не удалось подключиться к базе {}: {}", dbName, e.getMessage(), e);
            }
        }
    }

    private Connection connectWithRetries(String url, String username, String password) throws InterruptedException {
        int attempt = 0;
        while (attempt < maxRetries) {
            try {
                return DriverManager.getConnection(url, username, password);
            } catch (SQLException e) {
                attempt++;
                log.warn("Попытка подключения {}/{} не удалась для URL {}: {}", attempt, maxRetries, url, e.getMessage());
                if (attempt >= maxRetries) throw new RuntimeException("Достигнут предел попыток подключения к БД " + url, e);
                Thread.sleep(retryDelayMs);
            }
        }
        throw new RuntimeException("Неожиданная ошибка подключения к БД " + url);
    }

    private List<String> getAllUserDatabases(SourceDbConnections source) {
        List<String> databases = new ArrayList<>();
        String adminUrl = buildAdminUrl(source.getUrl());

        String sql = "SELECT datname FROM pg_database " +
                "WHERE datistemplate = false AND datallowconn = true AND datname NOT IN ('postgres')";

        try (Connection conn = DriverManager.getConnection(adminUrl, source.getUsername(), source.getPassword());
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                databases.add(rs.getString("datname"));
            }

        } catch (SQLException e) {
            log.error("Ошибка при получении списка баз для {}: {}", source.getName(), e.getMessage(), e);
        }

        return databases;
    }

    private List<String> getAllUserTables(Connection conn) throws SQLException {
        List<String> userTables = new ArrayList<>();
        String sql = """
                SELECT n.nspname AS schema_name,
                                                                c.relname AS table_name
                                                         FROM pg_class c
                                                                  JOIN pg_namespace n ON n.oid = c.relnamespace
                                                                  LEFT JOIN pg_inherits i ON c.oid = i.inhrelid  -- проверяем, есть ли родитель
                                                         WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                                                           AND ((c.relkind = 'r' AND i.inhrelid IS NULL)  -- обычные таблицы, не партиции
                                                             OR c.relkind = 'p')                         -- родительские партиции
                                                         ORDER BY n.nspname, c.relname;          
                """;

        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String schema = rs.getString("schema_name");
                String table = rs.getString("table_name");
                userTables.add(schema + "." + table);
            }
        }

        return userTables;
    }

    private void processTableColumns(Connection conn, SourceDbConnections source, String schema, String tableName,
                                     Collector<Row> collector, Timestamp currentTimestamp, String dbName) {
        String sql = """
            SELECT a.attname AS column_name,
                   t.typname AS data_type,
                   a.attnotnull AS not_null,
                   coalesce(pg_get_expr(ad.adbin, ad.adrelid), '') AS column_default,
                   a.attnum AS ordinal_position
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_type t ON t.oid = a.atttypid
            LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            WHERE c.relname = ?
              AND n.nspname = ?
              AND a.attnum > 0
            ORDER BY a.attnum
        """;

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableName);
            stmt.setString(2, schema);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> rowMap = new LinkedHashMap<>();
                    rowMap.put("column_name", rs.getString("column_name"));
                    rowMap.put("data_type", rs.getString("data_type"));
                    rowMap.put("not_null", rs.getBoolean("not_null"));
                    rowMap.put("column_default", rs.getString("column_default"));
                    rowMap.put("column_position", rs.getInt("ordinal_position"));

                    rowMap.put("data_source", source.getName());
                    rowMap.put("database", dbName);
                    rowMap.put("schema", schema);
                    rowMap.put("table", tableName);

                    String jsonData = objectMapper.writeValueAsString(rowMap);
                    String recordKey = buildRecordKey(rowMap);
                    String dataHash = DigestUtils.md5DigestAsHex(jsonData.getBytes(StandardCharsets.UTF_8));

                    Row flinkRow = new Row(ROW_FIELD_COUNT);
                    flinkRow.setField(0, source.getName());   // data_source
                    flinkRow.setField(1, tableName);          // table_name
                    flinkRow.setField(2, schema);             // schema_name
                    flinkRow.setField(3, recordKey);          // record_key
                    flinkRow.setField(4, dataHash);           // data_hash
                    flinkRow.setField(5, jsonData);           // data
                    flinkRow.setField(6, currentTimestamp);   // updated_at
                    flinkRow.setField(7, source.getDbType()); // db_type
                    flinkRow.setField(8, dbName);             // db_name


                    collector.collect(flinkRow);
                }
            }

        } catch (SQLException | JsonProcessingException e) {
            log.error("Ошибка при обработке таблицы {}.{} из базы {}: {}", schema, tableName, dbName, e.getMessage(), e);
        }
    }

    private String buildRecordKey(Map<String, Object> rowMap) {
        return String.join("|",
                safe(rowMap.get("data_source")),
                safe(rowMap.get("database")),
                safe(rowMap.get("schema")),
                safe(rowMap.get("table")),
                safe(rowMap.get("column_name"))
        );
    }

    private String safe(Object value) {
        return value != null ? value.toString() : "NULL";
    }

    private String buildAdminUrl(String originalUrl) {
        String url = originalUrl.trim();
        if (!url.matches(".*/[^/]+$")) {
            if (!url.endsWith("/")) url += "/";
            url += "postgres";
        } else {
            url = url.replaceFirst("/[^/]+$", "/postgres");
        }
        return url;
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
