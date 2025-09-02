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
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class MetadataExtractorByDatabase implements FlatMapFunction<SourceDbConnections, Row> {

    private static final Logger log = LoggerFactory.getLogger(MetadataExtractorByDatabase.class);

    private static final int ROW_FIELD_COUNT = 6;
    private static final int IDX_SOURCE_NAME = 0;
    private static final int IDX_TABLE_NAME = 1;
    private static final int IDX_RECORD_KEY  = 2;
    private static final int IDX_DATA_JSON   = 3;
    private static final int IDX_DATA_HASH   = 4;
    private static final int IDX_TIMESTAMP   = 5;

    private final List<String> tables;
    private final int maxRetries;
    private final long retryDelayMs;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void flatMap(SourceDbConnections source, Collector<Row> collector) {
        Connection conn = null;
        Timestamp currentTimestamp = Timestamp.valueOf(LocalDateTime.now());

        try {
            conn = connectWithRetries(source);
            for (String tableName : tables) {
                processTable(conn, source, tableName, collector, currentTimestamp);
            }
        } catch (Exception e) {
            log.error("Не удалось извлечь метаданные для БД {}: {}", source.getName(), e.getMessage(), e);
        } finally {
            closeQuietly(conn);
        }
    }

    private Connection connectWithRetries(SourceDbConnections source) throws InterruptedException {
        int attempt = 0;
        while (attempt < maxRetries) {
            try {
                return DriverManager.getConnection(source.getUrl(), source.getUsername(), source.getPassword());
            } catch (SQLException e) {
                attempt++;
                log.warn("Попытка подключения {}/{} не удалась для БД {}: {}", attempt, maxRetries, source.getName(), e.getMessage());
                if (attempt >= maxRetries) {
                    throw new RuntimeException("Достигнут предел попыток подключения к БД " + source.getName(), e);
                }
                Thread.sleep(retryDelayMs);
            }
        }
        throw new RuntimeException("Неожиданная ошибка подключения к БД " + source.getName());
    }

    private void processTable(Connection conn, SourceDbConnections source, String tableName,
                              Collector<Row> collector, Timestamp currentTimestamp) {
        String sql = "SELECT * FROM " + tableName;
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            while (rs.next()) {
                Map<String, Object> rowMap = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = meta.getColumnName(i);
                    Object value = rs.getObject(i);
                    value = convertJdbcValue(value);
                    rowMap.put(columnName, value);
                }

                // ⚡ Добавляем data_source в map, чтобы ключ был полным
                rowMap.put("data_source", source.getName());

                // JSON строки
                String jsonData = objectMapper.writeValueAsString(rowMap);

                // record_key — формируем стабильный ключ для всех типов системных таблиц
                String recordKey = buildRecordKey(rowMap);

                // data_hash — SHA256 от jsonData
                String dataHash = DigestUtils.md5DigestAsHex(jsonData.getBytes(StandardCharsets.UTF_8));

                Row flinkRow = new Row(6);
                flinkRow.setField(0, source.getName());   // data_source
                flinkRow.setField(1, tableName);          // table_name
                flinkRow.setField(2, recordKey);          // record_key
                flinkRow.setField(3, dataHash);           // MD5
                flinkRow.setField(4, jsonData);           // JSON
                flinkRow.setField(5, currentTimestamp);   // updated_at

                collector.collect(flinkRow);
            }

        } catch (SQLException | JsonProcessingException e) {
            log.error("Ошибка при выполнении запроса к таблице {} из БД {}: {}", tableName, source.getName(), e.getMessage(), e);
        }
    }

    private String buildRecordKey(Map<String, Object> rowMap) {
        String recordKey;

        // Колонки таблиц
        if (rowMap.containsKey("table_name") && rowMap.containsKey("column_name")) {
            recordKey = String.join("|",
                    safe(rowMap.get("data_source")),
                    safe(rowMap.get("table_schema")),
                    safe(rowMap.get("table_name")),
                    safe(rowMap.get("column_name"))
            );
        }
        // pg_attribute (системные колонки)
        else if (rowMap.containsKey("attrelid") && rowMap.containsKey("attnum")) {
            recordKey = String.join("|",
                    safe(rowMap.get("data_source")),
                    safe(rowMap.get("attrelid")),
                    safe(rowMap.get("attnum"))
            );
        }
        // pg_class (таблицы/индексы)
        else if (rowMap.containsKey("oid") && rowMap.containsKey("relname")) {
            recordKey = String.join("|",
                    safe(rowMap.get("data_source")),
                    safe(rowMap.get("oid")),
                    safe(rowMap.get("relname"))
            );
        }
        // pg_database
        else if (rowMap.containsKey("datname")) {
            recordKey = String.join("|",
                    safe(rowMap.get("data_source")),
                    safe(rowMap.get("datname"))
            );
        }
        // fallback — все значения конкатенируем (на крайний случай)
        else {
            recordKey = rowMap.values().stream()
                    .map(this::safe)
                    .collect(Collectors.joining("|"));
        }

        return recordKey;
    }

    private String safe(Object value) {
        return value != null ? value.toString() : "NULL";
    }

    private Object convertJdbcValue(Object value) {
        if (value instanceof Array array) {
            try {
                Object[] arr = (Object[]) array.getArray();
                return Arrays.asList(arr);
            } catch (SQLException e) {
                log.warn("Ошибка при чтении SQL Array: {}", e.getMessage());
                return "[НЕЧИТАЕМЫЙ ARRAY]";
            }
        } else if (value instanceof Clob clob) {
            try {
                return clob.getSubString(1, (int) clob.length());
            } catch (SQLException e) {
                log.warn("Ошибка при чтении CLOB: {}", e.getMessage());
                return "[НЕЧИТАЕМЫЙ CLOB]";
            }
        } else if (value instanceof Blob blob) {
            try {
                return "[BLOB " + blob.length() + " байт]";
            } catch (SQLException e) {
                return "[НЕЧИТАЕМЫЙ BLOB]";
            }
        } else if (value instanceof Struct struct) {
            return struct.toString();
        } else if (value != null && value.getClass().getName().startsWith("org.postgresql")) {
            return value.toString();
        }
        return value;
    }

    private void closeQuietly(AutoCloseable ac) {
        if (ac != null) {
            try {
                ac.close();
            } catch (Exception e) {
                log.warn("Ошибка при закрытии ресурса: {}", e.getMessage());
            }
        }
    }
}
