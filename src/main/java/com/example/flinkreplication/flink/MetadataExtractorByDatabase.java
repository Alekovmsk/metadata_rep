package com.example.flinkreplication.flink;

import com.example.flinkreplication.properties.SourceDbProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

@RequiredArgsConstructor
public class MetadataExtractorByDatabase implements FlatMapFunction<SourceDbProperties, Row> {

    private final List<String> tables;
    private final int maxRetries;
    private final long retryDelayMs;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void flatMap(SourceDbProperties source, Collector<Row> collector) {
        int attempt = 0;
        while (attempt < maxRetries) {
            try (Connection conn = DriverManager.getConnection(source.getUrl(), source.getUsername(), source.getPassword())) {
                for (String tableName : tables) {
                    try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + tableName);
                         ResultSet rs = stmt.executeQuery()) {

                        ResultSetMetaData meta = rs.getMetaData();
                        int columnCount = meta.getColumnCount();

                        while (rs.next()) {
                            Map<String, Object> rowMap = new LinkedHashMap<>();
                            for (int i = 1; i <= columnCount; i++) {
                                String columnName = meta.getColumnName(i);
                                Object value = rs.getObject(i);

                                if (value instanceof Array array) {
                                    try {
                                        value = Arrays.asList((Object[]) array.getArray());
                                    } catch (Exception e) {
                                        value = "[UNREADABLE ARRAY]";
                                    }
                                } else if (value instanceof Clob clob) {
                                    value = clob.getSubString(1, (int) clob.length());
                                } else if (value instanceof Blob blob) {
                                    value = "[BLOB " + blob.length() + " bytes]";
                                } else if (value instanceof Struct struct) {
                                    value = struct.toString();
                                } else if (value != null && value.getClass().getName().startsWith("org.postgresql")) {
                                    value = value.toString();
                                }

                                rowMap.put(columnName, value);
                            }

                            Row flinkRow = new Row(4);
                            flinkRow.setField(0, source.getName());
                            flinkRow.setField(1, tableName);
                            flinkRow.setField(2, objectMapper.writeValueAsString(rowMap));
                            flinkRow.setField(3, Timestamp.valueOf(LocalDateTime.now()));

                            collector.collect(flinkRow);
                        }

                    } catch (SQLException e) {
                        System.err.println("Error querying table " + tableName + " from DB " + source.getName() + ": " + e.getMessage());
                    }
                }
                break;

            } catch (SQLException e) {
                attempt++;
                System.err.println("Connection error to DB " + source.getName() + " (attempt " + attempt + "): " + e.getMessage());
                if (attempt >= maxRetries) {
                    System.err.println("Max retries reached for DB " + source.getName());
                } else {
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } catch (Exception ex) {
                System.err.println("Unexpected error for DB " + source.getName() + ": " + ex.getMessage());
                break;
            }
        }
    }
}

