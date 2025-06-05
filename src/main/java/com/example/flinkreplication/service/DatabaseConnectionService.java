package com.example.flinkreplication.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class DatabaseConnectionService {

    public List<Map<String, Object>> fetchTableData(JdbcTemplate jdbcTemplate, String tableName) {
        String query = "SELECT * FROM " + tableName;
        return jdbcTemplate.queryForList(query);
    }

    public void insertData(JdbcTemplate jdbcTemplate, String tableName, Map<String, Object> data) {
        // Реализация вставки данных с оптимизацией
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        Object[] params = new Object[data.size()];

        int i = 0;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (i > 0) {
                columns.append(", ");
                values.append(", ");
            }
            columns.append(entry.getKey());
            values.append("?");
            params[i] = entry.getValue();
            i++;
        }

        String query = String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
                tableName, columns.toString(), values.toString());
        jdbcTemplate.update(query, params);
    }
}