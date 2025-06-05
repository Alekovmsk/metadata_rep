package com.example.flinkreplication.dto;

import java.util.Map;

public class TableData {
    private String tableName;
    private String sourceDb;
    private Map<String, Object> data;

    // Конструкторы, геттеры и сеттеры
    public TableData() {}

    public TableData(String tableName, String sourceDb, Map<String, Object> data) {
        this.tableName = tableName;
        this.sourceDb = sourceDb;
        this.data = data;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSourceDb() {
        return sourceDb;
    }

    public void setSourceDb(String sourceDb) {
        this.sourceDb = sourceDb;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }
}