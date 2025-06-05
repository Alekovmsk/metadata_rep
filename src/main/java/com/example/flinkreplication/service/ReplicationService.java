package com.example.flinkreplication.service;

import com.example.flinkreplication.dto.TableData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ReplicationService {

    @Autowired
    private StreamExecutionEnvironment flinkEnv;

    @Autowired
    private DatabaseConnectionService dbConnectionService;

    @Autowired
    private Map<String, JdbcTemplate> sourceJdbcTemplates;

    @Autowired
    private JdbcTemplate targetJdbcTemplate;

    private static final String[] TABLES_TO_REPLICATE = {"table1", "table2", "table3", "table4"};

    public void startReplication() throws Exception {
        for (String tableName : TABLES_TO_REPLICATE) {
            DataStream<TableData> tableDataStream = flinkEnv
                    .fromCollection(sourceJdbcTemplates.keySet())
                    .flatMap((String dbName, org.apache.flink.util.Collector<TableData> out) -> {
                        JdbcTemplate sourceJdbc = sourceJdbcTemplates.get(dbName);
                        List<Map<String, Object>> rows = dbConnectionService.fetchTableData(sourceJdbc, tableName);

                        for (Map<String, Object> row : rows) {
                            out.collect(new TableData(tableName, dbName, row));
                        }
                    });

            tableDataStream.addSink(new SinkFunction<TableData>() {
                @Override
                public void invoke(TableData value, Context context) throws Exception {
                    dbConnectionService.insertData(targetJdbcTemplate, value.getTableName(), value.getData());
                }
            });
        }

        flinkEnv.execute("PostgreSQL Tables Replication");
    }
}