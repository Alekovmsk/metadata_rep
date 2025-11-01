package com.gpb.replication.postgres.logrepository;

import com.gpb.replication.postgres.service.CefLogFileService;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import java.sql.Timestamp;


@Repository
@RequiredArgsConstructor
public class LogRepository {

    private final JdbcTemplate logsJdbcTemplate;
    private final CefLogFileService cefLogFileService;
    public Log findLatestByType(String type, String host) {
        String sql = """
            SELECT l.id, l.type, l.log, l.created
            FROM audit.jdata_replicator_log l
            WHERE l.type = ?
              AND l.log LIKE ?
            ORDER BY l.created DESC
            LIMIT 1
        """;

        return logsJdbcTemplate.query(sql, new Object[]{type, "%" + host + "%"}, rs -> {
            if (rs.next()) {
                Log log = new Log();
                log.setId(rs.getObject("id", Integer.class));
                log.setType(rs.getString("type"));
                log.setLog(rs.getString("log"));
                log.setCreated(rs.getTimestamp("created").toLocalDateTime());
                return log;
            }
            return null;
        });
    }

    public void save(Log log) {
        String sql = """
            INSERT INTO audit.jdata_replicator_log (created, log, type)
            VALUES (?, ?, ?)
        """;

        logsJdbcTemplate.update(sql,
                Timestamp.valueOf(log.getCreated()),
                log.getLog(),
                log.getType());

        cefLogFileService.writeToFile(log.getCreated(), log.getLog());
        cefLogFileService.cleanupOldLogs();
    }
}
