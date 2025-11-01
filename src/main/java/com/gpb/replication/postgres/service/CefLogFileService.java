package com.gpb.replication.postgres.service;

import com.gpb.replication.postgres.properties.CefLoggingProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;

@Service
@RequiredArgsConstructor
public class CefLogFileService {

    private final CefLoggingProperties properties;

    private static final DateTimeFormatter FILE_TS =
            DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
    private static final DateTimeFormatter FILE_DATE =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final ZoneId ZONE = ZoneId.systemDefault();

    public Path getDailyLogPath() {
        String date = LocalDate.now(ZONE).format(FILE_DATE);
        return Paths.get(properties.getPath() + "-" + date + ".log");
    }

    public void writeToFile(LocalDateTime created, String cefLog) {
        Path path = getDailyLogPath();
        try {
            Files.createDirectories(path.getParent());
            String line = created.format(FILE_TS) + " " + cefLog + System.lineSeparator();
            Files.writeString(path, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Ошибка при записи CEF-лога: " + e.getMessage());
        }
    }
    public void cleanupOldLogs() {
        Path dir = Paths.get(properties.getPath()).getParent();
        if (dir == null || !Files.exists(dir)) return;

        LocalDate threshold = LocalDate.now(ZONE).minusDays(properties.getRetentionDays());
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "cef-*.log")) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                if (name.length() < 18) continue;
                try {
                    String datePart = name.substring(4, 14);
                    LocalDate fileDate = LocalDate.parse(datePart, FILE_DATE);
                    if (fileDate.isBefore(threshold)) {
                        Files.deleteIfExists(p);
                        System.out.println("Удалён старый лог: " + name);
                    }
                } catch (Exception ignored) {}
            }
        } catch (IOException e) {
            System.err.println("Ошибка при очистке старых логов: " + e.getMessage());
        }
    }
}

