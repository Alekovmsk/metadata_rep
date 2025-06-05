package com.example.flinkreplication.model;
//Модель целевой БД
import lombok.*;
import javax.persistence.*;
import java.util.Objects;

/**
 * Модель целевой базы данных для репликации
 */
@Entity
@Table(name = "target_databases")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TargetDatabase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(name = "jdbc_url", nullable = false)
    private String url;

    @Column(nullable = false)
    private String username;

    @ToString.Exclude
    @Column(nullable = false)
    private String password;

    @Column
    private String schema;

    @Column(name = "batch_size")
    @Builder.Default
    private int batchSize = 100;

    @Column(name = "replication_strategy")
    @Enumerated(EnumType.STRING)
    @Builder.Default
    private ReplicationStrategy replicationStrategy = ReplicationStrategy.FULL;

    @Column(name = "conflict_resolution")
    @Enumerated(EnumType.STRING)
    @Builder.Default
    private ConflictResolution conflictResolution = ConflictResolution.SKIP;

    // Перечисления для настроек
    public enum ReplicationStrategy {
        FULL, INCREMENTAL, MERGE
    }

    public enum ConflictResolution {
        SKIP, OVERWRITE, UPDATE
    }

    // Дополнительные методы
    public String getConnectionIdentifier() {
        return String.format("Target: %s (%s)", name, url);
    }

    public boolean isValid() {
        return url != null && url.startsWith("jdbc:postgresql://") &&
                username != null && !username.isEmpty() &&
                password != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TargetDatabase that = (TargetDatabase) o;
        return Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url);
    }
}