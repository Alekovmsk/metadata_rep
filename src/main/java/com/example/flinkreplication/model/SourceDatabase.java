package com.example.flinkreplication.model;
//Модель источника
import lombok.*;
import javax.persistence.*;
import java.util.Objects;

/**
 * Модель исходной базы данных для репликации
 */
@Entity
@Table(name = "source_databases")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SourceDatabase {

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

    @Column(nullable = false)
    @Builder.Default
    private boolean active = true;

    @Column(nullable = false)
    @Builder.Default
    private int priority = 1;

    @Column(name = "connection_timeout")
    @Builder.Default
    private int connectionTimeout = 30; // в секундах

    // Дополнительные бизнес-методы
    public String getConnectionIdentifier() {
        return String.format("%s (%s)", name, url);
    }

    public boolean isValid() {
        return name != null && !name.isEmpty() &&
                url != null && url.startsWith("jdbc:postgresql://") &&
                username != null && !username.isEmpty() &&
                password != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SourceDatabase that = (SourceDatabase) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, url);
    }
}