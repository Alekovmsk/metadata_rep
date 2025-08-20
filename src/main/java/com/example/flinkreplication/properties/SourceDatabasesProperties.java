package com.example.flinkreplication.properties;

import com.example.flinkreplication.dto.SourceDbConnections;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "replication.databases")
public class SourceDatabasesProperties {
    private List<SourceDbConnections> info = new ArrayList<>();

}
