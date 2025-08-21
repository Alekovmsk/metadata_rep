package com.example.flinkreplication.service.impl;

import com.example.flinkreplication.dto.SourceDbConnections;
import com.example.flinkreplication.service.DbSourcesService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
@RequiredArgsConstructor
public class DbSourcesServiceImpl implements DbSourcesService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public List<SourceDbConnections> getDbConnections() {
        List<SourceDbConnections> dbConnections = new ArrayList<>();

        try (var is = new ClassPathResource("db-connections.json").getInputStream()) {
            dbConnections = objectMapper.readValue(is, new TypeReference<List<SourceDbConnections>>() {});
        } catch (IOException e) {
            // Если файл не найден или ошибка чтения, создаём пустой объект
            dbConnections = Collections.emptyList();
        }
        return dbConnections;
    }
}