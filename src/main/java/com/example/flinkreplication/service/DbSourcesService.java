package com.example.flinkreplication.service;


import com.example.flinkreplication.dto.SourceDbConnections;

import java.util.List;

public interface DbSourcesService {
     List<SourceDbConnections> getDbConnections();
}
