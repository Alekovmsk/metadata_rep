package com.example.flinkreplication.service;

import com.example.flinkreplication.dto.ReplicationRequestDto;

public interface ReplicationJobService {
    void addToQueue(ReplicationRequestDto dto);
}
