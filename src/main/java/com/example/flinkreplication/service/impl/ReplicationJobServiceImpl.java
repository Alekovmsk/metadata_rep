package com.example.flinkreplication.service.impl;


import com.example.flinkreplication.ReplicationJobStatus;
import com.example.flinkreplication.dto.ReplicationRequestDto;
import com.example.flinkreplication.model.ReplicationJob;
import com.example.flinkreplication.repository.ReplicationJobRepository;
import com.example.flinkreplication.service.ReplicationJobService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ReplicationJobServiceImpl implements ReplicationJobService {
    private final ReplicationJobRepository replicationJobRepository;

    @Transactional
    public void addToQueue(ReplicationRequestDto dto) {
        String dbName = dto.getDbName();
        if (dbName == null || dbName.isBlank()) {
            throw new IllegalArgumentException("Database name must be provided");
        }

        ReplicationJob job = ReplicationJob.builder()
                .dbName(dbName)
                .status(ReplicationJobStatus.PENDING)
                .build();

        replicationJobRepository.save(job);
    }
}
