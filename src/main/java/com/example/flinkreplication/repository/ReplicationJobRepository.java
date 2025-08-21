package com.example.flinkreplication.repository;

import com.example.flinkreplication.enums.ReplicationJobStatus;
import com.example.flinkreplication.model.ReplicationJob;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;

@Repository
public interface ReplicationJobRepository extends JpaRepository<ReplicationJob, Long> {

    List<ReplicationJob> findByStatus(String status);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<ReplicationJob> findByStatusOrderByCreatedAt(ReplicationJobStatus status);

    @Query("SELECT rj FROM ReplicationJob rj")
    Set<ReplicationJob> findAllJob();
}
