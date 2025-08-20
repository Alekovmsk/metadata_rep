package com.example.flinkreplication.repository;

import com.example.flinkreplication.model.ReplicationJob;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;

@Repository
public interface ReplicationJobRepository extends JpaRepository<ReplicationJob, Long> {

    List<ReplicationJob> findByStatus(String status);

    @Lock(LockModeType.PESSIMISTIC_WRITE) // чтобы никто параллельно не взял те же задания
    @Query("SELECT j FROM ReplicationJob j WHERE j.status = 'PENDING' ORDER BY j.createdAt")
    List<ReplicationJob> findPendingJobs(Pageable pageable);

    @Query("SELECT rj FROM ReplicationJob rj")
    Set<ReplicationJob> findAllJob();
}
