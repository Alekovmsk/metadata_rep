package com.example.flinkreplication.logrepository;

import com.example.flinkreplication.model.Log;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface LogRepository extends JpaRepository<Log, Integer> {
    @Query("SELECT l FROM Log l WHERE l.type = :type AND l.log LIKE CONCAT('%',:host,'%') ORDER BY l.created DESC LIMIT 1")
    Log findLatestByType(@Param("type") String type, @Param("host") String host);
}
