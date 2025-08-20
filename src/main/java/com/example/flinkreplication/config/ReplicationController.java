package com.example.flinkreplication.config;

import com.esotericsoftware.kryo.serializers.TaggedFieldSerializer;
import com.example.flinkreplication.dto.ReplicationRequestDto;
import com.example.flinkreplication.service.ReplicationJobService;
import com.example.flinkreplication.service.impl.ReplicationServiceImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/replication")
@RequiredArgsConstructor
@Tag(name = "Persons", description = "API управления пользвателями ")
public class ReplicationController {

    private final ReplicationServiceImpl replicationService;
    private final ReplicationJobService replicationJobService;

    @PostMapping("/job/add")
    @Operation(summary = "Возвращает пользователя по id")
    public ResponseEntity<String> startReplication(@RequestBody ReplicationRequestDto request) {
        try {
            replicationJobService.addToQueue(request);
            return ResponseEntity.ok(String.format("Replication for %s added to queue", request.getDbName()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to start replication: " + e.getMessage());
        }
    }
}
