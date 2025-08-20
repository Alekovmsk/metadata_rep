package com.example.flinkreplication.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class SourceDbConnections implements Serializable {
    private String name;
    private String url;
    private String username;
    private String password;
    private boolean active;
    private String schema;
    private int priority;
}
