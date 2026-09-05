package com.periscope.config;

import java.util.Objects;

public record DatabaseConfig(String host, int port, String databaseName, String user, String password, String slotName, String publicationName) {
    public DatabaseConfig {
        Objects.requireNonNull(host, "host cannot be null");
        Objects.requireNonNull(databaseName, "databaseName cannot be null");
        Objects.requireNonNull(user, "user cannot be null");
        Objects.requireNonNull(password, "password cannot be null");
        Objects.requireNonNull(slotName, "slotName cannot be null");
        Objects.requireNonNull(publicationName, "publicationName cannot be null");

        if (host.isBlank()) throw new IllegalArgumentException("host cannot be blank");
        if (port <= 0 || port > 65535) throw new IllegalArgumentException("port must be between 0 and 65535");
        if (databaseName.isBlank()) throw new IllegalArgumentException("databaseName cannot be blank");
        if (slotName.isBlank()) throw new IllegalArgumentException("slotName cannot be blank");
        if (publicationName.isBlank()) throw new IllegalArgumentException("publicationName cannot be blank");
    }

    public String getJdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + databaseName;
    }
}
