package com.periscope.config;

import java.util.Objects;

public record KafkaConfig(String bootstrapServers, String acks, int retries, String topicPrefix) {
    public KafkaConfig {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers cannot be null");

        if (bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers cannot be blank");
        }
        if (acks == null || acks.isBlank()) {
            acks = "all";   // Default to safest durable setting
        }
        if (retries < 0) {
            retries = Integer.MAX_VALUE; // Kafka standard retry default
        }
        if (topicPrefix == null || topicPrefix.isBlank()) {
            topicPrefix = "periscope";
        }
    }
}
