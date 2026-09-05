package com.periscope.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaConfigTest {

    @Test
    void validConfigShouldRetainValues() {
        KafkaConfig config = new KafkaConfig("localhost:9092", "all", 3, "custom_prefix");

        assertEquals("localhost:9092", config.bootstrapServers());
        assertEquals("all", config.acks());
        assertEquals(3, config.retries());
        assertEquals("custom_prefix", config.topicPrefix());
    }

    @Test
    void defaultValuesShouldApplyWhenOptionalFieldsAreNullOrBlank() {
        // Passing null for acks and topicPrefix should apply sensible defaults
        KafkaConfig config = new KafkaConfig("localhost:9092", null, -1, null);

        assertEquals("all", config.acks());
        assertEquals(Integer.MAX_VALUE, config.retries());
        assertEquals("periscope", config.topicPrefix());
    }

    @Test
    void blankBootstrapServersShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> new KafkaConfig("   ", "all", 1, "test"));
    }
}