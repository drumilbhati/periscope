package com.periscope.api;

import com.periscope.config.DatabaseConfig;
import com.periscope.config.KafkaConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PeriscopeClientTest {

    @Test
    void builderRequiresBothInfrastructureConfigurations() {
        assertThrows(IllegalStateException.class, () -> PeriscopeClient.builder().build());
        assertThrows(IllegalStateException.class, () -> PeriscopeClient.builder()
                .database(database())
                .build());
        assertThrows(IllegalStateException.class, () -> PeriscopeClient.builder()
                .kafka(kafka())
                .build());
    }

    @Test
    void builderCreatesAClientWithoutConnectingUntilStart() {
        assertDoesNotThrow(() -> PeriscopeClient.builder()
                .database(database())
                .kafka(kafka())
                .createInfrastructure(false)
                .build());
    }

    private static DatabaseConfig database() {
        return new DatabaseConfig("localhost", 5432, "app", "postgres", "secret", "slot", "publication");
    }

    private static KafkaConfig kafka() {
        return new KafkaConfig("localhost:9092", "all", 3, "periscope");
    }
}
