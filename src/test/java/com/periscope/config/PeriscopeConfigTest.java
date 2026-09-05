package com.periscope.config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PeriscopeConfigTest {

    @Test
    void shouldLoadFromApplicationProperties() throws IOException {
        PeriscopeConfig config = PeriscopeConfig.load();

        assertNotNull(config.databaseConfig());
        assertNotNull(config.kafkaConfig());

        assertEquals("localhost", config.databaseConfig().host());
        assertEquals(5432, config.databaseConfig().port());
        assertEquals("periscope_db", config.databaseConfig().databaseName());
        assertEquals("postgres", config.databaseConfig().user());
        assertEquals("periscope", config.databaseConfig().password());
        assertEquals("periscope_slot", config.databaseConfig().slotName());
        assertEquals("periscope_pub", config.databaseConfig().publicationName());

        assertEquals("localhost:9092", config.kafkaConfig().bootstrapServers());
        assertEquals("all", config.kafkaConfig().acks());
        assertEquals(3, config.kafkaConfig().retries());
        assertEquals("periscope", config.kafkaConfig().topicPrefix());
    }

    @Test
    void shouldLoadFromCustomProperties() throws IOException {
        Properties custom = new Properties();
        custom.setProperty("periscope.db.host", "pg.internal");
        custom.setProperty("periscope.db.port", "5433");
        custom.setProperty("periscope.db.name", "custom_db");
        custom.setProperty("periscope.db.user", "admin");
        custom.setProperty("periscope.db.password", "secret");
        custom.setProperty("periscope.db.slot", "custom_slot");
        custom.setProperty("periscope.db.publication", "custom_pub");

        custom.setProperty("periscope.kafka.bootstrap-servers", "kafka.internal:9092");
        custom.setProperty("periscope.kafka.acks", "1");
        custom.setProperty("periscope.kafka.retries", "5");
        custom.setProperty("periscope.kafka.topic-prefix", "my_prefix");

        PeriscopeConfig config = PeriscopeConfig.load(custom);

        assertEquals("pg.internal", config.databaseConfig().host());
        assertEquals(5433, config.databaseConfig().port());
        assertEquals("custom_db", config.databaseConfig().databaseName());
        assertEquals("admin", config.databaseConfig().user());
        assertEquals("secret", config.databaseConfig().password());
        assertEquals("custom_slot", config.databaseConfig().slotName());
        assertEquals("custom_pub", config.databaseConfig().publicationName());

        assertEquals("kafka.internal:9092", config.kafkaConfig().bootstrapServers());
        assertEquals("1", config.kafkaConfig().acks());
        assertEquals(5, config.kafkaConfig().retries());
        assertEquals("my_prefix", config.kafkaConfig().topicPrefix());
    }
}
