package com.periscope.kafka;

import com.periscope.model.ChangeEvent;
import com.periscope.model.OperationType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EventSerializerTest {

    @Test
    void testSerializationProducesCorrectJson() {
        // 1. Arrange
        EventSerializer serializer = new EventSerializer();
        Instant now = Instant.parse("2023-10-27T10:00:00Z");
        
        ChangeEvent event = new ChangeEvent(
                "public",
                "users",
                OperationType.INSERT,
                12345678L,
                now,
                null,
                Map.of("id", "1", "name", "Alice"),
                "id"
        );

        // 2. Act
        byte[] result = serializer.serialize(event);
        assertNotNull(result, "Serialized byte array should not be null");
        String jsonOutput = new String(result);

        // 3. Assert
        assertTrue(jsonOutput.contains("\"schemaName\":\"public\""), "JSON should contain schemaName");
        assertTrue(jsonOutput.contains("\"tableName\":\"users\""), "JSON should contain tableName");
        assertTrue(jsonOutput.contains("\"operation\":\"INSERT\""), "JSON should contain operation type");
        assertTrue(jsonOutput.contains("\"lsn\":12345678"), "JSON should contain LSN");
        assertTrue(jsonOutput.contains("\"2023-10-27T10:00:00Z\""), "JSON should contain ISO-8601 formatted timestamp");
        assertTrue(jsonOutput.contains("\"name\":\"Alice\""), "JSON should contain after data");
        assertTrue(jsonOutput.contains("\"primaryKeyColumn\":\"id\""), "JSON should contain primary key column");
    }
}
