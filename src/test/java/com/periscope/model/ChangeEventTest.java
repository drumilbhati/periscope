package com.periscope.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ChangeEventTest {

    @Test
    @DisplayName("Should create INSERT event and extract primary key from after map")
    void insertEventCreationAndPrimaryKeyExtraction() {
        Map<String, Object> after = Map.of("id", 101L, "name", "Alice", "email", "alice@example.com");

        ChangeEvent event = new ChangeEvent(
                "public",
                "customers",
                OperationType.INSERT,
                12345678L,
                Instant.now(),
                null,
                after,
                "id"
        );

        assertNull(event.before());
        assertNotNull(event.after());
        assertEquals("public", event.schemaName());
        assertEquals("customers", event.tableName());
        assertEquals(OperationType.INSERT, event.operation());
        assertEquals(12345678L, event.lsn());
        assertEquals(101L, event.getPrimaryKeyValue());
    }

    @Test
    @DisplayName("Should create UPDATE event and extract primary key from after map")
    void updateEventCreationAndPrimaryKeyExtraction() {
        Map<String, Object> before = Map.of("id", 101L, "email", "old@example.com");
        Map<String, Object> after = Map.of("id", 101L, "email", "new@example.com");

        ChangeEvent event = new ChangeEvent(
                "public",
                "customers",
                OperationType.UPDATE,
                12345679L,
                Instant.now(),
                before,
                after,
                "id"
        );

        assertEquals("old@example.com", event.before().get("email"));
        assertEquals("new@example.com", event.after().get("email"));
        assertEquals(101L, event.getPrimaryKeyValue());
    }

    @Test
    @DisplayName("Should create DELETE event and extract primary key from before map")
    void deleteEventCreationAndPrimaryKeyExtraction() {
        Map<String, Object> before = Map.of("id", 101L, "name", "Alice");

        ChangeEvent event = new ChangeEvent(
                "public",
                "customers",
                OperationType.DELETE,
                12345680L,
                Instant.now(),
                before,
                null,
                "id"
        );

        assertNotNull(event.before());
        assertNull(event.after());
        assertEquals(OperationType.DELETE, event.operation());
        assertEquals(101L, event.getPrimaryKeyValue());
    }

    @Test
    @DisplayName("Should guarantee immutability through defensive copying and unmodifiable wrapper")
    void immutabilityAndDefensiveCopying() {
        Map<String, Object> mutableMap = new HashMap<>();
        mutableMap.put("id", 1L);
        mutableMap.put("status", "PENDING");

        ChangeEvent event = new ChangeEvent(
                "public",
                "orders",
                OperationType.INSERT,
                100L,
                Instant.now(),
                null,
                mutableMap,
                "id"
        );

        // 1. Modifying the original external map must NOT affect the event
        mutableMap.put("status", "CANCELLED");
        assertEquals("PENDING", event.after().get("status"), "Event should not be affected by external map modifications");

        // 2. Modifying the event's internal map must throw UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class, () -> event.after().put("new_key", "value"));
    }

    @Test
    @DisplayName("Should handle nullable column values gracefully in before and after maps")
    void handlesNullColumnValues() {
        Map<String, Object> dataWithNull = new HashMap<>();
        dataWithNull.put("id", 1L);
        dataWithNull.put("middle_name", null);

        ChangeEvent event = new ChangeEvent(
                "public",
                "users",
                OperationType.INSERT,
                200L,
                null, // should default to Instant.now()
                null,
                dataWithNull,
                "id"
        );

        assertNotNull(event.timestamp(), "Null timestamp should default to Instant.now()");
        assertTrue(event.after().containsKey("middle_name"));
        assertNull(event.after().get("middle_name"));
    }

    @Test
    @DisplayName("getPrimaryKeyValue returns null when primary key column is absent or invalid")
    void getPrimaryKeyValueEdgeCases() {
        Map<String, Object> after = Map.of("id", 1L);

        ChangeEvent eventNullPk = new ChangeEvent("public", "t", OperationType.INSERT, 1L, Instant.now(), null, after, null);
        assertNull(eventNullPk.getPrimaryKeyValue());

        ChangeEvent eventBlankPk = new ChangeEvent("public", "t", OperationType.INSERT, 1L, Instant.now(), null, after, "   ");
        assertNull(eventBlankPk.getPrimaryKeyValue());

        ChangeEvent eventMissingPk = new ChangeEvent("public", "t", OperationType.INSERT, 1L, Instant.now(), null, after, "missing_col");
        assertNull(eventMissingPk.getPrimaryKeyValue());
    }
}
