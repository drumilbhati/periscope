package com.periscope.cdc;

import com.periscope.model.ChangeEvent;
import com.periscope.model.OperationType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class WalMessageParserTest {

    private WalMessageParser parser;

    @BeforeEach
    void setUp() {
        parser = new WalMessageParser();
    }

    @Test
    @DisplayName("Should return Optional.empty for BEGIN and COMMIT transaction boundaries")
    void transactionBoundariesReturnEmpty() {
        assertTrue(parser.parse("BEGIN 684", 100L).isEmpty());
        assertTrue(parser.parse("COMMIT 684", 101L).isEmpty());
    }

    @Test
    @DisplayName("Should return Optional.empty for null, blank, or invalid messages")
    void invalidMessagesReturnEmpty() {
        assertTrue(parser.parse(null, 100L).isEmpty());
        assertTrue(parser.parse("", 100L).isEmpty());
        assertTrue(parser.parse("   ", 100L).isEmpty());
        assertTrue(parser.parse("random unformatted log message", 100L).isEmpty());
    }

    @Test
    @DisplayName("Should parse INSERT message into ChangeEvent with typed columns and primary key")
    void parseInsertMessage() {
        String raw = "table public.customers: INSERT: id[integer]:1 name[text]:'Alice Smith' email[text]:'alice@example.com' active[boolean]:true balance[numeric]:150.75";
        long lsn = 54321L;

        Optional<ChangeEvent> result = parser.parse(raw, lsn);

        assertTrue(result.isPresent());
        ChangeEvent event = result.get();

        assertEquals("public", event.schemaName());
        assertEquals("customers", event.tableName());
        assertEquals(OperationType.INSERT, event.operation());
        assertEquals(lsn, event.lsn());
        assertNull(event.before(), "INSERT must have null before map");
        assertNotNull(event.after(), "INSERT must have non-null after map");

        // Verify typed column values
        assertEquals(1, event.after().get("id"));
        assertEquals("Alice Smith", event.after().get("name"));
        assertEquals("alice@example.com", event.after().get("email"));
        assertEquals(true, event.after().get("active"));
        assertEquals(150.75, event.after().get("balance"));

        // Primary key extraction
        assertEquals("id", event.primaryKeyColumn());
        assertEquals(1, event.getPrimaryKeyValue());
    }

    @Test
    @DisplayName("Should parse UPDATE message into ChangeEvent with after columns")
    void parseUpdateMessage() {
        String raw = "table public.customers: UPDATE: id[integer]:1 name[text]:'Bob Jones' email[text]:'bob@example.com'";
        long lsn = 54322L;

        Optional<ChangeEvent> result = parser.parse(raw, lsn);

        assertTrue(result.isPresent());
        ChangeEvent event = result.get();

        assertEquals("public", event.schemaName());
        assertEquals("customers", event.tableName());
        assertEquals(OperationType.UPDATE, event.operation());
        assertEquals(lsn, event.lsn());
        assertNull(event.before());
        assertNotNull(event.after());

        assertEquals(1, event.after().get("id"));
        assertEquals("Bob Jones", event.after().get("name"));
        assertEquals("bob@example.com", event.after().get("email"));
        assertEquals(1, event.getPrimaryKeyValue());
    }

    @Test
    @DisplayName("Should parse DELETE message into ChangeEvent with before columns")
    void parseDeleteMessage() {
        String raw = "table public.customers: DELETE: id[integer]:42";
        long lsn = 54323L;

        Optional<ChangeEvent> result = parser.parse(raw, lsn);

        assertTrue(result.isPresent());
        ChangeEvent event = result.get();

        assertEquals("public", event.schemaName());
        assertEquals("customers", event.tableName());
        assertEquals(OperationType.DELETE, event.operation());
        assertEquals(lsn, event.lsn());
        assertNotNull(event.before(), "DELETE must have non-null before map");
        assertNull(event.after(), "DELETE must have null after map");

        assertEquals(42, event.before().get("id"));
        assertEquals(42, event.getPrimaryKeyValue());
    }

    @Test
    @DisplayName("Should parse columns with null and bigint values correctly")
    void parseNullAndBigintColumns() {
        String raw = "table public.orders: INSERT: id[bigint]:9876543210 customer_id[integer]:5 notes[text]:null";
        long lsn = 54324L;

        Optional<ChangeEvent> result = parser.parse(raw, lsn);

        assertTrue(result.isPresent());
        ChangeEvent event = result.get();

        assertEquals(9876543210L, event.after().get("id"));
        assertEquals(5, event.after().get("customer_id"));
        assertTrue(event.after().containsKey("notes"));
        assertNull(event.after().get("notes"));
    }

    @Test
    @DisplayName("Should parse TRUNCATE message with empty columns")
    void parseTruncateMessage() {
        String raw = "table public.customers: TRUNCATE: ";
        long lsn = 54325L;

        Optional<ChangeEvent> result = parser.parse(raw, lsn);

        assertTrue(result.isPresent());
        ChangeEvent event = result.get();

        assertEquals(OperationType.TRUNCATE, event.operation());
        assertNull(event.before());
        assertNull(event.after());
        assertNull(event.primaryKeyColumn());
        assertNull(event.getPrimaryKeyValue());
    }

    @Test
    @DisplayName("Should handle string values containing spaces")
    void parseStringsWithSpaces() {
        String raw = "table public.products: INSERT: id[integer]:99 description[text]:'A long description with multiple spaces'";

        Optional<ChangeEvent> result = parser.parse(raw, 1L);

        assertTrue(result.isPresent());
        assertEquals("A long description with multiple spaces", result.get().after().get("description"));
    }
}
