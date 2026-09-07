package com.periscope.cdc;

import com.periscope.config.DatabaseConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ReplicationSlotManagerTest {

    private PostgresConnectionFactory connectionFactory;
    private ReplicationSlotManager slotManager;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig(
                "localhost",
                5432,
                "periscope_db",
                "postgres",
                "periscope",
                "test_slot",
                "test_pub"
        );
        connectionFactory = new PostgresConnectionFactory(config);
        slotManager = new ReplicationSlotManager(connectionFactory);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (isPostgresReachable()) {
            slotManager.dropSlotIfExists("test_lifecycle_slot");
            try (Connection conn = connectionFactory.createConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP PUBLICATION IF EXISTS test_lifecycle_pub");
            }
        }
    }

    @Test
    @DisplayName("Constructor throws IllegalArgumentException when connectionFactory is null")
    void nullFactoryThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new ReplicationSlotManager(null));
    }

    @Test
    @DisplayName("createPublicationIfMissing throws IllegalArgumentException for invalid names")
    void invalidPublicationNameThrowsException() {
        try (Connection conn = connectionFactory.createConnection()) {
            assertThrows(IllegalArgumentException.class, () -> slotManager.createPublicationIfMissing(conn, null));
            assertThrows(IllegalArgumentException.class, () -> slotManager.createPublicationIfMissing(conn, ""));
            assertThrows(IllegalArgumentException.class, () -> slotManager.createPublicationIfMissing(conn, "invalid name"));
            assertThrows(IllegalArgumentException.class, () -> slotManager.createPublicationIfMissing(conn, "invalid;drop table customers;"));
        } catch (SQLException ignored) {
            // Only thrown if Postgres is not reachable when opening connection
        }
    }

    @Test
    @DisplayName("Live test: replication slot lifecycle and idempotency (create, recreate, drop, drop again)")
    void slotLifecycleAndIdempotency() throws SQLException {
        assumeTrue(isPostgresReachable(), "Postgres is not running on localhost:5432 - skipping live integration test");

        String slotName = "test_lifecycle_slot";
        String outputPlugin = "test_decoding";

        // Clean start
        slotManager.dropSlotIfExists(slotName);
        assertFalse(slotManager.slotExists(slotName), "Slot should not exist initially");

        // 1. Create slot
        slotManager.createSlotIfMissing(slotName, outputPlugin);
        assertTrue(slotManager.slotExists(slotName), "Slot should exist after creation");

        // 2. Re-create slot (must be idempotent, not throw duplicate slot exception)
        assertDoesNotThrow(() -> slotManager.createSlotIfMissing(slotName, outputPlugin));
        assertTrue(slotManager.slotExists(slotName), "Slot should still exist");

        // 3. Drop slot
        slotManager.dropSlotIfExists(slotName);
        assertFalse(slotManager.slotExists(slotName), "Slot should not exist after being dropped");

        // 4. Drop slot again (must be idempotent, not throw exception)
        assertDoesNotThrow(() -> slotManager.dropSlotIfExists(slotName));
    }

    @Test
    @DisplayName("Live test: publication lifecycle and idempotency")
    void publicationLifecycleAndIdempotency() throws SQLException {
        assumeTrue(isPostgresReachable(), "Postgres is not running on localhost:5432 - skipping live integration test");

        String pubName = "test_lifecycle_pub";

        // Clean start
        try (Connection conn = connectionFactory.createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP PUBLICATION IF EXISTS " + pubName);
        }
        assertFalse(slotManager.publicationExists(pubName), "Publication should not exist initially");

        // 1. Create publication
        slotManager.createPublicationIfMissing(pubName);
        assertTrue(slotManager.publicationExists(pubName), "Publication should exist after creation");

        // 2. Re-create publication (must be idempotent, not throw duplicate publication error)
        assertDoesNotThrow(() -> slotManager.createPublicationIfMissing(pubName));
        assertTrue(slotManager.publicationExists(pubName), "Publication should still exist");
    }

    @Test
    @DisplayName("Live test: pre-existing publication from init.sql is detected")
    void preExistingPublicationFromInitSql() throws SQLException {
        assumeTrue(isPostgresReachable(), "Postgres is not running on localhost:5432 - skipping live integration test");

        // In init.sql, periscope_pub was created on startup
        assertTrue(slotManager.publicationExists("periscope_pub"), "periscope_pub from init.sql should exist");
        assertDoesNotThrow(() -> slotManager.createPublicationIfMissing("periscope_pub"));
    }

    private boolean isPostgresReachable() {
        try (Socket socket = new Socket("localhost", 5432)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
