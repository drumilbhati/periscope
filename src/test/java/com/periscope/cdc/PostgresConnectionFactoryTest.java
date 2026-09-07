package com.periscope.cdc;

import com.periscope.config.DatabaseConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;

import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class PostgresConnectionFactoryTest {

    private DatabaseConfig config;
    private PostgresConnectionFactory factory;

    @BeforeEach
    void setUp() {
        config = new DatabaseConfig(
                "localhost",
                5432,
                "periscope_db",
                "postgres",
                "periscope",
                "periscope_slot",
                "periscope_pub"
        );
        factory = new PostgresConnectionFactory(config);
    }

    @Test
    @DisplayName("Constructor should throw IllegalArgumentException when config is null")
    void nullConfigShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> new PostgresConnectionFactory(null));
    }

    @Test
    @DisplayName("getConfig should return the underlying DatabaseConfig")
    void getConfigShouldReturnProvidedConfig() {
        assertSame(config, factory.getConfig());
    }

    @Test
    @DisplayName("createReplicationProperties should configure all required logical replication settings")
    void createReplicationPropertiesShouldConfigureCorrectSettings() {
        Properties props = factory.createReplicationProperties();

        assertNotNull(props);
        assertEquals("postgres", PGProperty.USER.getOrDefault(props));
        assertEquals("periscope", PGProperty.PASSWORD.getOrDefault(props));
        assertEquals("10", PGProperty.ASSUME_MIN_SERVER_VERSION.getOrDefault(props));
        assertEquals("database", PGProperty.REPLICATION.getOrDefault(props));
        assertEquals("simple", PGProperty.PREFER_QUERY_MODE.getOrDefault(props));
    }

    @Test
    @DisplayName("Live connection test: should establish regular and replication connections when Postgres is up")
    void liveReplicationConnectionShouldSucceedWhenPostgresIsRunning() throws SQLException {
        assumeTrue(isPostgresReachable(), "Postgres is not running on localhost:5432 - skipping live integration test");

        // 1. Verify regular JDBC connection works for standard SQL execution
        try (Connection conn = factory.createConnection()) {
            assertNotNull(conn);
            assertTrue(conn.isValid(2));
            assertFalse(conn.isClosed());
        }

        // 2. Verify logical replication connection unwraps to PGConnection and has replication API
        PGConnection pgConn = factory.createReplicationConnection();
        assertNotNull(pgConn);
        assertNotNull(pgConn.getReplicationAPI());

        // Clean up replication connection
        if (pgConn instanceof Connection conn) {
            conn.close();
            assertTrue(conn.isClosed());
        }
    }

    /**
     * Checks if Postgres port is open on localhost.
     */
    private boolean isPostgresReachable() {
        try (Socket socket = new Socket("localhost", 5432)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
