package com.periscope.cdc;

import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Manages the lifecycle of PostgreSQL logical replication slots and publications.
 * Ensures slots and publications are created idempotently without throwing duplicate errors.
 */
public class ReplicationSlotManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationSlotManager.class);

    private final PostgresConnectionFactory connectionFactory;

    public ReplicationSlotManager(PostgresConnectionFactory connectionFactory) {
        if (connectionFactory == null) throw new IllegalArgumentException("connectionFactory cannot be null");
        this.connectionFactory = connectionFactory;
    }

    /**
     * Checks whether a logical replication slot with the specified name already exists.
     * Uses a regular SQL query against the pg_replication_slots system catalog.
     */
    public boolean slotExists(Connection conn, String slotName) throws SQLException {
        String query = "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?";
        try (PreparedStatement statement = conn.prepareStatement(query)) {
            statement.setString(1, slotName);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Creates a logical replication slot if it does not already exist.
     * Uses regularConn to check existence and pgConn's ReplicationAPI to create the slot.
     */
    public void createSlotIfMissing(Connection regularConn, PGConnection pgConn, String slotName, String outputPlugin) throws SQLException {
        boolean slotExists = slotExists(regularConn, slotName);
        if (slotExists) {
            log.info("Slot {} already exists", slotName);
        } else {
            pgConn.getReplicationAPI().createReplicationSlot().logical().withSlotName(slotName).withOutputPlugin(outputPlugin).make();
            log.info("Slot {} created", slotName);
        }
    }

    /**
     * Drops a replication slot using the Postgres Replication API.
     */
    public void dropSlot(PGConnection pgConn, String slotName) throws SQLException {
        pgConn.getReplicationAPI().dropReplicationSlot(slotName);
        log.info("Slot {} dropped", slotName);
    }

    /**
     * Checks whether a publication with the specified name exists in pg_publication.
     */
    public boolean publicationExists(Connection conn, String pubName) throws SQLException {
        String query = "SELECT 1 FROM pg_publication WHERE pubname = ?";
        try (PreparedStatement statement = conn.prepareStatement(query)) {
            statement.setString(1, pubName);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Creates a publication for all tables if it does not already exist.
     */
    public void createPublicationIfMissing(Connection conn, String pubName) throws SQLException {
        if (pubName == null || !pubName.matches("^[a-zA-Z0-9_]+$")) {
            throw new IllegalArgumentException("Invalid publication name: " + pubName);
        }
        boolean publicationExists = publicationExists(conn, pubName);
        if (publicationExists) {
            log.info("Publication {} already exists", pubName);
        } else {
            String query = "CREATE PUBLICATION " + pubName + " FOR ALL TABLES";
            try (Statement statement = conn.createStatement()) {
                statement.execute(query);
                log.info("Publication {} created", pubName);
            }
        }
    }

    // =========================================================================
    // Convenience methods that manage connection lifecycles automatically
    // using the configured PostgresConnectionFactory
    // =========================================================================

    /**
     * Checks if a replication slot exists by opening and closing a temporary connection.
     */
    public boolean slotExists(String slotName) throws SQLException {
        try (Connection conn = connectionFactory.createConnection()) {
            return slotExists(conn, slotName);
        }
    }

    /**
     * Creates a logical replication slot if missing, handling connections automatically.
     */
    public void createSlotIfMissing(String slotName, String outputPlugin) throws SQLException {
        try (Connection regularConn = connectionFactory.createConnection()) {
            if (slotExists(regularConn, slotName)) {
                log.info("Replication slot '{}' already exists", slotName);
                return;
            }
            PGConnection pgConn = connectionFactory.createReplicationConnection();
            try {
                createSlotIfMissing(regularConn, pgConn, slotName, outputPlugin);
            } finally {
                if (pgConn instanceof Connection conn) {
                    conn.close();
                }
            }
        }
    }

    /**
     * Drops a replication slot if it exists, handling connections automatically.
     */
    public void dropSlotIfExists(String slotName) throws SQLException {
        try (Connection regularConn = connectionFactory.createConnection()) {
            if (!slotExists(regularConn, slotName)) {
                log.info("Replication slot '{}' does not exist, nothing to drop", slotName);
                return;
            }
            PGConnection pgConn = connectionFactory.createReplicationConnection();
            try {
                dropSlot(pgConn, slotName);
            } finally {
                if (pgConn instanceof Connection conn) {
                    conn.close();
                }
            }
        }
    }

    /**
     * Checks if a publication exists, handling connection automatically.
     */
    public boolean publicationExists(String pubName) throws SQLException {
        try (Connection conn = connectionFactory.createConnection()) {
            return publicationExists(conn, pubName);
        }
    }

    /**
     * Creates a publication for all tables if missing, handling connection automatically.
     */
    public void createPublicationIfMissing(String pubName) throws SQLException {
        try (Connection conn = connectionFactory.createConnection()) {
            createPublicationIfMissing(conn, pubName);
        }
    }
}
