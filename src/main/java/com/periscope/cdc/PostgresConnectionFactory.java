package com.periscope.cdc;

import com.periscope.config.DatabaseConfig;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Factory responsible for creating PostgreSQL JDBC connections, including
 * regular query connections and streaming replication connections.
 */
public class PostgresConnectionFactory {

    private final DatabaseConfig config;

    public PostgresConnectionFactory(DatabaseConfig config) {
        if (config == null) throw new IllegalArgumentException("config cannot be null");
        this.config = config;
    }

    /**
     * Creates a standard JDBC Connection for regular SQL queries (e.g. checking slots, DDL).
     */
    public Connection createConnection() throws SQLException {
        Properties props = new Properties();
        PGProperty.USER.set(props, config.user());
        PGProperty.PASSWORD.set(props, config.password());

        return DriverManager.getConnection(config.getJdbcUrl(), props);
    }

    /**
     * Creates a connection configured for PostgreSQL logical streaming replication.
     */
    public PGConnection createReplicationConnection() throws SQLException {
        Properties props = createReplicationProperties();
        Connection conn = DriverManager.getConnection(config.getJdbcUrl(), props);
        return conn.unwrap(PGConnection.class);
    }

    /**
     * Helper method to build properties configured for streaming replication.
     * Package-private for unit testing.
     */
    Properties createReplicationProperties() {
        Properties props = new Properties();
        PGProperty.USER.set(props, config.user());
        PGProperty.PASSWORD.set(props, config.password());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        return props;
    }

    public DatabaseConfig getConfig() {
        return config;
    }
}
