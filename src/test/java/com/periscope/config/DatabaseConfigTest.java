package com.periscope.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseConfigTest {

    @Test
    void validConfigShouldCreateSuccessfully() {
        // 1. Arrange & Act
        DatabaseConfig config = new DatabaseConfig(
                "localhost",
                5432,
                "periscope_db",
                "postgres",
                "postgrespassword",
                "periscope_slot",
                "periscope_pub"
        );

        // 2. Assert (testing record component accessors and getJdbcUrl)
        assertEquals("localhost", config.host());
        assertEquals(5432, config.port());
        assertEquals("periscope_db", config.databaseName());
        assertEquals("jdbc:postgresql://localhost:5432/periscope_db", config.getJdbcUrl());
    }

    @Test
    void nullHostShouldThrowException() {
        // Assert that passing null host throws NullPointerException
        assertThrows(NullPointerException.class, () -> new DatabaseConfig(
                null,
                5432,
                "periscope_db",
                "postgres",
                "password",
                "slot",
                "pub"
        ));
    }

    @Test
    void invalidPortShouldThrowException() {
        // Port must be > 0 and <= 65535
        assertThrows(IllegalArgumentException.class, () -> new DatabaseConfig(
                "localhost",
                -1,
                "periscope_db",
                "postgres",
                "password",
                "slot",
                "pub"
        ));

        assertThrows(IllegalArgumentException.class, () -> new DatabaseConfig(
                "localhost",
                70000,
                "periscope_db",
                "postgres",
                "password",
                "slot",
                "pub"
        ));
    }
}