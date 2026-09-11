package com.periscope.cdc;

import com.periscope.config.DatabaseConfig;
import com.periscope.model.ChangeEvent;
import com.periscope.model.OperationType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CdcStreamConsumerTest {

    private DatabaseConfig config;
    private PostgresConnectionFactory connectionFactory;
    private WalMessageParser parser;
    private CdcStreamConsumer consumer;

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
        connectionFactory = new PostgresConnectionFactory(config);
        parser = new WalMessageParser();
        consumer = new CdcStreamConsumer(connectionFactory, parser);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (consumer != null && consumer.isRunning()) {
            consumer.close();
        }
    }

    @Test
    @DisplayName("Constructor should reject null parameters")
    void constructorNullChecks() {
        assertThrows(NullPointerException.class, () -> new CdcStreamConsumer((PostgresConnectionFactory) null, parser));
        assertThrows(NullPointerException.class, () -> new CdcStreamConsumer(connectionFactory, null));
    }

    @Test
    @DisplayName("start should reject null handler")
    void startNullHandlerCheck() {
        assertThrows(NullPointerException.class, () -> consumer.start(null));
    }

    @Test
    @DisplayName("close without starting should be safe and idempotent")
    void closeWithoutStartIsSafe() {
        assertFalse(consumer.isRunning());
        assertDoesNotThrow(() -> consumer.close());
        assertDoesNotThrow(() -> consumer.close());
        assertFalse(consumer.isRunning());
    }

    @Test
    @DisplayName("Live integration: should stream mutations from PostgreSQL and acknowledge LSN")
    void liveStreamingAndLsnAcknowledgment() throws Exception {
        assumeTrue(isPostgresReachable(), "Postgres is not running on localhost:5432 - skipping live integration test");

        // 1. Ensure replication slot and publication exist
        ReplicationSlotManager slotManager = new ReplicationSlotManager(connectionFactory);
        slotManager.createPublicationIfMissing(config.publicationName());
        slotManager.createSlotIfMissing(config.slotName(), "test_decoding");

        // 2. Start CDC stream consumer collecting events
        List<ChangeEvent> receivedEvents = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        consumer.start(event -> {
            receivedEvents.add(event);
            latch.countDown();
        });

        assertTrue(consumer.isRunning(), "Consumer should be running");

        // 3. Execute an INSERT in Postgres on a separate connection
        try (Connection conn = connectionFactory.createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO customers (name, email) VALUES ('StreamTest User', 'streamtest@example.com')");
        }

        // 4. Await event capture
        boolean eventReceived = latch.await(5, TimeUnit.SECONDS);
        assertTrue(eventReceived, "Should have received a ChangeEvent within 5 seconds");
        assertFalse(receivedEvents.isEmpty());

        ChangeEvent event = receivedEvents.getFirst();
        assertEquals("customers", event.tableName());
        assertEquals(OperationType.INSERT, event.operation());
        assertEquals("StreamTest User", event.after().get("name"));
        assertTrue(event.lsn() > 0);

        // 5. Test LSN acknowledgment
        assertDoesNotThrow(() -> consumer.acknowledgeLsn(event.lsn()));

        // 6. Clean shutdown
        consumer.close();
        assertFalse(consumer.isRunning(), "Consumer should be stopped after close()");
    }

    private boolean isPostgresReachable() {
        try (Socket socket = new Socket("localhost", 5432)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
