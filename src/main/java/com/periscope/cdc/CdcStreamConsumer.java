package com.periscope.cdc;

import com.periscope.config.DatabaseConfig;
import com.periscope.model.ChangeEvent;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Consumer loop that reads raw bytes from PostgreSQL's logical replication stream,
 * converts them to ChangeEvents using WalMessageParser, and handles LSN feedback acknowledgments.
 */
public class CdcStreamConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CdcStreamConsumer.class);

    private final PostgresConnectionFactory connectionFactory;
    private final WalMessageParser parser;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private PGConnection pgConnection;
    private PGReplicationStream stream;
    private Thread workerThread;

    public CdcStreamConsumer(PostgresConnectionFactory connectionFactory, WalMessageParser parser) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory cannot be null");
        this.parser = Objects.requireNonNull(parser, "parser cannot be null");
    }

    /**
     * Constructor allowing direct injection of an existing stream (useful for unit testing).
     */
    public CdcStreamConsumer(PGReplicationStream stream, WalMessageParser parser) {
        this.connectionFactory = null;
        this.stream = Objects.requireNonNull(stream, "stream cannot be null");
        this.parser = Objects.requireNonNull(parser, "parser cannot be null");
    }

    /**
     * Starts the CDC streaming consumption loop in a background thread.
     *
     * @param handler the callback function invoked whenever a ChangeEvent is decoded
     * @throws SQLException if establishing the replication connection fails
     */
    public synchronized void start(Consumer<ChangeEvent> handler) throws SQLException {
        Objects.requireNonNull(handler, "handler cannot be null");

        if (running.get()) {
            log.warn("CdcStreamConsumer is already running");
            return;
        }

        if (stream == null) {
            assert connectionFactory != null;
            pgConnection = connectionFactory.createReplicationConnection();
            stream = pgConnection.getReplicationAPI().replicationStream()
                    .logical()
                    .withSlotName(connectionFactory.getConfig().slotName())
                    .withStatusInterval(10, TimeUnit.SECONDS)
                    .start();
        }

        running.set(true);
        workerThread = new Thread(() -> {
            try {
                streamLoop(handler);
            } catch (SQLException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, "periscope-cdc-worker");
        workerThread.start();
    }

    /**
     * The continuous streaming loop executed by the background thread.
     */
    void streamLoop(Consumer<ChangeEvent> handler) throws SQLException, InterruptedException {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                if (stream.isClosed()) {
                    break;
                }
                ByteBuffer buffer = stream.readPending();
                if (buffer == null) {
                    Thread.sleep(10);
                    continue;
                }
                String rawMessage = new String(buffer.array(), buffer.arrayOffset(), buffer.remaining(), StandardCharsets.UTF_8);
                long lsn = stream.getLastReceiveLSN().asLong();
                Optional<ChangeEvent> changeEvent = parser.parse(rawMessage, lsn);
                changeEvent.ifPresent(handler);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (SQLException e) {
                if (!running.get() || stream.isClosed()) {
                    break;
                }
                log.error("Error reading from CDC replication stream: ", e);
            }
        }
    }

    /**
     * Acknowledges flushed LSN back to PostgreSQL so the replication slot can safely advance.
     * This forms the LSN feedback loop.
     *
     * @param lsn the Log Sequence Number that has been successfully processed/persisted
     * @throws SQLException if communicating status update to Postgres fails
     */
    public void acknowledgeLsn(long lsn) throws SQLException {
        if (stream != null && !stream.isClosed()) {
            LogSequenceNumber loglsn = LogSequenceNumber.valueOf(lsn);
            stream.setFlushedLSN(loglsn);
            stream.setAppliedLSN(loglsn);
            stream.forceUpdateStatus();
        }
    }

    /**
     * Stops the streaming consumer and releases all database and thread resources cleanly.
     */
    @Override
    public synchronized void close() throws Exception {
        running.set(false);

        // Close the stream before joining the worker so a blocked readPending()
        // call is interrupted by the driver's close operation.
        if (stream != null && !stream.isClosed()) {
            stream.close();
        }

        if (workerThread != null && workerThread.isAlive()) {
            workerThread.interrupt();
            workerThread.join(2000);
        }
        if (pgConnection instanceof Connection conn && !conn.isClosed()) {
            conn.close();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public PGReplicationStream getStream() {
        return stream;
    }
}
