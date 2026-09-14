package com.periscope.api;

import com.periscope.cdc.CdcStreamConsumer;
import com.periscope.cdc.PostgresConnectionFactory;
import com.periscope.cdc.ReplicationSlotManager;
import com.periscope.cdc.WalMessageParser;
import com.periscope.config.DatabaseConfig;
import com.periscope.config.KafkaConfig;
import com.periscope.config.PeriscopeConfig;
import com.periscope.kafka.EventSerializer;
import com.periscope.kafka.KafkaChangePublisher;
import com.periscope.kafka.TopicRouter;
import com.periscope.model.ChangeEvent;
import com.periscope.pipeline.CdcPipelineCoordinator;

import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Public embedding API for Periscope's PostgreSQL CDC pipeline.
 *
 * <p>The client owns the replication stream, Kafka producer, and their
 * associated resources. Consumers receive immutable {@link ChangeEvent}
 * values only after the event has been acknowledged by Kafka and the LSN
 * feedback has been sent to PostgreSQL.</p>
 *
 * <pre>{@code
 * PeriscopeClient client = PeriscopeClient.builder()
 *     .database(new DatabaseConfig("localhost", 5432, "app", "postgres",
 *             "secret", "periscope_slot", "periscope_pub"))
 *     .kafka(new KafkaConfig("localhost:9092", "all", 3, "periscope"))
 *     .onChange(event -> System.out.println(event.operation()))
 *     .build();
 * client.start();
 * }</pre>
 */
public final class PeriscopeClient implements AutoCloseable {

    private final DatabaseConfig databaseConfig;
    private final KafkaConfig kafkaConfig;
    private final Consumer<ChangeEvent> eventListener;
    private final boolean createInfrastructure;
    private final String outputPlugin;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private CdcPipelineCoordinator pipeline;

    private PeriscopeClient(Builder builder) {
        this.databaseConfig = builder.databaseConfig;
        this.kafkaConfig = builder.kafkaConfig;
        this.eventListener = builder.eventListener;
        this.createInfrastructure = builder.createInfrastructure;
        this.outputPlugin = builder.outputPlugin;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Creates a client from the same property/environment configuration as the standalone app. */
    public static PeriscopeClient fromConfig(PeriscopeConfig config, Consumer<ChangeEvent> listener) {
        Objects.requireNonNull(config, "config cannot be null");
        return builder()
                .database(config.databaseConfig())
                .kafka(config.kafkaConfig())
                .onChange(listener)
                .build();
    }

    /** Starts the background PostgreSQL replication stream. */
    public synchronized void start() throws SQLException {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("PeriscopeClient is already started");
        }

        PostgresConnectionFactory connectionFactory = new PostgresConnectionFactory(databaseConfig);
        try {
            ReplicationSlotManager slotManager = new ReplicationSlotManager(connectionFactory);
            if (createInfrastructure) {
                slotManager.createPublicationIfMissing(databaseConfig.publicationName());
                slotManager.createSlotIfMissing(databaseConfig.slotName(), outputPlugin);
            }

            CdcStreamConsumer consumer = new CdcStreamConsumer(connectionFactory, new WalMessageParser());
            KafkaChangePublisher publisher = new KafkaChangePublisher(kafkaConfig, new EventSerializer());
            pipeline = new CdcPipelineCoordinator(
                    consumer,
                    new WalMessageParser(),
                    new TopicRouter(kafkaConfig),
                    publisher,
                    eventListener);
            pipeline.start();
        } catch (SQLException | RuntimeException e) {
            started.set(false);
            closeQuietly();
            throw e;
        }
    }

    public boolean isRunning() {
        return started.get() && pipeline != null;
    }

    @Override
    public synchronized void close() throws Exception {
        if (!started.getAndSet(false)) {
            return;
        }
        if (pipeline != null) {
            pipeline.close();
            pipeline = null;
        }
    }

    private void closeQuietly() {
        if (pipeline != null) {
            try {
                pipeline.close();
            } catch (Exception ignored) {
                // Preserve the original startup failure.
            }
            pipeline = null;
        }
    }

    public static final class Builder {
        private DatabaseConfig databaseConfig;
        private KafkaConfig kafkaConfig;
        private Consumer<ChangeEvent> eventListener = event -> { };
        private boolean createInfrastructure = true;
        private String outputPlugin = "test_decoding";

        private Builder() {
        }

        public Builder database(DatabaseConfig databaseConfig) {
            this.databaseConfig = databaseConfig;
            return this;
        }

        public Builder kafka(KafkaConfig kafkaConfig) {
            this.kafkaConfig = kafkaConfig;
            return this;
        }

        public Builder onChange(Consumer<ChangeEvent> eventListener) {
            this.eventListener = Objects.requireNonNull(eventListener, "eventListener cannot be null");
            return this;
        }

        /** Disables idempotent publication setup when infrastructure is managed externally. */
        public Builder createInfrastructure(boolean createInfrastructure) {
            this.createInfrastructure = createInfrastructure;
            return this;
        }

        public Builder outputPlugin(String outputPlugin) {
            if (outputPlugin == null || outputPlugin.isBlank()) {
                throw new IllegalArgumentException("outputPlugin cannot be blank");
            }
            this.outputPlugin = outputPlugin;
            return this;
        }

        public PeriscopeClient build() {
            if (databaseConfig == null) {
                throw new IllegalStateException("database configuration is required");
            }
            if (kafkaConfig == null) {
                throw new IllegalStateException("Kafka configuration is required");
            }
            return new PeriscopeClient(this);
        }
    }
}
