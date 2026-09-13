package com.periscope.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.periscope.cdc.CdcStreamConsumer;
import com.periscope.cdc.PostgresConnectionFactory;
import com.periscope.cdc.ReplicationSlotManager;
import com.periscope.cdc.WalMessageParser;
import com.periscope.config.DatabaseConfig;
import com.periscope.config.KafkaConfig;
import com.periscope.kafka.EventSerializer;
import com.periscope.kafka.KafkaChangePublisher;
import com.periscope.kafka.TopicRouter;
import com.periscope.pipeline.CdcPipelineCoordinator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Full PostgreSQL logical-replication to Kafka ordering verification. */
class DataIntegrityE2ETest {

    private static final String TOPIC = "periscope.public.customers";
    private static final int CUSTOMER_ID = 100;

    @Test
    void insertUpdateDeleteArrivesInOrderWithStablePartitionKey() throws Exception {
        assumeTrue(isReachable("localhost", 5432), "PostgreSQL is not running on localhost:5432");
        assumeTrue(isReachable("localhost", 9092), "Kafka is not running on localhost:9092");

        DatabaseConfig databaseConfig = new DatabaseConfig(
                "localhost", 5432, "periscope_db", "postgres", "periscope",
                "periscope_e2e_slot", "periscope_pub");
        KafkaConfig kafkaConfig = new KafkaConfig("localhost:9092", "all", 3, "periscope");
        PostgresConnectionFactory connectionFactory = new PostgresConnectionFactory(databaseConfig);
        ReplicationSlotManager slotManager = new ReplicationSlotManager(connectionFactory);

        ensureTopic(kafkaConfig.bootstrapServers(), TOPIC);
        cleanupCustomer(connectionFactory);

        // Recreate the test slot so mutations from a previous failed run cannot
        // contaminate this run's three-event assertion.
        slotManager.dropSlotIfExists(databaseConfig.slotName());
        slotManager.createPublicationIfMissing(databaseConfig.publicationName());
        slotManager.createSlotIfMissing(databaseConfig.slotName(), "test_decoding");

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapServers());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "periscope-e2e-" + UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        CdcStreamConsumer streamConsumer = new CdcStreamConsumer(connectionFactory, new WalMessageParser());
        KafkaChangePublisher publisher = new KafkaChangePublisher(kafkaConfig, new EventSerializer());
        CdcPipelineCoordinator pipeline = new CdcPipelineCoordinator(
                streamConsumer, new WalMessageParser(), new TopicRouter(kafkaConfig), publisher);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(List.of(TOPIC));
            waitForAssignment(consumer);
            pipeline.start();

            executeOrderedMutations(connectionFactory);
            List<ConsumerRecord<String, String>> received = readThreeEvents(consumer);

            assertEquals(3, received.size(), "Exactly three events should be received");
            assertEquals(List.of("INSERT", "UPDATE", "DELETE"), received.stream()
                    .map(record -> operation(record.value()))
                    .toList());
            assertTrue(received.stream().allMatch(record -> Integer.toString(CUSTOMER_ID).equals(record.key())),
                    "Every event must use the entity primary key as its Kafka partition key");
        } finally {
            pipeline.close();
            cleanupCustomer(connectionFactory);
        }
    }

    private static void executeOrderedMutations(PostgresConnectionFactory factory) throws Exception {
        try (Connection connection = factory.createConnection(); Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.executeUpdate("INSERT INTO customers (id, name, email) " +
                    "VALUES (100, 'John Doe', 'john@example.com')");
            statement.executeUpdate("UPDATE customers SET name = 'Johnathan Doe' WHERE id = 100");
            statement.executeUpdate("DELETE FROM customers WHERE id = 100");
            connection.commit();
        }
    }

    private static List<ConsumerRecord<String, String>> readThreeEvents(
            KafkaConsumer<String, String> consumer) {
        List<ConsumerRecord<String, String>> received = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (received.size() < 3 && System.nanoTime() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records.records(TOPIC)) {
                if (Integer.toString(CUSTOMER_ID).equals(record.key())) {
                    received.add(record);
                }
            }
        }
        return received;
    }

    private static void waitForAssignment(KafkaConsumer<String, String> consumer) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(100));
        }
        assertTrue(!consumer.assignment().isEmpty(), "Kafka consumer did not receive a partition assignment");
    }

    private static void ensureTopic(String bootstrapServers, String topic) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(properties)) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e;
                }
            }
        }
    }

    private static void cleanupCustomer(PostgresConnectionFactory factory) throws Exception {
        try (Connection connection = factory.createConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM customers WHERE id = 100 OR email = 'john@example.com'");
        }
    }

    private static String operation(String json) {
        try {
            JsonNode node = new ObjectMapper().readTree(json);
            return node.path("operation").asText();
        } catch (IOException e) {
            throw new AssertionError("Invalid event JSON", e);
        }
    }

    private static boolean isReachable(String host, int port) {
        try (Socket socket = new Socket(host, port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
