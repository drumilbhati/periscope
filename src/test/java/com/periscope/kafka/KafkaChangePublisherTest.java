package com.periscope.kafka;

import com.periscope.config.KafkaConfig;
import com.periscope.model.ChangeEvent;
import com.periscope.model.OperationType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class KafkaChangePublisherTest {

    private boolean isKafkaRunning() {
        try (Socket socket = new Socket("localhost", 9092)) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void testPublishEventToLocalBroker() {
        // Only run this test if Kafka is actually running locally
        Assumptions.assumeTrue(isKafkaRunning(), "Kafka is not running on localhost:9092, skipping test.");

        KafkaConfig config = new KafkaConfig("localhost:9092", "all", 3, "periscope");
        EventSerializer serializer = new EventSerializer();

        ChangeEvent event = new ChangeEvent(
                "public",
                "users",
                OperationType.INSERT,
                100L,
                Instant.now(),
                null,
                Map.of("id", "99", "email", "test@example.com"),
                "id"
        );

        // 1. Initialize publisher in a try-with-resources block so close() is called automatically
        assertDoesNotThrow(() -> {
            try (KafkaChangePublisher publisher = new KafkaChangePublisher(config, serializer)) {

                // 2. Publish the event
                CompletableFuture<Void> future = publisher.publish("test_topic", "99", event);

                // 3. Block and wait for the network to finish (max 5 seconds)
                future.get(5, TimeUnit.SECONDS);
            }
        });
    }
}