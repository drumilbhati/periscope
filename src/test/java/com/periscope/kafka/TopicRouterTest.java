package com.periscope.kafka;

import com.periscope.config.KafkaConfig;
import com.periscope.model.ChangeEvent;
import com.periscope.model.OperationType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TopicRouterTest {

    @Test
    void testRoutingWithPrimaryKey() {
        // Arrange
        KafkaConfig config = new KafkaConfig("localhost:9092", "all", 3, "test_prefix");
        TopicRouter router = new TopicRouter(config);
        
        ChangeEvent event = new ChangeEvent(
                "public",
                "orders",
                OperationType.INSERT,
                100L,
                Instant.now(),
                null,
                Map.of("order_id", 404, "amount", 50.0),
                "order_id"
        );

        // Act
        TopicRouter.RoutingDecision decision = router.route(event);
        
        // Assert
        assertEquals("test_prefix.public.orders", decision.topic(), "Topic should match the expected format");
        assertEquals("404", decision.key(), "Key should be extracted and converted to a String");
    }

    @Test
    void testRoutingWithoutPrimaryKey() {
        // Arrange
        KafkaConfig config = new KafkaConfig("localhost:9092", "all", 3, "test_prefix");
        TopicRouter router = new TopicRouter(config);
        
        ChangeEvent event = new ChangeEvent(
                "public",
                "audit_log",
                OperationType.INSERT,
                100L,
                Instant.now(),
                null,
                Map.of("message", "User logged in"),
                null // No PK defined!
        );

        // Act
        TopicRouter.RoutingDecision decision = router.route(event);
        
        // Assert
        assertEquals("test_prefix.public.audit_log", decision.topic(), "Topic should match the expected format");
        assertNull(decision.key(), "Key should be null when no primary key is defined");
    }
}
