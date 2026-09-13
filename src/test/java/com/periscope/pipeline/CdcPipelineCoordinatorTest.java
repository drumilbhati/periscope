package com.periscope.pipeline;

import com.periscope.cdc.CdcStreamConsumer;
import com.periscope.cdc.WalMessageParser;
import com.periscope.kafka.KafkaChangePublisher;
import com.periscope.kafka.TopicRouter;
import com.periscope.model.ChangeEvent;
import com.periscope.model.OperationType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class CdcPipelineCoordinatorTest {

    @Test
    void testPipelineSuccessfullyRoutesAndPublishesAndAcknowledges() throws SQLException {
        // 1. Arrange: Mock all dependencies
        CdcStreamConsumer mockConsumer = mock(CdcStreamConsumer.class);
        WalMessageParser mockParser = mock(WalMessageParser.class);
        TopicRouter mockRouter = mock(TopicRouter.class);
        KafkaChangePublisher mockPublisher = mock(KafkaChangePublisher.class);

        CdcPipelineCoordinator coordinator = new CdcPipelineCoordinator(mockConsumer, mockParser, mockRouter, mockPublisher);

        // Setup a fake event and routing decision
        ChangeEvent fakeEvent = new ChangeEvent("public", "users", OperationType.INSERT, 12345L, Instant.now(), null, Map.of("id", "1"), "id");
        TopicRouter.RoutingDecision fakeDecision = new TopicRouter.RoutingDecision("topic.users", "1");
        
        when(mockRouter.route(fakeEvent)).thenReturn(fakeDecision);
        
        // Return a successfully completed future from the publisher
        when(mockPublisher.publish(anyString(), anyString(), any(ChangeEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        // 2. Act: Start the coordinator
        coordinator.start();

        // Capture the lambda that the coordinator passed to the consumer
        ArgumentCaptor<Consumer<ChangeEvent>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockConsumer).start(captor.capture());
        
        Consumer<ChangeEvent> pipelineLambda = captor.getValue();
        
        // Simulate Postgres sending an event to our lambda
        pipelineLambda.accept(fakeEvent);

        // 3. Assert: Verify the exact sequence of events
        verify(mockRouter).route(fakeEvent);
        verify(mockPublisher).publish("topic.users", "1", fakeEvent);
        
        // Crucial check: verify that acknowledgeLsn was called precisely because the future succeeded!
        verify(mockConsumer).acknowledgeLsn(12345L);
    }

    @Test
    void testPipelineDoesNotAcknowledgeIfPublishFails() throws SQLException {
        CdcStreamConsumer mockConsumer = mock(CdcStreamConsumer.class);
        TopicRouter mockRouter = mock(TopicRouter.class);
        KafkaChangePublisher mockPublisher = mock(KafkaChangePublisher.class);

        CdcPipelineCoordinator coordinator = new CdcPipelineCoordinator(mockConsumer, mock(WalMessageParser.class), mockRouter, mockPublisher);

        ChangeEvent fakeEvent = new ChangeEvent("public", "users", OperationType.INSERT, 99999L, Instant.now(), null, Map.of(), null);
        when(mockRouter.route(fakeEvent)).thenReturn(new TopicRouter.RoutingDecision("topic", "key"));

        // Simulate Kafka Network Failure!
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka Broker Down!"));
        when(mockPublisher.publish(anyString(), anyString(), any(ChangeEvent.class))).thenReturn(failedFuture);

        coordinator.start();

        ArgumentCaptor<Consumer<ChangeEvent>> captor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockConsumer).start(captor.capture());
        
        // Trigger event
        captor.getValue().accept(fakeEvent);

        // Verify it published...
        verify(mockPublisher).publish("topic", "key", fakeEvent);
        
        // BUT verify it NEVER acknowledged the LSN to Postgres!
        verify(mockConsumer, never()).acknowledgeLsn(anyLong());
    }
}
