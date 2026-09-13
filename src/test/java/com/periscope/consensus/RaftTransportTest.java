package com.periscope.consensus;

import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

class RaftTransportTest {

    @Test
    void testRequestVoteExchange() throws Exception {
        // 1. Arrange: Start a Transport acting as the "Server" on port 9093
        AtomicBoolean handlerCalled = new AtomicBoolean(false);
        
        try (RaftTransport server = new RaftTransport(9093)) {
            server.startListening(request -> {
                handlerCalled.set(true);
                // When we receive a RequestVote, we reply with a RequestVoteResponse
                assertTrue(request instanceof RaftMessage.RequestVote);
                RaftMessage.RequestVote req = (RaftMessage.RequestVote) request;
                
                assertEquals(5L, req.term());
                assertEquals("node-1", req.candidateId());
                
                return new RaftMessage.RequestVoteResponse(5L, true);
            });

            // 2. Act: Start a Transport acting as the "Client" and send a message to the Server
            try (RaftTransport client = new RaftTransport(0)) { // Port 0 means random ephemeral port
                RaftMessage.RequestVote request = new RaftMessage.RequestVote(5L, "node-1");
                
                // Send it to localhost:9093
                RaftMessage.RequestVoteResponse response = client.sendRequestVote("localhost:9093", request);
                
                // 3. Assert: Verify the response came back correctly
                assertNotNull(response);
                assertEquals(5L, response.term());
                assertTrue(response.voteGranted());
            }
        }
        
        assertTrue(handlerCalled.get(), "The server handler should have been called!");
    }
}
