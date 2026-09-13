package com.periscope.consensus;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class RaftConsensusEngineTest {

    @Test
    void testNodeBecomesCandidateAndRequestsVotesWhenTimerExpires() throws Exception {
        RaftTransport mockTransport = mock(RaftTransport.class);
        
        // Mock the transport to grant the vote!
        when(mockTransport.sendRequestVote(anyString(), any(RaftMessage.RequestVote.class)))
                .thenReturn(new RaftMessage.RequestVoteResponse(1, true));

        // Create engine with 2 peers (total 3 nodes, majority is 2)
        RaftConsensusEngine engine = new RaftConsensusEngine("node-1", List.of("peer1", "peer2"), mockTransport);
        
        // Expose a way to manually trigger startElection for testing, or use reflection
        // For simplicity in this test, we will just wait for the timer to expire naturally
        engine.start();

        // Wait a little longer than the max random timeout (300ms)
        Thread.sleep(500);

        // Verify that the transport sent a RequestVote to peer1 and peer2
        verify(mockTransport, atLeastOnce()).sendRequestVote(eq("peer1"), any(RaftMessage.RequestVote.class));
        verify(mockTransport, atLeastOnce()).sendRequestVote(eq("peer2"), any(RaftMessage.RequestVote.class));
        
        // Verify that after getting the votes, the node started sending heartbeats!
        verify(mockTransport, atLeastOnce()).sendHeartbeat(eq("peer1"), any(RaftMessage.AppendEntries.class));
    }
}
