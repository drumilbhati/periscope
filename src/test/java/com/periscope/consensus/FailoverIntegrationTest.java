package com.periscope.consensus;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailoverIntegrationTest {

    @Test
    void testClusterLeaderElectionAndFailover() throws Exception {
        List<Integer> ports = reservePorts(3);
        List<String> addresses = ports.stream()
                .map(port -> "localhost:" + port)
                .toList();
        CountDownLatch initialLeaderElected = new CountDownLatch(1);

        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < addresses.size(); i++) {
            List<String> peers = new ArrayList<>(addresses);
            peers.remove(i);
            nodes.add(new Node("node-" + (i + 1), peers, ports.get(i), initialLeaderElected));
        }

        try {
            nodes.forEach(Node::start);

            assertTrue(initialLeaderElected.await(1, TimeUnit.SECONDS),
                    "A cluster leader should be elected within 1 second");
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(1, activeLeaders(nodes)));

            Node initialLeader = nodes.stream()
                    .filter(node -> node.observer.isLeader())
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No leader was elected"));
            int leadershipEventsBeforeFailure = totalLeadershipEvents(nodes);

            initialLeader.close();

            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(1, activeLeaders(nodes));
                if (totalLeadershipEvents(nodes) <= leadershipEventsBeforeFailure) {
                    throw new AssertionError("No follower became leader after leader failure");
                }
            });
        } finally {
            nodes.forEach(Node::closeQuietly);
        }
    }

    private static int activeLeaders(List<Node> nodes) {
        return (int) nodes.stream()
                .filter(node -> !node.closed && node.observer.isLeader())
                .count();
    }

    private static int totalLeadershipEvents(List<Node> nodes) {
        return nodes.stream()
                .mapToInt(node -> node.observer.leadershipEvents())
                .sum();
    }

    private static List<Integer> reservePorts(int count) throws IOException {
        List<ServerSocket> sockets = new ArrayList<>();
        try {
            for (int i = 0; i < count; i++) {
                sockets.add(new ServerSocket(0));
            }
            return sockets.stream().map(ServerSocket::getLocalPort).toList();
        } finally {
            for (ServerSocket socket : sockets) {
                socket.close();
            }
        }
    }

    private static final class Node implements AutoCloseable {
        private final RaftConsensusEngine engine;
        private final LeadershipObserver observer = new LeadershipObserver();
        private volatile boolean closed;

        private Node(String nodeId, List<String> peers, int port, CountDownLatch leaderElected) {
            this.engine = new RaftConsensusEngine(nodeId, peers, new RaftTransport(port));
            this.observer.leaderElected = leaderElected;
            this.engine.setListener(observer);
        }

        private void start() {
            engine.start();
        }

        @Override
        public void close() throws Exception {
            if (!closed) {
                closed = true;
                engine.close();
            }
        }

        private void closeQuietly() {
            try {
                close();
            } catch (Exception ignored) {
                // Preserve the original assertion while still cleaning up all nodes.
            }
        }
    }

    private static final class LeadershipObserver implements RaftConsensusEngine.LeadershipListener {
        private final Set<RaftState.Role> roles = ConcurrentHashMap.newKeySet();
        private final List<RaftState.Role> transitions = new CopyOnWriteArrayList<>();
        private CountDownLatch leaderElected;

        @Override
        public void onRoleChanged(RaftState.Role newRole) {
            roles.removeIf(role -> role == RaftState.Role.LEADER);
            roles.add(newRole);
            transitions.add(newRole);
            if (newRole == RaftState.Role.LEADER) {
                leaderElected.countDown();
            }
        }

        private boolean isLeader() {
            return roles.contains(RaftState.Role.LEADER);
        }

        private int leadershipEvents() {
            return (int) transitions.stream()
                    .filter(role -> role == RaftState.Role.LEADER)
                    .count();
        }
    }
}
