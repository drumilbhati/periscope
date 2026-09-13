package com.periscope;

import com.periscope.cdc.CdcStreamConsumer;
import com.periscope.cdc.PostgresConnectionFactory;
import com.periscope.cdc.ReplicationSlotManager;
import com.periscope.config.PeriscopeConfig;
import com.periscope.consensus.LeaderElectionController;
import com.periscope.consensus.RaftConsensusEngine;
import com.periscope.consensus.RaftTransport;
import com.periscope.kafka.EventSerializer;
import com.periscope.kafka.KafkaChangePublisher;
import com.periscope.pipeline.CdcPipelineCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class PeriscopeApp {
    private static final Logger log = LoggerFactory.getLogger(PeriscopeApp.class);

    public static void main(String[] args) {
        try {
            PeriscopeConfig config = PeriscopeConfig.load();
            PostgresConnectionFactory connectionFactory =
                    new PostgresConnectionFactory(config.databaseConfig());
            ReplicationSlotManager slotManager = new ReplicationSlotManager(connectionFactory);
            slotManager.createPublicationIfMissing(config.databaseConfig().publicationName());
            slotManager.createSlotIfMissing(config.databaseConfig().slotName(), "test_decoding");

            CdcStreamConsumer consumer = new CdcStreamConsumer(connectionFactory, new com.periscope.cdc.WalMessageParser());
            KafkaChangePublisher publisher = new KafkaChangePublisher(
                    config.kafkaConfig(), new EventSerializer());
            CdcPipelineCoordinator pipeline = new CdcPipelineCoordinator(
                    consumer, new com.periscope.cdc.WalMessageParser(),
                    new com.periscope.kafka.TopicRouter(config.kafkaConfig()), publisher);

            String nodeId = environment("PERISCOPE_NODE_ID", "node-1");
            int raftPort = Integer.parseInt(environment("PERISCOPE_RAFT_PORT", "9001"));
            List<String> peers = parsePeers(System.getenv("PERISCOPE_RAFT_PEERS"));
            LeaderElectionController controller = new LeaderElectionController(
                    new RaftConsensusEngine(nodeId, peers, new RaftTransport(raftPort)), pipeline);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    controller.close();
                } catch (Exception e) {
                    log.warn("Error while shutting down Periscope: {}", e.getMessage());
                }
            }, "periscope-shutdown"));

            controller.start();
            log.info("Periscope node {} started on Raft port {}", nodeId, raftPort);
        } catch (Exception e) {
            log.error("Failed to start Periscope", e);
            System.exit(1);
        }
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static List<String> parsePeers(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(peer -> !peer.isBlank())
                .toList();
    }
}
