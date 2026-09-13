package com.periscope.consensus;

import com.periscope.consensus.RaftState.Role;
import com.periscope.pipeline.CdcPipelineCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElectionController
	implements RaftConsensusEngine.LeadershipListener, AutoCloseable
{

	private static final Logger log = LoggerFactory.getLogger(
		LeaderElectionController.class
	);

	private final RaftConsensusEngine consensusEngine;
	private final CdcPipelineCoordinator pipelineCoordinator;
	private boolean isPipelineRunning = false;

	public LeaderElectionController(
		RaftConsensusEngine consensusEngine,
		CdcPipelineCoordinator pipelineCoordinator
	) {
		this.consensusEngine = consensusEngine;
		this.pipelineCoordinator = pipelineCoordinator;

		// Register this controller as the listener!
		this.consensusEngine.setListener(this);
	}

	public void start() {
		// Start the consensus engine, which begins the background election timers and network transport
		log.info(
			"Starting LeaderElectionController. Node entering Raft FOLLOWER state."
		);
		consensusEngine.start();
	}

	@Override
	public synchronized void onRoleChanged(RaftState.Role newRole) {
		log.info("Raft Role Changed to: {}", newRole);

		if (newRole == Role.LEADER) {
			try {
				pipelineCoordinator.start();
				isPipelineRunning = true;
			} catch (Exception e) {
				log.error("Failed to start pipeline: {}", e.getMessage());
			}
		} else if (isPipelineRunning) {
			try {
				pipelineCoordinator.close();
				isPipelineRunning = false;
			} catch (Exception e) {
				log.error("Failed to stop pipeline: {}", e.getMessage());
			}
		}

	}

	@Override
	public void close() throws Exception {
		if (isPipelineRunning) {
			pipelineCoordinator.close();
			isPipelineRunning = false;
		}
		consensusEngine.close();
	}
}
