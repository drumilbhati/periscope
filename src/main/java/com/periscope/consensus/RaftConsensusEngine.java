package com.periscope.consensus;

import com.periscope.consensus.RaftState.Role;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftConsensusEngine {

	private static final Logger log = LoggerFactory.getLogger(
		RaftConsensusEngine.class
	);

	private final String nodeId;
	private final List<String> peerAddresses;
	private final RaftState state;
	private final RaftTransport transport;

	private final ScheduledExecutorService timer =
		Executors.newSingleThreadScheduledExecutor();
	private ScheduledFuture<?> electionTimeoutTask;
	private ScheduledFuture<?> heartbeatTask;
	private final Random random = new Random();

	public interface LeadershipListener {
		void onRoleChanged(RaftState.Role newRole);
	}

	private LeadershipListener listener;

	public RaftConsensusEngine(
		String nodeId,
		List<String> peerAddresses,
		RaftTransport transport
	) {
		this.nodeId = nodeId;
		this.peerAddresses = peerAddresses;
		this.transport = transport;
		this.state = new RaftState();
		this.state.setRole(RaftState.Role.FOLLOWER);
	}

	public void setListener(LeadershipListener listener) {
		this.listener = listener;
	}

	private void changeRole(RaftState.Role newRole) {
		if (this.state.getRole() != newRole) {
			this.state.setRole(newRole);
			if (this.listener != null) {
				this.listener.onRoleChanged(newRole);
			}
		}
	}

	public void start() {
		// Start listening for incoming messages from peers
		try {
			transport.startListening(this::handleIncomingMessage);
		} catch (Exception e) {
			log.error("Failed to start transport server", e);
		}

		resetElectionTimer();
	}

	/**
	 * Resets the countdown timer. If this timer hits 0 before we hear from a Leader,
	 * we start an election!
	 */
	private synchronized void resetElectionTimer() {
		if (electionTimeoutTask != null) {
			electionTimeoutTask.cancel(false);
		}
		// Raft requires randomized timeouts (e.g., 150ms to 300ms) so nodes don't vote at the exact same time.
		int timeoutMs = 150 + random.nextInt(150);

		electionTimeoutTask = timer.schedule(
			this::startElection,
			timeoutMs,
			TimeUnit.MILLISECONDS
		);
	}

	/**
	 * Called when the election timer expires.
	 */
	private synchronized void startElection() {
		changeRole(Role.CANDIDATE);
		this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);
		this.state.setVotedFor(this.nodeId);

		log.info(
			"Node {} starting election for term {}",
			nodeId,
			state.getCurrentTerm()
		);

		// We always vote for ourselves!
		AtomicInteger votesReceived = new AtomicInteger(1);
		int majority = (peerAddresses.size() + 1) / 2 + 1; // e.g. 2 out of 3

		// Ask all peers for a vote
		RaftMessage.RequestVote request = new RaftMessage.RequestVote(
			state.getCurrentTerm(),
			nodeId
		);

		for (String peer : peerAddresses) {
			Thread.ofVirtual().start(() -> {
				try {
					RaftMessage.RequestVoteResponse response =
						transport.sendRequestVote(peer, request);
					// If votesReceived hits majority, call becomeLeader()!
					if (response.voteGranted()) {
						int currentVotes = votesReceived.incrementAndGet();
						if (currentVotes == majority) {
							becomeLeader();
						}
					}
				} catch (Exception e) {
					// Peer is likely down
					log.warn(
						"Failed to send RequestVote to peer {}: {}",
						peer,
						e.getMessage()
					);
				}
			});
		}

		// A single-node cluster already has a quorum. There are no peer
		// responses that can trigger becomeLeader() in this case.
		if (majority == 1) {
			becomeLeader();
		} else {
			// Restart the timer in case this election results in a tie.
			resetElectionTimer();
		}
	}

	private synchronized void becomeLeader() {
		changeRole(Role.LEADER);
		if (electionTimeoutTask != null) {
			electionTimeoutTask.cancel(false);
		}
		heartbeatTask = timer.scheduleAtFixedRate(
			this::sendHeartbeats,
			0,
			50,
			TimeUnit.MILLISECONDS
		);
		log.info(
			"Node {} became leader for term {}",
			nodeId,
			state.getCurrentTerm()
		);
	}

	private void sendHeartbeats() {
		RaftMessage.AppendEntries heartbeat = new RaftMessage.AppendEntries(
			state.getCurrentTerm(),
			nodeId
		);
		for (String peer : peerAddresses) {
			Thread.ofVirtual().start(() -> {
				try {
					transport.sendHeartbeat(peer, heartbeat);
				} catch (Exception e) {
					// Ignore dead peers
				}
			});
		}
	}

	/**
	 * The master router for all incoming network messages.
	 */
	private synchronized RaftMessage handleIncomingMessage(
		RaftMessage message
	) {
		if (message instanceof RaftMessage.AppendEntries hb) {
			this.state.setCurrentTerm(hb.term());
			changeRole(Role.FOLLOWER);
			this.state.setLeaderId(hb.leaderId());
			resetElectionTimer();
			return new RaftMessage.AppendEntriesResponse(
				state.getCurrentTerm(),
				true
			);
		} else if (message instanceof RaftMessage.RequestVote rv) {
			if (rv.term() > this.state.getCurrentTerm()) {
				this.state.setCurrentTerm(rv.term());
				changeRole(Role.FOLLOWER);
				return new RaftMessage.RequestVoteResponse(
					this.state.getCurrentTerm(),
					true
				);
			}
			// Otherwise, deny the vote (return false).
			return new RaftMessage.RequestVoteResponse(
				state.getCurrentTerm(),
				false
			);
		}

		throw new IllegalArgumentException("Unknown message type");
	}

	public void close() throws Exception {
		timer.shutdownNow();
		transport.close();
	}
}
