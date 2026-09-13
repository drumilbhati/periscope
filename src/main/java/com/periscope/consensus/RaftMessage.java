package com.periscope.consensus;

/**
 * Represents RPC messages exchanged between Raft nodes.
 * We use a sealed interface because there are only a strict, known set of message types.
 */
public sealed interface RaftMessage
	permits
		RaftMessage.RequestVote,
		RaftMessage.RequestVoteResponse,
		RaftMessage.AppendEntries,
		RaftMessage.AppendEntriesResponse
{
	// TODO 1: Create a record 'RequestVote' that implements RaftMessage.
	// Fields: long term, String candidateId
	// (Used when a candidate wants to become leader)
	/**
	 * RequestVote
	 */
	public record RequestVote(
		long term,
		String candidateId
	) implements RaftMessage {}

	// TODO 2: Create a record 'RequestVoteResponse' that implements RaftMessage.
	// Fields: long term, boolean voteGranted
	// (Used to reply to a RequestVote)
	/**
	 * RequestVoteResponse
	 */
	public record RequestVoteResponse(
		long term,
		boolean voteGranted
	) implements RaftMessage {}

	// TODO 3: Create a record 'AppendEntries' that implements RaftMessage.
	// Fields: long term, String leaderId
	// (In our simplified CDC system, we don't actually replicate logs through Raft.
	// We just use empty AppendEntries as Heartbeats to maintain leadership.)
	/**
	 * AppendEntries
	 */
	public record AppendEntries(
		long term,
		String leaderId
	) implements RaftMessage {}

	// TODO 4: Create a record 'AppendEntriesResponse' that implements RaftMessage.
	// Fields: long term, boolean success
	/**
	 * AppendEntriesResponse
	 */
	public record AppendEntriesResponse(
		long term,
		boolean success
	) implements RaftMessage {}
}
