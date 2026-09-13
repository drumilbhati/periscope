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
	/**
	 * RequestVote
	 */
	public record RequestVote(
		long term,
		String candidateId
	) implements RaftMessage {}

	/**
	 * RequestVoteResponse
	 */
	public record RequestVoteResponse(
		long term,
		boolean voteGranted
	) implements RaftMessage {}

	/**
	 * AppendEntries
	 */
	public record AppendEntries(
		long term,
		String leaderId
	) implements RaftMessage {}

	/**
	 * AppendEntriesResponse
	 */
	public record AppendEntriesResponse(
		long term,
		boolean success
	) implements RaftMessage {}
}
