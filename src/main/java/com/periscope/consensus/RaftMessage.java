package com.periscope.consensus;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Represents RPC messages exchanged between Raft nodes.
 * We use a sealed interface because there are only a strict, known set of message types.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
	@JsonSubTypes.Type(value = RaftMessage.RequestVote.class, name = "RequestVote"),
	@JsonSubTypes.Type(value = RaftMessage.RequestVoteResponse.class, name = "RequestVoteResponse"),
	@JsonSubTypes.Type(value = RaftMessage.AppendEntries.class, name = "AppendEntries"),
	@JsonSubTypes.Type(value = RaftMessage.AppendEntriesResponse.class, name = "AppendEntriesResponse")
})
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
