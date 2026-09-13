package com.periscope.consensus;

public class RaftState {

	public enum Role {
		LEADER,
		CANDIDATE,
		FOLLOWER,
	}

	private long currentTerm;
	private String votedFor;
	private String leaderId;
	private Role role;

	public long getCurrentTerm() {
		return currentTerm;
	}

	public void setCurrentTerm(long currentTerm) {
		this.currentTerm = currentTerm;
	}

	public String getVotedFor() {
		return votedFor;
	}

	public void setVotedFor(String votedFor) {
		this.votedFor = votedFor;
	}

	public String getLeaderId() {
		return leaderId;
	}

	public void setLeaderId(String leaderId) {
		this.leaderId = leaderId;
	}

	public Role getRole() {
		return role;
	}

	public void setRole(Role role) {
		this.role = role;
	}
}
