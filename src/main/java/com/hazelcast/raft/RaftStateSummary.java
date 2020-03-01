package com.hazelcast.raft;

/**
 * Contains a summary of a Raft node's state.
 */
public class RaftStateSummary {

    private final Object groupId;
    private final RaftEndpoint localEndpoint;
    private final RaftGroupMembers initialMembers;
    private final RaftRole role;
    private final RaftNodeStatus status;
    private final RaftEndpoint leader;
    private final RaftGroupMembers members;
    private final int term;
    private final long commitIndex;
    private final long lastLogOrSnapshotTerm;
    private final long lastLogOrSnapshotIndex;
    private final long snapshotTerm;
    private final long snapshotIndex;

    public RaftStateSummary(Object groupId, RaftEndpoint localEndpoint, RaftGroupMembers initialMembers, RaftRole role,
                            RaftNodeStatus status, RaftEndpoint leader, RaftGroupMembers members, int term, long commitIndex,
                            long lastLogOrSnapshotTerm, long lastLogOrSnapshotIndex, long snapshotTerm, long snapshotIndex) {
        this.groupId = groupId;
        this.localEndpoint = localEndpoint;
        this.initialMembers = initialMembers;
        this.role = role;
        this.status = status;
        this.leader = leader;
        this.members = members;
        this.term = term;
        this.commitIndex = commitIndex;
        this.lastLogOrSnapshotTerm = lastLogOrSnapshotTerm;
        this.lastLogOrSnapshotIndex = lastLogOrSnapshotIndex;
        this.snapshotTerm = snapshotTerm;
        this.snapshotIndex = snapshotIndex;
    }

    /**
     * Returns the unique ID of the Raft group that this Raft node belongs to
     */
    public Object getGroupId() {
        return groupId;
    }

    /**
     * Returns endpoint of the Raft node
     */
    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    /**
     * Returns initial members of the Raft group.
     */
    public RaftGroupMembers getInitialMembers() {
        return initialMembers;
    }

    /**
     * Returns role of the Raft node in the current term
     */
    public RaftRole getRole() {
        return role;
    }

    /**
     * Returns status of the Raft node
     */
    public RaftNodeStatus getStatus() {
        return status;
    }

    /**
     * Returns the known leader endpoint
     *
     * @see RaftNode#getLeaderEndpoint()
     */
    public RaftEndpoint getLeader() {
        return leader;
    }

    /**
     * Returns the currently effective applied member list of the Raft
     * group this Raft node belongs to.
     * <p>
     * Please note that the returned member list is read from the local
     * state and can be different from the current committed member list
     * of the Raft group, if there is an ongoing (appended but not-yet
     * committed) membership change in the group or the Raft node has
     * fallen behind the current Raft group leader that has committed
     * new membership changes.
     *
     * @see RaftNode#getEffectiveMembers()
     */
    public RaftGroupMembers getMembers() {
        return members;
    }

    /**
     * Returns the current term
     */
    public int getTerm() {
        return term;
    }

    /**
     * Returns index of the highest log entry known to be committed
     */
    public long getCommitIndex() {
        return commitIndex;
    }

    /**
     * Returns the last term in the Raft log,
     * either from the last log entry or from the last snapshot
     */
    public long getLastLogOrSnapshotTerm() {
        return lastLogOrSnapshotTerm;
    }

    /**
     * Returns the last log entry index in the Raft log,
     * either from the last log entry or from the last snapshot
     */
    public long getLastLogOrSnapshotIndex() {
        return lastLogOrSnapshotIndex;
    }

    /**
     * Returns term of the last snapshot
     */
    public long getSnapshotTerm() {
        return snapshotTerm;
    }

    /**
     * Returns log index of the last snapshot
     */
    public long getSnapshotIndex() {
        return snapshotIndex;
    }

    @Override
    public String toString() {
        return "RaftStateSummary{" + "groupId=" + groupId + ", localEndpoint=" + localEndpoint + ", initialMembers="
                + initialMembers + ", role=" + role + ", status=" + status + ", leader=" + leader + ", members=" + members
                + ", term=" + term + ", commitIndex=" + commitIndex + ", lastLogOrSnapshotTerm=" + lastLogOrSnapshotTerm
                + ", lastLogOrSnapshotIndex=" + lastLogOrSnapshotIndex + ", snapshotTerm=" + snapshotTerm + ", snapshotIndex="
                + snapshotIndex + '}';
    }

}
