package com.hazelcast.raft.impl.state;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftGroupMembers;
import com.hazelcast.raft.RaftNodeStatus;
import com.hazelcast.raft.RaftRole;
import com.hazelcast.raft.RaftStateSummary;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.log.SnapshotEntry;
import com.hazelcast.raft.impl.msg.VoteRequest;
import com.hazelcast.raft.impl.util.InternallyCompletableFuture;

import java.util.Collection;

/**
 * State maintained by each Raft node.
 *
 * @author mdogan
 * @author metanet
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class RaftState {

    /**
     * Unique ID of the Raft group that this Raft node belongs to
     */
    private final Object groupId;

    /**
     * Endpoint of this Raft node
     */
    private final RaftEndpoint localEndpoint;

    /**
     * Initial members of the Raft group
     */
    private final RaftGroupMembers initialMembers;

    /**
     * Latest committed group members of the Raft group.
     */
    private volatile RaftGroupMembers committedGroupMembers;

    /**
     * Latest applied group members of the Raft group.
     * This member may not be committed yet and can be reverted.
     * (initially equal to {@link #committedGroupMembers})
     */
    private volatile RaftGroupMembers lastGroupMembers;

    /**
     * Role of this Raft node.
     */
    private RaftRole role = RaftRole.FOLLOWER;

    /**
     * Latest term this Raft node has seen.
     * (starts with 0 and increases monotonically)
     */
    private int term;

    /**
     * Latest known Raft leader endpoint (or null if not known).
     */
    private volatile RaftEndpoint leader;

    /**
     * Index of highest log entry known to be committed.
     * (starts with 0 and increases monotonically)
     */
    private long commitIndex;

    /**
     * Index of highest log entry applied to state machine.
     * (starts with 0 and increases monotonically)
     * <p>
     * {@code lastApplied <= commitIndex} condition holds true always.
     */
    private long lastApplied;

    /**
     * Endpoint that this Raft node voted for in the current term,
     * or null if none.
     */
    private RaftEndpoint votedFor;

    /**
     * Raft log entries; each log entry contains command for state machine,
     * and the term when entry was received by Raft group leader.
     * First log index is 1.
     */
    private final RaftLog log;

    /**
     * State maintained by the Raft group leader, or null if this Raft node
     * is not the leader.
     */
    private LeaderState leaderState;

    /**
     * Candidate state maintained during the pre-voting step.
     * Becomes null when pre-voting ends by one of
     * {@link #toCandidate(boolean)}, {@link #toLeader()} or
     * {@link #toFollower(int)} methods is called.
     */
    private CandidateState preCandidateState;

    /**
     * Candidate state maintained during the leader election step.
     * Initialized when this Raft node becomes a candidate via a
     * {@link #toCandidate(boolean)} call and becomes null when
     * the voting ends when {@link #toLeader()} or {@link #toFollower(int)}
     * is called.
     */
    private CandidateState candidateState;

    /**
     * State maintained by the Raft group leader during leadership transfer.
     */
    private LeadershipTransferState leadershipTransferState;

    public RaftState(Object groupId, RaftEndpoint localEndpoint, Collection<RaftEndpoint> members, int logCapacity) {
        this.groupId = groupId;
        this.localEndpoint = localEndpoint;
        RaftGroupMembers groupMembers = new RaftGroupMembers(0, members, localEndpoint);
        this.initialMembers = groupMembers;
        this.committedGroupMembers = groupMembers;
        this.lastGroupMembers = groupMembers;
        this.log = new RaftLog(logCapacity);
    }

    /**
     * Returns the unique ID of the Raft group that this Raft node belongs to.
     */
    public Object groupId() {
        return groupId;
    }

    /**
     * Returns the endpoint of this Raft node.
     */
    public RaftEndpoint localEndpoint() {
        return localEndpoint;
    }

    /**
     * Returns the initial members of the Raft group.
     */
    public RaftGroupMembers initialMembers() {
        return initialMembers;
    }

    /**
     * Returns all members in the last applied group members.
     */
    public Collection<RaftEndpoint> members() {
        return lastGroupMembers.members();
    }

    /**
     * Returns remote members in the last applied group members.
     */
    public Collection<RaftEndpoint> remoteMembers() {
        return lastGroupMembers.remoteMembers();
    }

    /**
     * Returns number of members in the last applied group members.
     */
    public int memberCount() {
        return lastGroupMembers.memberCount();
    }

    /**
     * Returns majority number of the last applied group members.
     */
    public int majority() {
        return lastGroupMembers.majority();
    }

    /**
     * Returns log index of the last applied group members.
     */
    public long membersLogIndex() {
        return lastGroupMembers.index();
    }

    /**
     * Returns the committed group members.
     */
    public RaftGroupMembers committedGroupMembers() {
        return committedGroupMembers;
    }

    /**
     * Returns the last applied group members.
     */
    public RaftGroupMembers lastGroupMembers() {
        return lastGroupMembers;
    }

    /**
     * Returns role of this Raft node.
     */
    public RaftRole role() {
        return role;
    }

    /**
     * Returns the latest term this Raft node has seen.
     */
    public int term() {
        return term;
    }

    /**
     * Returns endpoint of the known leader in the current term.
     */
    public RaftEndpoint leader() {
        return leader;
    }

    /**
     * Returns the endpoint this Raft note voted for in the current term.
     */
    public Object votedFor() {
        return votedFor;
    }

    /**
     * Updates the known leader to the given endpoint.
     */
    public void leader(RaftEndpoint endpoint) {
        leader = endpoint;
    }

    /**
     * Returns index of the highest log entry known to be committed.
     */
    public long commitIndex() {
        return commitIndex;
    }

    /**
     * Updates the commit index.
     */
    public void commitIndex(long index) {
        assert index >= commitIndex : "new commit index: " + index + " is smaller than current commit index: " + commitIndex;
        commitIndex = index;
    }

    /**
     * Returns index of the highest log entry applied to state machine
     */
    public long lastApplied() {
        return lastApplied;
    }

    /**
     * Updates the last applied index
     */
    public void lastApplied(long index) {
        assert index >= lastApplied : "new last applied: " + index + " is smaller than current last applied: " + lastApplied;
        lastApplied = index;
    }

    /**
     * Returns the Raft log.
     */
    public RaftLog log() {
        return log;
    }

    /**
     * Returns the leader state.
     */
    public LeaderState leaderState() {
        return leaderState;
    }

    /**
     * Returns the candidate state.
     */
    public CandidateState candidateState() {
        return candidateState;
    }

    /**
     * Persist a vote for the endpoint in current term during leader election.
     */
    public void persistVote(int term, RaftEndpoint member) {
        assert this.term == term;
        assert this.votedFor == null;
        this.votedFor = member;
    }

    /**
     * Switches this Raft node to follower role.
     * Clears leader and (pre)candidate states, updates the term.
     *
     * @param term current term
     */
    public void toFollower(int term) {
        role = RaftRole.FOLLOWER;
        leader = null;
        preCandidateState = null;
        leaderState = null;
        candidateState = null;
        completeLeadershipTransfer(null);
        setTerm(term);
    }

    /**
     * Switches this Raft node to candidate role.
     * Clears pre-candidate and leader states. Initializes candidate state for
     * the current majority and grants a vote for the local endpoint.
     *
     * @return the vote request to sent to other members during leader election
     */
    public VoteRequest toCandidate(boolean disruptive) {
        role = RaftRole.CANDIDATE;
        preCandidateState = null;
        leaderState = null;
        candidateState = new CandidateState(majority());
        candidateState.grantVote(localEndpoint);
        setTerm(term + 1);
        persistVote(term, localEndpoint);

        LogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        return new VoteRequest(localEndpoint, term, lastLogEntry.term(), lastLogEntry.index(), disruptive);
    }

    private void setTerm(int newTerm) {
        assert newTerm >= term : "New term: " + newTerm + ", current term: " + term;
        term = newTerm;
        votedFor = null;
    }

    /**
     * Switches this Raft node to leader role.
     * Sets local endpoint as the known leader. Clears (pre)candidate states
     * and initializes leader state for the current members.
     */
    public void toLeader() {
        role = RaftRole.LEADER;
        leader(localEndpoint);
        preCandidateState = null;
        candidateState = null;
        leaderState = new LeaderState(lastGroupMembers.remoteMembers(), log.lastLogOrSnapshotIndex());
    }

    /**
     * Returns true if the given endpoint is in of the last applied members,
     * false otherwise.
     */
    public boolean isKnownMember(RaftEndpoint endpoint) {
        return lastGroupMembers.isKnownMember(endpoint);
    }

    /**
     * Initializes the pre-candidate state for pre-voting and grants a vote
     * for the local endpoint.
     */
    public void initPreCandidateState() {
        preCandidateState = new CandidateState(majority());
        preCandidateState.grantVote(localEndpoint);
    }

    /**
     * Returns the pre-candidate state.
     */
    public CandidateState preCandidateState() {
        return preCandidateState;
    }

    /**
     * Initializes the last applied group members with the given members
     * and the log Index.
     * <p>
     * This method expects that there's no uncommitted membership changes
     * and the committed members are the same as the last applied members.
     * <p>
     * The leader state is also updated for the members which don't exist
     * in the committed members and the committed members that don't exist
     * in the latest applied members are removed.
     *
     * @param logIndex log index of membership change
     * @param members  latest applied members
     */
    public void updateGroupMembers(long logIndex, Collection<RaftEndpoint> members) {
        assert committedGroupMembers == lastGroupMembers :
                "Cannot update group members to: " + members + " at log index: " + logIndex + " because last group members: "
                        + lastGroupMembers + " is different than committed group members: " + committedGroupMembers;
        assert lastGroupMembers.index() < logIndex :
                "Cannot update group members to: " + members + " at log index: " + logIndex + " because last group members: "
                        + lastGroupMembers + " has a bigger log index.";

        RaftGroupMembers newGroupMembers = new RaftGroupMembers(logIndex, members, localEndpoint);
        committedGroupMembers = lastGroupMembers;
        lastGroupMembers = newGroupMembers;

        if (leaderState != null) {
            members.stream().filter(member -> !committedGroupMembers.isKnownMember(member))
                   .forEach(member -> leaderState.add(member, log.lastLogOrSnapshotIndex()));

            committedGroupMembers.remoteMembers().stream().filter(member -> !members.contains(member))
                                 .forEach(member -> leaderState.remove(member));
        }
    }

    /**
     * Marks the last applied group members as committed.
     * At this point {@link #committedGroupMembers}
     * and {@link #lastGroupMembers} are the same.
     */
    public void commitGroupMembers() {
        assert committedGroupMembers != lastGroupMembers :
                "Cannot commit last group members: " + lastGroupMembers + " because it is same with committed group members";

        committedGroupMembers = lastGroupMembers;
    }

    /**
     * Resets the last group members back to the committed group members.
     * Essentially this means that the applied but not-yet-committed membership
     * change is reverted.
     */
    public void resetGroupMembers() {
        assert this.committedGroupMembers != this.lastGroupMembers;

        this.lastGroupMembers = this.committedGroupMembers;
        // there is no leader state to clean up
    }

    /**
     * Restores group members from the snapshot. Both the committed group
     * members and the last group members are overwritten with the given
     * member list.
     */
    public void restoreGroupMembers(long logIndex, Collection<RaftEndpoint> members) {
        assert lastGroupMembers.index() <= logIndex :
                "Cannot restore group members to: " + members + " at log index: " + logIndex + " because last group members: "
                        + lastGroupMembers + " has a bigger log index.";

        // there is no leader state to clean up

        RaftGroupMembers groupMembers = new RaftGroupMembers(logIndex, members, localEndpoint);
        this.committedGroupMembers = groupMembers;
        this.lastGroupMembers = groupMembers;
    }

    /**
     * Initializes a new leadership transfer process for the given endpoint.
     * Returns {@code true} if the leadership transfer process is initialized
     * with this call, {@code false} if there is already an ongoing leadership
     * transfer process. If there is already an ongoing leadership transfer
     * process, the given future object is also attached to it.
     */
    public boolean initLeadershipTransfer(RaftEndpoint targetEndpoint, final InternallyCompletableFuture<Object> resultFuture) {
        assert lastGroupMembers.members().contains(targetEndpoint);

        if (leadershipTransferState == null) {
            leadershipTransferState = new LeadershipTransferState(term, targetEndpoint, resultFuture);
            return true;
        }

        leadershipTransferState.andThen(targetEndpoint, resultFuture);
        return false;
    }

    /**
     * Returns the current leadership transfer process.
     */
    public LeadershipTransferState leadershipTransferState() {
        return leadershipTransferState;
    }

    /**
     * Completes the current leadership transfer process with the given result
     * and resets it.
     */
    public void completeLeadershipTransfer(Object result) {
        if (leadershipTransferState == null) {
            return;
        }

        leadershipTransferState.complete(result);
        leadershipTransferState = null;
    }

    /**
     * Returns a summary of the current Raft node state.
     */
    public RaftStateSummary summarize(RaftNodeStatus status) {
        LogEntry entry = log.lastLogOrSnapshotEntry();
        SnapshotEntry snapshot = log.snapshot();
        return new RaftStateSummary(groupId, localEndpoint, initialMembers, role, status, leader, lastGroupMembers, term,
                commitIndex, entry.term(), entry.index(), snapshot.term(), snapshot.term());
    }

}
