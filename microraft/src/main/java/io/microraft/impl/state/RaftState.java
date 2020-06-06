/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright 2020, MicroRaft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.microraft.impl.state;

import io.microraft.RaftEndpoint;
import io.microraft.RaftRole;
import io.microraft.exception.NotLeaderException;
import io.microraft.exception.RaftException;
import io.microraft.impl.log.RaftLog;
import io.microraft.impl.log.SnapshotChunkCollector;
import io.microraft.impl.util.OrderedFuture;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.persistence.NopRaftStore;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;

import java.io.IOException;
import java.util.Collection;

import static io.microraft.RaftRole.CANDIDATE;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEADER;
import static io.microraft.model.log.SnapshotEntry.isNonInitial;
import static java.util.Objects.requireNonNull;

/**
 * State maintained by each Raft node.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public final class RaftState {

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
     * <p>
     * [PERSISTENT]
     */
    private final RaftGroupMembersState initialMembers;

    /**
     * Used for reflecting persistent-state changes to persistent storage.
     */
    private final RaftStore store;
    /**
     * Raft log entries; each log entry contains command for state machine,
     * and the term when entry was received by Raft group leader.
     * First log index is 1.
     */
    private final RaftLog log;
    /**
     * Latest committed group members of the Raft group.
     */
    private volatile RaftGroupMembersState committedGroupMembers;
    /**
     * Latest applied group members of the Raft group.
     * This member may not be committed yet and can be reverted.
     * (initially equal to {@link #committedGroupMembers})
     */
    private volatile RaftGroupMembersState effectiveGroupMembers;
    /**
     * Role of this Raft node.
     */
    private volatile RaftRole role = FOLLOWER;
    /**
     * Latest term this Raft node has seen along with the
     * latest known Raft leader endpoint (or null if not known).
     */
    private volatile RaftTermState termState;
    /**
     * Index of highest log entry known to be committed.
     * (starts with 0 and increases monotonically)
     * <p>
     * [NOT-PERSISTENT] because we can re-calculate commitIndex after restoring
     * logs.
     */
    private long commitIndex;
    /**
     * Index of highest log entry applied to state machine.
     * (starts with 0 and increases monotonically)
     * <p>
     * {@code lastApplied <= commitIndex} condition holds true always.
     * <p>
     * [NOT-PERSISTENT] because we can apply restored logs and re-calculate
     * lastApplied.
     */
    private long lastApplied;
    /**
     * State maintained by the Raft group leader, or null if this Raft node
     * is not the leader.
     */
    private LeaderState leaderState;

    /**
     * Candidate state maintained during the pre-voting step.
     * Becomes null when pre-voting ends by one of
     * {@link #toCandidate()}, {@link #toLeader()} or
     * {@link #toFollower(int)} methods is called.
     */
    private CandidateState preCandidateState;

    /**
     * Candidate state maintained during the leader election step.
     * Initialized when this Raft node becomes a candidate via a
     * {@link #toCandidate()} call and becomes null when
     * the voting ends when {@link #toLeader()} or {@link #toFollower(int)}
     * is called.
     */
    private CandidateState candidateState;

    /**
     * State maintained by the Raft group leader during leadership transfer.
     */
    private LeadershipTransferState leadershipTransferState;

    /**
     * State maintained by followers to keep received snapshot chunks
     * during snapshot installation.
     */
    private SnapshotChunkCollector snapshotChunkCollector;

    private RaftState(Object groupId, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints, int logCapacity,
                      RaftStore store) {
        this.groupId = requireNonNull(groupId);
        this.localEndpoint = requireNonNull(localEndpoint);
        RaftGroupMembersState groupMembers = new RaftGroupMembersState(0, endpoints, localEndpoint);
        this.initialMembers = groupMembers;
        this.committedGroupMembers = groupMembers;
        this.effectiveGroupMembers = groupMembers;
        this.termState = RaftTermState.INITIAL;
        this.store = requireNonNull(store);
        this.log = RaftLog.create(logCapacity, store);
    }

    private RaftState(Object groupId, RestoredRaftState restoredState, int logCapacity, RaftStore store) {
        requireNonNull(restoredState);
        this.groupId = requireNonNull(groupId);
        this.localEndpoint = restoredState.getLocalEndpoint();
        this.initialMembers = new RaftGroupMembersState(0, restoredState.getInitialMembers(), this.localEndpoint);
        this.committedGroupMembers = this.initialMembers;
        this.effectiveGroupMembers = this.committedGroupMembers;
        this.termState = RaftTermState.restore(restoredState.getTerm(), restoredState.getVotedEndpoint());

        SnapshotEntry snapshot = restoredState.getSnapshotEntry();
        if (isNonInitial(snapshot)) {
            RaftGroupMembersState groupMembers = new RaftGroupMembersState(snapshot.getGroupMembersLogIndex(),
                                                                           snapshot.getGroupMembers(), this.localEndpoint);
            this.committedGroupMembers = groupMembers;
            this.effectiveGroupMembers = groupMembers;
            this.commitIndex = snapshot.getIndex();
            this.lastApplied = snapshot.getIndex();
        }

        this.log = RaftLog.restore(logCapacity, snapshot, restoredState.getLogEntries(), store);
        this.store = requireNonNull(store);
    }

    public static RaftState create(Object groupId, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints,
                                   int logCapacity) {
        return create(groupId, localEndpoint, endpoints, logCapacity, new NopRaftStore());
    }

    public static RaftState create(Object groupId, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints,
                                   int logCapacity, RaftStore store) {
        return new RaftState(groupId, localEndpoint, endpoints, logCapacity, store);
    }

    public static RaftState restore(Object groupId, RestoredRaftState restoredState, int logCapacity) {
        return restore(groupId, restoredState, logCapacity, new NopRaftStore());
    }

    public static RaftState restore(Object groupId, RestoredRaftState restoredState, int logCapacity, RaftStore store) {
        return new RaftState(groupId, restoredState, logCapacity, store);
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
    public RaftGroupMembersState initialMembers() {
        return initialMembers;
    }

    /**
     * Returns all members in the last applied group members.
     */
    public Collection<RaftEndpoint> members() {
        return effectiveGroupMembers.getMembers();
    }

    /**
     * Returns remote members in the last applied group members.
     */
    public Collection<RaftEndpoint> remoteMembers() {
        return effectiveGroupMembers.remoteMembers();
    }

    /**
     * Returns number of members in the last applied group members.
     */
    public int memberCount() {
        return effectiveGroupMembers.memberCount();
    }

    /**
     * Returns the committed group members.
     */
    public RaftGroupMembersState committedGroupMembers() {
        return committedGroupMembers;
    }

    /**
     * Returns the last applied group members.
     */
    public RaftGroupMembersState effectiveGroupMembers() {
        return effectiveGroupMembers;
    }

    /**
     * Returns role of this Raft node.
     */
    public RaftRole role() {
        return role;
    }

    /**
     * Returns the latest term information this Raft node has seen.
     */
    public RaftTermState termState() {
        return termState;
    }

    /**
     * Returns endpoint of the known leader in the current term.
     */
    public RaftEndpoint leader() {
        return termState.getLeaderEndpoint();
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
     * Persists the initial member list to the Raft store
     *
     * @throws IOException
     *         if an IO error occurs inside the store
     * @see RaftStore#persistInitialMembers(RaftEndpoint, Collection)
     */
    public void persistInitialMembers()
            throws IOException {
        store.persistInitialMembers(localEndpoint, initialMembers.getMembers());
    }

    /**
     * Switches this Raft node to follower role.
     * Clears leader and (pre)candidate states, updates the term.
     *
     * @param term
     *         current term
     */
    public void toFollower(int term) {
        role = FOLLOWER;
        preCandidateState = null;
        LeaderState currentLeaderState = leaderState;
        leaderState = null;
        candidateState = null;
        completeLeadershipTransfer(null);
        setTerm(term);
        if (currentLeaderState != null) {
            // this is done here to read the updated leader field
            currentLeaderState.queryState().fail(new NotLeaderException(localEndpoint, leader()));
        }
        persistTerm();
    }

    private void setTerm(int newTerm) {
        termState = termState.switchTo(newTerm);
    }

    private void persistTerm() {
        try {
            store.persistTerm(term(), votedEndpoint());
        } catch (IOException e) {
            throw new RaftException(e);
        }
    }

    /**
     * Returns the latest term this Raft node has seen.
     */
    public int term() {
        return termState.getTerm();
    }

    /**
     * Returns the endpoint this Raft note voted for in the current term.
     */
    public RaftEndpoint votedEndpoint() {
        return termState.getVotedEndpoint();
    }

    /**
     * Completes the current leadership transfer process with the given result
     * and resets it.
     */
    public void completeLeadershipTransfer(Object result) {
        if (leadershipTransferState == null) {
            return;
        }

        leadershipTransferState.complete(commitIndex, result);
        leadershipTransferState = null;
    }

    /**
     * Switches this Raft node to candidate role.
     * Clears pre-candidate and leader states. Initializes candidate state for
     * the current majority and grants a vote for the local endpoint.
     */
    public void toCandidate() {
        role = CANDIDATE;
        preCandidateState = null;
        leaderState = null;
        candidateState = new CandidateState(majority());
        candidateState.grantVote(localEndpoint);
        int newTerm = term() + 1;
        setTerm(newTerm);
        grantVote(newTerm, localEndpoint);
    }

    /**
     * Returns majority number of the last applied group members.
     */
    public int majority() {
        return effectiveGroupMembers.getMajority();
    }

    /**
     * Persist a vote for the endpoint in current term during leader election.
     */
    public void grantVote(int term, RaftEndpoint member) {
        termState = termState.grantVote(term, member);
        persistTerm();
    }

    /**
     * Switches this Raft node to leader role.
     * Sets local endpoint as the known leader. Clears (pre)candidate states
     * and initializes leader state for the current members.
     */
    public void toLeader() {
        role = LEADER;
        leader(localEndpoint);
        preCandidateState = null;
        candidateState = null;
        leaderState = new LeaderState(effectiveGroupMembers.remoteMembers(), log.lastLogOrSnapshotIndex());
    }

    /**
     * Updates the known leader to the given endpoint.
     */
    public void leader(RaftEndpoint endpoint) {
        termState = termState.withLeader(endpoint);
    }

    /**
     * Returns true if the given endpoint is in of the last applied members,
     * false otherwise.
     */
    public boolean isKnownMember(RaftEndpoint endpoint) {
        return effectiveGroupMembers.isKnownMember(endpoint);
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
     * This method expects that there's no pending membership changes and
     * the committed members are the same as the last applied members.
     * <p>
     * The leader state is also updated for the members which don't exist
     * in the committed members and the committed members that don't exist
     * in the latest applied members are removed.
     *
     * @param logIndex
     *         log index of membership change
     * @param members
     *         latest applied members
     */
    public void updateGroupMembers(long logIndex, Collection<RaftEndpoint> members) {
        assert committedGroupMembers == effectiveGroupMembers : "Cannot update group members to: " + members + " at log index: "
                + logIndex + " because last group " + "members: " + effectiveGroupMembers
                + " is different than committed group members: " + committedGroupMembers;
        assert effectiveGroupMembers.getLogIndex() < logIndex : "Cannot update group members to: " + members + " at log index: "
                + logIndex + " because last group " + "members: " + effectiveGroupMembers + " has a bigger log index.";

        RaftGroupMembersState newGroupMembers = new RaftGroupMembersState(logIndex, members, localEndpoint);
        committedGroupMembers = effectiveGroupMembers;
        effectiveGroupMembers = newGroupMembers;

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
     * and {@link #effectiveGroupMembers} are the same.
     */
    public void commitGroupMembers() {
        assert committedGroupMembers != effectiveGroupMembers : "Cannot commit last group members: " + effectiveGroupMembers
                + " because it is same with committed " + "group " + "members";

        committedGroupMembers = effectiveGroupMembers;
    }

    /**
     * Reverts the last group members back to the committed group members.
     * Essentially this means that the applied but not-yet-committed membership
     * change is reverted.
     */
    public void revertGroupMembers() {
        assert this.committedGroupMembers != this.effectiveGroupMembers;

        this.effectiveGroupMembers = this.committedGroupMembers;
        // there is no leader state to clean up
    }

    /**
     * Restores group members from the snapshot. Both the committed group
     * members and the last group members are overwritten with the given
     * member list.
     */
    public boolean restoreGroupMembers(long logIndex, Collection<RaftEndpoint> members) {
        assert effectiveGroupMembers.getLogIndex() <= logIndex : "Cannot restore group members to: " + members + " at log index: "
                + logIndex + " because last group " + "members: " + effectiveGroupMembers + " has a bigger log index.";

        // there is no leader state to clean up

        boolean changed = effectiveGroupMembers.getLogIndex() < logIndex;

        RaftGroupMembersState groupMembers = new RaftGroupMembersState(logIndex, members, localEndpoint);
        this.committedGroupMembers = groupMembers;
        this.effectiveGroupMembers = groupMembers;

        return changed;
    }

    /**
     * Initializes a new leadership transfer process for the given endpoint.
     * Returns {@code true} if the leadership transfer process is initialized
     * with this call, {@code false} if there is already an ongoing leadership
     * transfer process. If there is already an ongoing leadership transfer
     * process, the given future object is also attached to it.
     */
    public boolean initLeadershipTransfer(RaftEndpoint targetEndpoint, OrderedFuture<Object> resultFuture) {
        assert effectiveGroupMembers.getMembers().contains(targetEndpoint);

        if (leadershipTransferState == null) {
            leadershipTransferState = new LeadershipTransferState(termState.getTerm(), targetEndpoint, resultFuture);
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

    public SnapshotChunkCollector snapshotChunkCollector() {
        return snapshotChunkCollector;
    }

    public void snapshotChunkCollector(SnapshotChunkCollector snapshotChunkCollector) {
        if (this.snapshotChunkCollector != null && snapshotChunkCollector != null
                && this.snapshotChunkCollector.getSnapshotIndex() >= snapshotChunkCollector.getSnapshotIndex()) {
            throw new IllegalArgumentException(
                    "Current snapshot chunk collector at snapshot index: " + this.snapshotChunkCollector.getSnapshotIndex()
                            + " invalid new snapshot chunk collector at snapshot index: " + snapshotChunkCollector
                            .getSnapshotIndex());
        }

        this.snapshotChunkCollector = snapshotChunkCollector;
    }

    public RaftStore store() {
        return store;
    }

}
