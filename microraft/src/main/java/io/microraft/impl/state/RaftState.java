/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright (c) 2020, MicroRaft.
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

import static io.microraft.RaftRole.CANDIDATE;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEADER;
import static io.microraft.RaftRole.LEARNER;
import static io.microraft.model.log.SnapshotEntry.isNonInitial;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.microraft.RaftEndpoint;
import io.microraft.RaftRole;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.exception.NotLeaderException;
import io.microraft.exception.RaftException;
import io.microraft.impl.log.RaftLog;
import io.microraft.impl.log.SnapshotChunkCollector;
import io.microraft.impl.util.Long2ObjectHashMap;
import io.microraft.impl.util.OrderedFuture;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.RaftGroupMembersView.RaftGroupMembersViewBuilder;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.persistence.NopRaftStore;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;
import io.microraft.impl.state.QueryState.QueryContainer;

/**
 * State maintained by each Raft node.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public final class RaftState {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftState.class);

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
    private final RaftGroupMembersState initialGroupMembers;

    /**
     * Used for reflecting persistent-state changes to persistent storage.
     */
    private final RaftStore store;

    /**
     * Raft log entries; each log entry contains command for state machine, and the
     * term when entry was received by Raft group leader. First log index is 1.
     */
    private final RaftLog log;

    private final RaftModelFactory modelFactory;

    /**
     * Future objects to complete with the results of operations committed and
     * executed on the state machine. Key is the commit index.
     */
    private final Long2ObjectHashMap<OrderedFuture> futures = new Long2ObjectHashMap<>();

    /**
     * Queries to be executed on the state machine on commit indices which are
     * greater than the current commit index. These queries are registered with a
     * timeout, so if the commit index does not advance enough before the timeout, a
     * query's result future is completed with an exception.
     */
    private final NavigableMap<Long, Set<QueryContainer>> scheduledQueries = new TreeMap<>();

    /**
     * Latest committed group members of the Raft group.
     */
    private volatile RaftGroupMembersState committedGroupMembers;

    /**
     * Latest applied group members of the Raft group. This member may not be
     * committed yet and can be reverted. (initially equal to
     * {@link #committedGroupMembers})
     */
    private volatile RaftGroupMembersState effectiveGroupMembers;

    /**
     * Role of this Raft node.
     */
    private volatile RaftRole role;

    /**
     * Latest term this Raft node has seen along with the latest known Raft leader
     * endpoint (or null if not known).
     */
    private volatile RaftTermState termState;

    /**
     * Index of highest log entry known to be committed. (starts with 0 and
     * increases monotonically)
     * <p>
     * [NOT-PERSISTENT] because we can re-calculate commitIndex after restoring
     * logs.
     */
    private long commitIndex;

    /**
     * Index of highest log entry applied to state machine. (starts with 0 and
     * increases monotonically)
     * <p>
     * {@code lastApplied <= commitIndex} condition holds true always.
     * <p>
     * [NOT-PERSISTENT] because we can apply restored logs and re-calculate
     * lastApplied.
     */
    private long lastApplied;

    /**
     * State maintained by the Raft group leader, or null if this Raft node is not
     * the leader.
     */
    private LeaderState leaderState;

    /**
     * Candidate state maintained during the pre-voting step. Becomes null when
     * pre-voting ends by one of {@link #toCandidate()}, {@link #toLeader(long)} or
     * {@link #toFollower(int)} methods is called.
     */
    private CandidateState preCandidateState;

    /**
     * Candidate state maintained during the leader election step. Initialized when
     * this Raft node becomes a candidate via a {@link #toCandidate()} call and
     * becomes null when the voting ends when {@link #toLeader(long)} or
     * {@link #toFollower(int)} is called.
     */
    private CandidateState candidateState;

    /**
     * State maintained by the Raft group leader during leadership transfer.
     */
    private LeadershipTransferState leadershipTransferState;

    /**
     * State maintained by followers to keep received snapshot chunks during
     * snapshot installation.
     */
    private SnapshotChunkCollector snapshotChunkCollector;

    private RaftState(Object groupId, RaftEndpoint localEndpoint, RaftGroupMembersView initialGroupMembers,
            int logCapacity, RaftStore store, RaftModelFactory modelFactory) {
        this.groupId = requireNonNull(groupId);
        this.localEndpoint = requireNonNull(localEndpoint);
        if (requireNonNull(initialGroupMembers).getLogIndex() != 0) {
            throw new IllegalArgumentException(
                    "Invalid initial Raft group members log index: " + initialGroupMembers.getLogIndex());
        }
        this.role = initialGroupMembers.getVotingMembers().contains(this.localEndpoint) ? FOLLOWER : LEARNER;
        RaftGroupMembersState groupMembers = new RaftGroupMembersState(0, initialGroupMembers.getMembers(),
                initialGroupMembers.getVotingMembers(), localEndpoint);
        this.initialGroupMembers = groupMembers;
        this.committedGroupMembers = groupMembers;
        this.effectiveGroupMembers = groupMembers;
        this.termState = RaftTermState.INITIAL;
        this.store = requireNonNull(store);
        this.log = RaftLog.create(logCapacity, store);
        this.modelFactory = modelFactory;
    }

    private RaftState(Object groupId, RestoredRaftState restoredState, int logCapacity, RaftStore store,
            RaftModelFactory modelFactory) {
        this.groupId = requireNonNull(groupId);
        this.localEndpoint = requireNonNull(restoredState).getLocalEndpointPersistentState().getLocalEndpoint();
        this.role = restoredState.getLocalEndpointPersistentState().isVoting() ? FOLLOWER : LEARNER;
        RaftGroupMembersView initialGroupMembers = restoredState.getInitialGroupMembers();
        if (requireNonNull(initialGroupMembers).getLogIndex() != 0) {
            throw new IllegalArgumentException(
                    "Invalid initial Raft group members log index: " + initialGroupMembers.getLogIndex());
        }
        this.initialGroupMembers = new RaftGroupMembersState(0, initialGroupMembers.getMembers(),
                initialGroupMembers.getVotingMembers(), this.localEndpoint);
        this.committedGroupMembers = this.initialGroupMembers;
        this.effectiveGroupMembers = this.committedGroupMembers;
        this.termState = RaftTermState.restore(restoredState.getTermPersistentState().getTerm(),
                restoredState.getTermPersistentState().getVotedFor());

        SnapshotEntry snapshot = restoredState.getSnapshotEntry();
        if (isNonInitial(snapshot)) {
            installGroupMembers(snapshot.getGroupMembersView());
            this.commitIndex = snapshot.getIndex();
            this.lastApplied = snapshot.getIndex();
        }

        this.store = requireNonNull(store);
        this.log = RaftLog.restore(logCapacity, snapshot, restoredState.getLogEntries(), store);
        this.modelFactory = modelFactory;
    }

    public static RaftState create(Object groupId, RaftEndpoint localEndpoint, RaftGroupMembersView initialGroupMembers,
            int logCapacity, RaftModelFactory modelFactory) {
        return create(groupId, localEndpoint, initialGroupMembers, logCapacity, new NopRaftStore(), modelFactory);
    }

    public static RaftState create(Object groupId, RaftEndpoint localEndpoint, RaftGroupMembersView initialGroupMembers,
            int logCapacity, RaftStore store, RaftModelFactory modelFactory) {
        return new RaftState(groupId, localEndpoint, initialGroupMembers, logCapacity, store, modelFactory);
    }

    public static RaftState restore(Object groupId, RestoredRaftState restoredState, int logCapacity,
            RaftModelFactory modelFactory) {
        return restore(groupId, restoredState, logCapacity, new NopRaftStore(), modelFactory);
    }

    public static RaftState restore(Object groupId, RestoredRaftState restoredState, int logCapacity, RaftStore store,
            RaftModelFactory modelFactory) {
        return new RaftState(groupId, restoredState, logCapacity, store, modelFactory);
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
        return initialGroupMembers;
    }

    /**
     * Returns all members in the effective group members.
     */
    public Collection<RaftEndpoint> members() {
        return effectiveGroupMembers.getMembers();
    }

    /**
     * Returns all voting members in the effective group members.
     */
    public Collection<RaftEndpoint> votingMembers() {
        return effectiveGroupMembers.getVotingMembers();
    }

    /**
     * Returns remote members in the effective group members.
     */
    public Collection<RaftEndpoint> remoteMembers() {
        return effectiveGroupMembers.remoteMembers();
    }

    /**
     * Returns remote voting members in the effective group members.
     */
    public Collection<RaftEndpoint> remoteVotingMembers() {
        return effectiveGroupMembers.remoteVotingMembers();
    }

    /**
     * Returns number of members in the effective group members.
     */
    public int memberCount() {
        return effectiveGroupMembers.memberCount();
    }

    /**
     * Returns number of voting members in the effective group members.
     */
    public int votingMemberCount() {
        return effectiveGroupMembers.votingMemberCount();
    }

    /**
     * Returns the committed group members.
     */
    public RaftGroupMembersState committedGroupMembers() {
        return committedGroupMembers;
    }

    /**
     * Returns the effective group members.
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
        assert index >= commitIndex
                : "new commit index: " + index + " is smaller than current commit index: " + commitIndex;
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
        assert index >= lastApplied
                : "new last applied: " + index + " is smaller than current last applied: " + lastApplied;
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
     *             if an IO error occurs inside the store
     *
     * @see RaftStore#persistAndFlushInitialGroupMembers(RaftGroupMembersView)
     */
    public void persistInitialState(RaftGroupMembersViewBuilder initialGroupMembersBuilder) throws IOException {
        store.persistAndFlushLocalEndpoint(modelFactory.createRaftEndpointPersistentStateBuilder()
                .setLocalEndpoint(localEndpoint).setVoting(role != LEARNER).build());
        initialGroupMembersBuilder.setLogIndex(initialGroupMembers.getLogIndex())
                .setMembers(initialGroupMembers.getMembers()).setVotingMembers(initialGroupMembers.getVotingMembers());
        store.persistAndFlushInitialGroupMembers(initialGroupMembersBuilder.build());
    }

    /**
     * Switches this Raft node to follower role. Clears leader and (pre)candidate
     * states, updates the term.
     *
     * @param term
     *            current term
     */
    public void toFollower(int term) {
        if (role != LEARNER) {
            // If I am a LEARNER, I will stay in this role until I get promoted.
            role = FOLLOWER;
        }

        RaftTermState newTermState = termState.switchTo(term);
        persistTerm(newTermState);
        preCandidateState = null;
        LeaderState currentLeaderState = leaderState;
        leaderState = null;
        candidateState = null;
        completeLeadershipTransfer(null);
        termState = newTermState;
        if (currentLeaderState != null) {
            // this is done here to read the updated leader field
            currentLeaderState.queryState().fail(new NotLeaderException(localEndpoint, leader()));
        }
        invalidateFuturesFrom(commitIndex + 1, new IndeterminateStateException());
    }

    private void persistTerm(RaftTermState termStateToPersist) {
        try {
            store.persistAndFlushTerm(modelFactory.createRaftTermPersistentStateBuilder()
                    .setTerm(termStateToPersist.getTerm()).setVotedFor(termStateToPersist.getVotedEndpoint()).build());
        } catch (IOException e) {
            throw new RaftException("Failed to persist " + termStateToPersist, null, e);
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
     * Completes the current leadership transfer process with the given result and
     * resets it.
     */
    public void completeLeadershipTransfer(Object result) {
        if (leadershipTransferState == null) {
            return;
        }

        leadershipTransferState.complete(commitIndex, result);
        leadershipTransferState = null;
    }

    /**
     * Switches this Raft node to candidate role. Clears pre-candidate and leader
     * states. Initializes candidate state for the current majority and grants a
     * vote for the local endpoint.
     */
    public void toCandidate() {
        if (role == LEARNER) {
            throw new IllegalStateException(LEARNER + " cannot become " + CANDIDATE);
        }

        preCandidateState = null;
        int newTerm = term() + 1;
        RaftTermState newTermState = termState.switchTo(newTerm);
        persistTerm(newTermState);
        termState = newTermState;
        leaderState = null;
        grantVote(newTerm, localEndpoint);
        role = CANDIDATE;
        candidateState = new CandidateState(leaderElectionQuorumSize());
        candidateState.grantVote(localEndpoint);
    }

    private void promoteToVotingMember() throws IOException {
        if (role == LEADER || role == CANDIDATE) {
            throw new IllegalStateException("Cannot promote to voting member while the role is " + role);
        } else if (role == LEARNER) {
            store.persistAndFlushLocalEndpoint(modelFactory.createRaftEndpointPersistentStateBuilder()
                    .setLocalEndpoint(localEndpoint).setVoting(true).build());
            role = FOLLOWER;
        }
    }

    private void demoteToNonVotingMember() throws IOException {
        if (role == LEADER) {
            throw new IllegalStateException("Cannot revert voting member promotion while the role is " + role);
        } else if (role != LEARNER) {
            store.persistAndFlushLocalEndpoint(modelFactory.createRaftEndpointPersistentStateBuilder()
                    .setLocalEndpoint(localEndpoint).setVoting(false).build());
            role = LEARNER;
        }
    }

    /**
     * Returns the quorum size for a candidate to win leader election.
     */
    public int leaderElectionQuorumSize() {
        return effectiveGroupMembers.getMajorityQuorumSize();
    }

    /**
     * Returns the quorum size for committing a log entry.
     */
    public int logReplicationQuorumSize() {
        /*
         * We use the improved majority quorums technique of FPaxos here. In a cluster
         * of size N * 2, we can commit log entries after collecting acks from N nodes.
         * Since leader elections are done with majority quorums (N + 1), we still
         * guarantee that a new leader will always have all committed log entries.
         *
         * Here, we treat the 2-node group as a special case and use the normal majority
         * quorum calculation, where also the log replication logic uses the 2-node
         * quorum. The reason is to ensure that the replicated data is guaranteed to
         * have another copy in case the leader unrecoverably crashes.
         */
        int quorumSize = leaderElectionQuorumSize();
        return effectiveGroupMembers.votingMemberCount() % 2 != 0
                || committedGroupMembers.getLogIndex() != effectiveGroupMembers.getLogIndex() || quorumSize == 2
                        ? quorumSize
                        : quorumSize - 1;
    }

    /**
     * Persist a vote for the endpoint in current term during leader election.
     */
    public void grantVote(int term, RaftEndpoint member) {
        RaftTermState newTermState = termState.grantVote(term, member);
        persistTerm(newTermState);
        termState = newTermState;
    }

    /**
     * Switches this Raft node to leader role. Sets local endpoint as the known
     * leader. Clears (pre)candidate states and initializes leader state for the
     * current members.
     */
    public void toLeader(long currentTimeMillis) {
        role = LEADER;
        leader(localEndpoint);
        preCandidateState = null;
        candidateState = null;
        leaderState = new LeaderState(effectiveGroupMembers.remoteMembers(), log.lastLogOrSnapshotIndex(),
                currentTimeMillis);
    }

    /**
     * Updates the known leader to the given endpoint.
     */
    public void leader(RaftEndpoint endpoint) {
        termState = termState.withLeader(endpoint);
    }

    /**
     * Returns true if the given endpoint is in the effective group members, false
     * otherwise.
     */
    public boolean isKnownMember(RaftEndpoint endpoint) {
        return effectiveGroupMembers.isKnownMember(endpoint);
    }

    /**
     * Returns true if the given endpoint is a voting member in the effective group
     * members, false otherwise.
     */
    public boolean isVotingMember(RaftEndpoint endpoint) {
        return effectiveGroupMembers.isVotingMember(endpoint);
    }

    /**
     * Initializes the pre-candidate state for pre-voting and grants a vote for the
     * local endpoint.
     */
    public void initPreCandidateState() {
        preCandidateState = new CandidateState(leaderElectionQuorumSize());
        preCandidateState.grantVote(localEndpoint);
    }

    /**
     * Returns the pre-candidate state.
     */
    public CandidateState preCandidateState() {
        return preCandidateState;
    }

    /**
     * Initializes the effective members with the given members and the log index.
     * <p>
     * This method expects that there's no pending membership changes and the
     * committed members are the same as the effective members.
     * <p>
     * The leader state is also updated for the members which don't exist in the
     * committed members and the committed members that don't exist in the effective
     * members are removed.
     *
     * @param logIndex
     *            log index of membership change
     * @param members
     *            latest applied members
     * @param votingMembers
     *            latest applied voting members
     * @param currentTimeMillis
     *            the current time since epoch
     */
    public void updateGroupMembers(long logIndex, Collection<RaftEndpoint> members,
            Collection<RaftEndpoint> votingMembers, long currentTimeMillis) {
        assert committedGroupMembers == effectiveGroupMembers : "Cannot update group members to: " + members
                + " at log index: " + logIndex + " because effective group members: " + effectiveGroupMembers
                + " is different than committed group members: " + committedGroupMembers;
        assert effectiveGroupMembers.getLogIndex() < logIndex
                : "Cannot update group members to: " + members + " at log index: " + logIndex
                        + " because effective group members: " + effectiveGroupMembers + " has a bigger log index.";

        RaftGroupMembersState newGroupMembers = new RaftGroupMembersState(logIndex, members, votingMembers,
                localEndpoint);
        committedGroupMembers = effectiveGroupMembers;
        effectiveGroupMembers = newGroupMembers;

        if (leaderState != null) {
            members.stream().filter(member -> !committedGroupMembers.isKnownMember(member))
                    .forEach(member -> leaderState.add(member, log.lastLogOrSnapshotIndex(), currentTimeMillis));

            committedGroupMembers.remoteMembers().stream().filter(member -> !members.contains(member))
                    .forEach(member -> leaderState.remove(member));
        }

        if (role == LEARNER && effectiveGroupMembers.getVotingMembers().contains(this.localEndpoint)) {
            try {
                promoteToVotingMember();
            } catch (IOException e) {
                throw new RaftException("Promotion to voting member failed", null, e);
            }
        }
    }

    /**
     * Marks the effective group members as committed. At this point
     * {@link #committedGroupMembers} and {@link #effectiveGroupMembers} are the
     * same.
     */
    public void commitGroupMembers() {
        assert committedGroupMembers != effectiveGroupMembers : "Cannot commit effective group members: "
                + effectiveGroupMembers + " because it is same with committed " + "group " + "members";

        committedGroupMembers = effectiveGroupMembers;
    }

    /**
     * Reverts the effective group members back to the committed group members.
     * Essentially this means that the applied but not-yet-committed membership
     * change is reverted.
     */
    public void revertGroupMembers() {
        assert this.committedGroupMembers != this.effectiveGroupMembers;
        boolean demoteToNonVotingMember = !this.committedGroupMembers.getVotingMembers().contains(this.localEndpoint)
                && this.effectiveGroupMembers.getVotingMembers().contains(this.localEndpoint);
        if (demoteToNonVotingMember) {
            try {
                demoteToNonVotingMember();
            } catch (IOException e) {
                throw new RaftException("Demotion to non-voting member failed", null, e);
            }
        }
        this.effectiveGroupMembers = this.committedGroupMembers;
        // there is no leader state to clean up
    }

    /**
     * Installs group members from the snapshot. Both the committed group members
     * and the effective group members are overwritten with the given member list.
     */
    public boolean installGroupMembers(RaftGroupMembersView groupMembersView) {
        assert effectiveGroupMembers.getLogIndex() <= groupMembersView.getLogIndex()
                : "Cannot restore group members to: " + groupMembersView.getMembers() + " at log index: "
                        + groupMembersView.getLogIndex() + " because effective group members: " + effectiveGroupMembers
                        + " has a bigger log index.";

        // there is no leader state to clean up

        boolean changed = effectiveGroupMembers.getLogIndex() < groupMembersView.getLogIndex();
        RaftGroupMembersState previousGroupMembers = this.effectiveGroupMembers;
        RaftGroupMembersState groupMembers = new RaftGroupMembersState(groupMembersView.getLogIndex(),
                groupMembersView.getMembers(), groupMembersView.getVotingMembers(), localEndpoint);

        if (changed) {
            try {
                if (!previousGroupMembers.getVotingMembers().contains(localEndpoint)
                        && groupMembers.getVotingMembers().contains(localEndpoint)) {
                    promoteToVotingMember();
                } else if (previousGroupMembers.getVotingMembers().contains(localEndpoint)
                        && !groupMembers.getVotingMembers().contains(localEndpoint)) {
                    assert role == FOLLOWER || role == CANDIDATE
                            : "Cannot demote to non voting member since role is " + role;
                    demoteToNonVotingMember();
                }
            } catch (IOException e) {
                throw new RaftException(
                        "Persisting voting member status failed while installing group members: " + groupMembersView,
                        null, e);
            }
        }

        this.committedGroupMembers = groupMembers;
        this.effectiveGroupMembers = groupMembers;

        return changed;
    }

    /**
     * Initializes a new leadership transfer process for the given endpoint. Returns
     * {@code true} if the leadership transfer process is initialized with this
     * call, {@code false} if there is already an ongoing leadership transfer
     * process. If there is already an ongoing leadership transfer process, the
     * given future object is also attached to it.
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
            throw new IllegalArgumentException("Current snapshot chunk collector at snapshot index: "
                    + this.snapshotChunkCollector.getSnapshotIndex()
                    + " invalid new snapshot chunk collector at snapshot index: "
                    + snapshotChunkCollector.getSnapshotIndex());
        }

        this.snapshotChunkCollector = snapshotChunkCollector;
    }

    public RaftStore store() {
        return store;
    }

    /**
     * Registers the given future with its {@code entryIndex}. This future will be
     * notified when the corresponding operation is committed or its log entry is
     * reverted.
     *
     * @param logIndex
     *            the log index to register the given future
     * @param future
     *            the future object to register
     */
    public void registerFuture(long logIndex, OrderedFuture future) {
        OrderedFuture f = futures.put(logIndex, future);
        assert f == null : localEndpoint + " future object is already registered for log index: " + logIndex;
    }

    /**
     * If there is a future object at the given log index, it is completed with the
     * given result. Future objects are registered only in the leader node.
     *
     * @param logIndex
     *            the log index to complete the future at
     * @param result
     *            the result to object the future at the given log index
     */
    public void completeFuture(long logIndex, Object result) {
        OrderedFuture f = futures.remove(logIndex);
        if (f != null) {
            if (result instanceof Throwable) {
                f.fail((Throwable) result);
            } else {
                f.complete(logIndex, result);
            }
        }
    }

    /**
     * Completes futures with the given exception for indices greater than or equal
     * to the given index. Note that the given index is inclusive.
     *
     * @param startIndexInclusive
     *            the (inclusive) starting log index to complete registered futures
     * @param e
     *            the RaftException object to complete registered futures
     */
    public void invalidateFuturesFrom(long startIndexInclusive, RaftException e) {
        int count = 0;
        Iterator<Entry<Long, OrderedFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Long, OrderedFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index >= startIndexInclusive) {
                entry.getValue().fail(e);
                iterator.remove();
                count++;
            }
        }

        if (count > 0) {
            LOGGER.warn("{} Invalidated {} futures from log index: {} with: {}", localEndpoint, count,
                    startIndexInclusive, e);
        }
    }

    /**
     * Completes futures with the given exception for indices smaller than or equal
     * to the given index. Note that the given index is inclusive.
     *
     * @param endIndexInclusive
     *            the log index (inclusive) until which all waiting futures will be
     *            completed
     * @param e
     *            the error to complete the waiting futures
     */
    public void invalidateFuturesUntil(long endIndexInclusive, RaftException e) {
        int count = 0;
        Iterator<Entry<Long, OrderedFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Long, OrderedFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index <= endIndexInclusive) {
                entry.getValue().fail(e);
                iterator.remove();
                count++;
            }
        }

        if (count > 0) {
            LOGGER.warn("{} Completed {} futures until log index: {} with {}", localEndpoint, count, endIndexInclusive,
                    e);
        }
    }

    /**
     * Adds a query object which is going to be executed when the commit index
     * becomes equal to or greater than the given commit index.
     *
     * @param minCommitIndex
     *            the minimum commit index to execute the given query
     * @param query
     *            the query to be executed
     */
    public void addScheduledQuery(long minCommitIndex, QueryContainer query) {
        scheduledQueries.computeIfAbsent(minCommitIndex, (v) -> new LinkedHashSet<>(1)).add(query);
    }

    /**
     * Completes futures of the all queries waiting to be executed at a future
     * commit index.
     *
     * @param t
     *            the error to complete the futures of the queries waiting to be
     *            executed.
     */
    public void invalidateScheduledQueries() {
        if (scheduledQueries.isEmpty()) {
            return;
        }

        int invalidated = 0;
        for (Map.Entry<Long, Set<QueryContainer>> entry : scheduledQueries.entrySet()) {
            for (QueryContainer query : entry.getValue()) {
                query.fail(new LaggingCommitIndexException(lastApplied, entry.getKey(), leader()));
            }
            invalidated += entry.getValue().size();
        }

        scheduledQueries.clear();
        if (invalidated > 0) {
            LOGGER.warn("{} invalidated {} waiting queries.", localEndpoint, invalidated);
        }
    }

    /**
     * Tries to remove the given query at the given minimum commit index and returns
     * true if successful. The given query might have been already executed, in
     * which case returns false.
     *
     * @param minCommitIndex
     *            the minimum commit index to execute the given query
     * @param query
     *            the query to be executed
     * @return true if successfully removed the given query, false otherwise
     */
    public boolean removeScheduledQuery(long minCommitIndex, QueryContainer query) {
        if (scheduledQueries.isEmpty()) {
            return false;
        }

        Set<QueryContainer> queries = scheduledQueries.get(minCommitIndex);
        if (queries == null) {
            return false;
        }

        if (queries.remove(query)) {
            if (queries.isEmpty()) {
                scheduledQueries.remove(minCommitIndex);
            }

            return true;
        }

        return false;
    }

    /**
     * Removes and returns all queries waiting to be executed at a log index which
     * is less than equal to the current commit index.
     *
     * @return the queries to be executed now.
     */
    public Collection<QueryContainer> collectScheduledQueriesToExecute() {
        if (scheduledQueries.isEmpty()) {
            return Collections.emptyList();
        }

        NavigableMap<Long, Set<QueryContainer>> queriesToExecute = scheduledQueries.headMap(lastApplied, true);
        if (queriesToExecute.isEmpty()) {
            return Collections.emptyList();
        }

        List<QueryContainer> queries = new ArrayList<>();
        queriesToExecute.values().forEach(queries::addAll);

        return queries;
    }
}
