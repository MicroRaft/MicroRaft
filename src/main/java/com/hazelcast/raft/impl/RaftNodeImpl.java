package com.hazelcast.raft.impl;

import com.hazelcast.raft.MembershipChangeMode;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftGroupMembers;
import com.hazelcast.raft.RaftIntegration;
import com.hazelcast.raft.RaftMsg;
import com.hazelcast.raft.RaftNode;
import com.hazelcast.raft.RaftNodeStatus;
import com.hazelcast.raft.RaftStateSummary;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.OperationResultUnknownException;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.impl.groupop.RaftGroupOp;
import com.hazelcast.raft.impl.groupop.TerminateRaftGroupOp;
import com.hazelcast.raft.impl.groupop.UpdateRaftGroupMembersOp;
import com.hazelcast.raft.impl.handler.AppendEntriesFailureResponseHandler;
import com.hazelcast.raft.impl.handler.AppendEntriesRequestHandler;
import com.hazelcast.raft.impl.handler.AppendEntriesSuccessResponseHandler;
import com.hazelcast.raft.impl.handler.InstallSnapshotRequestHandler;
import com.hazelcast.raft.impl.handler.PreVoteRequestHandler;
import com.hazelcast.raft.impl.handler.PreVoteResponseHandler;
import com.hazelcast.raft.impl.handler.TriggerLeaderElectionHandler;
import com.hazelcast.raft.impl.handler.VoteRequestHandler;
import com.hazelcast.raft.impl.handler.VoteResponseHandler;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.log.SnapshotEntry;
import com.hazelcast.raft.impl.msg.AppendEntriesFailureResponse;
import com.hazelcast.raft.impl.msg.AppendEntriesRequest;
import com.hazelcast.raft.impl.msg.AppendEntriesSuccessResponse;
import com.hazelcast.raft.impl.msg.InstallSnapshotRequest;
import com.hazelcast.raft.impl.msg.PreVoteRequest;
import com.hazelcast.raft.impl.msg.PreVoteResponse;
import com.hazelcast.raft.impl.msg.TriggerLeaderElectionRequest;
import com.hazelcast.raft.impl.msg.VoteRequest;
import com.hazelcast.raft.impl.msg.VoteResponse;
import com.hazelcast.raft.impl.state.FollowerState;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.raft.impl.state.LeadershipTransferState;
import com.hazelcast.raft.impl.state.QueryState;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.task.LeaderElectionTimeoutTask;
import com.hazelcast.raft.impl.task.MembershipChangeTask;
import com.hazelcast.raft.impl.task.PreVoteTask;
import com.hazelcast.raft.impl.task.PreVoteTimeoutTask;
import com.hazelcast.raft.impl.task.QueryTask;
import com.hazelcast.raft.impl.task.RaftNodeStatusAwareTask;
import com.hazelcast.raft.impl.task.ReplicateTask;
import com.hazelcast.raft.impl.util.InternallyCompletableFuture;
import com.hazelcast.raft.impl.util.Long2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.raft.RaftNodeStatus.ACTIVE;
import static com.hazelcast.raft.RaftNodeStatus.INITIAL;
import static com.hazelcast.raft.RaftNodeStatus.STEPPED_DOWN;
import static com.hazelcast.raft.RaftNodeStatus.TERMINATED;
import static com.hazelcast.raft.RaftNodeStatus.TERMINATING;
import static com.hazelcast.raft.RaftNodeStatus.UPDATING_GROUP_MEMBER_LIST;
import static com.hazelcast.raft.RaftRole.FOLLOWER;
import static com.hazelcast.raft.RaftRole.LEADER;
import static com.hazelcast.raft.impl.util.Preconditions.checkFalse;
import static com.hazelcast.raft.impl.util.Preconditions.checkNotNull;
import static com.hazelcast.raft.impl.util.Preconditions.checkTrue;
import static com.hazelcast.raft.impl.util.RandomPicker.getRandomInt;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * In-memory implementation of {@link RaftNode}.
 * <p>
 * Each Raft node runs in a single-threaded manner with an event-based
 * approach.
 * <p>
 * Raft state is not persisted to disk.
 *
 * @author mdogan
 * @author metanet
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class RaftNodeImpl
        implements RaftNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNodeImpl.class);
    private static final long LEADER_ELECTION_TIMEOUT_MILLIS_RANGE = 1000;
    private static final long RAFT_NODE_INIT_DELAY_MILLIS = 500;
    private static final float RATIO_TO_KEEP_LOG_ENTRIES_AFTER_SNAPSHOT = 0.1f;

    private final Object groupId;
    private final RaftState state;
    private final RaftIntegration integration;
    private final RaftConfig config;
    private final String localEndpointName;
    private final Long2ObjectHashMap<InternallyCompletableFuture<Object>> futures = new Long2ObjectHashMap<>();

    private final int maxLogEntryCountToKeepAfterSnapshot;
    private final Runnable appendRequestBackoffResetTask;

    private long lastAppendEntriesRequestTimestamp;
    private boolean appendEntriesRequestBackoffResetTaskScheduled;
    private volatile RaftNodeStatus status = INITIAL;

    public RaftNodeImpl(Object groupId, RaftEndpoint localEndpoint, Collection<RaftEndpoint> members, RaftConfig config,
                        RaftIntegration integration) {
        checkNotNull(groupId);
        checkNotNull(integration);
        checkNotNull(localEndpoint);
        checkNotNull(members);
        this.groupId = groupId;
        this.integration = integration;
        this.config = config;
        this.localEndpointName = localEndpoint.identifierString() + "<" + groupId + ">";
        this.maxLogEntryCountToKeepAfterSnapshot = (int) (config.getCommitIndexAdvanceCountToTakeSnapshot()
                * RATIO_TO_KEEP_LOG_ENTRIES_AFTER_SNAPSHOT);
        int logCapacity =
                config.getCommitIndexAdvanceCountToTakeSnapshot() + config.getUncommittedLogEntryCountToRejectNewAppends()
                        + maxLogEntryCountToKeepAfterSnapshot;
        this.state = new RaftState(groupId, localEndpoint, members, logCapacity);
        this.appendRequestBackoffResetTask = new AppendEntriesRequestBackoffResetTask();
    }

    @Nonnull
    @Override
    public Object getGroupId() {
        return groupId;
    }

    @Nonnull
    @Override
    public RaftEndpoint getLocalEndpoint() {
        return state.localEndpoint();
    }

    @Nullable
    @Override
    public RaftEndpoint getLeaderEndpoint() {
        // It reads the most recent write to the volatile leader field, however leader might be already changed.
        checkFalse(status.isTerminal(), "Cannot get leader when " + status);
        return state.leader();
    }

    @Nonnull
    @Override
    public RaftNodeStatus getStatus() {
        // It reads the volatile status field
        return status;
    }

    @Nonnull
    @Override
    public RaftGroupMembers getInitialMembers() {
        return state.initialMembers();
    }

    @Nonnull
    @Override
    public RaftGroupMembers getCommittedMembers() {
        return state.committedGroupMembers();
    }

    @Nonnull
    @Override
    public RaftGroupMembers getEffectiveMembers() {
        return state.lastGroupMembers();
    }

    @Override
    public void forceTerminate() {
        if (status.isTerminal()) {
            return;
        }

        integration.execute(() -> {
            if (status.isTerminal()) {
                return;
            }

            setStatus(TERMINATED);
            invalidateFuturesFrom(state.commitIndex() + 1);
            LeaderState leaderState = state.leaderState();
            if (leaderState != null) {
                leaderState.queryState().operations().forEach(
                        t -> t.getValue().internalCompleteExceptionally(new LeaderDemotedException(state.localEndpoint(), null)));
            }
            state.completeLeadershipTransfer(new LeaderDemotedException(state.localEndpoint(), null));
            integration.close();
        });
    }

    @Override
    public void start() {
        if (status == ACTIVE) {
            return;
        } else if (status != INITIAL) {
            throw new IllegalStateException("Cannot start RaftNode when " + status);
        }

        if (!integration.isReady()) {
            integration.schedule(this::start, RAFT_NODE_INIT_DELAY_MILLIS, MILLISECONDS);
            return;
        }

        integration.execute(() -> {
            if (status == ACTIVE) {
                return;
            } else if (status != INITIAL) {
                throw new IllegalStateException("Cannot start RaftNode when " + status);
            }

            LOGGER.debug("{} Starting for {} with {} members: {}", localEndpointName, groupId, state.memberCount(),
                    state.members());

            status = ACTIVE;
            integration.execute(new PreVoteTask(RaftNodeImpl.this, 0));
            scheduleLeaderFailureDetection();
            scheduleRaftStateSummaryPublishTask();
        });
    }

    @Override
    public void handle(@Nonnull RaftMsg msg) {
        if (status.isTerminal()) {
            LOGGER.warn("{} will not handle {} because {}", localEndpointName, msg, status);
            return;
        }

        Runnable handler;
        if (msg instanceof AppendEntriesRequest) {
            handler = new AppendEntriesRequestHandler(this, (AppendEntriesRequest) msg);
        } else if (msg instanceof AppendEntriesSuccessResponse) {
            handler = new AppendEntriesSuccessResponseHandler(this, (AppendEntriesSuccessResponse) msg);
        } else if (msg instanceof AppendEntriesFailureResponse) {
            handler = new AppendEntriesFailureResponseHandler(this, (AppendEntriesFailureResponse) msg);
        } else if (msg instanceof InstallSnapshotRequest) {
            handler = new InstallSnapshotRequestHandler(this, (InstallSnapshotRequest) msg);
        } else if (msg instanceof VoteRequest) {
            handler = new VoteRequestHandler(this, (VoteRequest) msg);
        } else if (msg instanceof VoteResponse) {
            handler = new VoteResponseHandler(this, (VoteResponse) msg);
        } else if (msg instanceof PreVoteRequest) {
            handler = new PreVoteRequestHandler(this, (PreVoteRequest) msg);
        } else if (msg instanceof PreVoteResponse) {
            handler = new PreVoteResponseHandler(this, (PreVoteResponse) msg);
        } else if (msg instanceof TriggerLeaderElectionRequest) {
            handler = new TriggerLeaderElectionHandler(this, (TriggerLeaderElectionRequest) msg);
        } else {
            throw new IllegalArgumentException("Invalid Raft msg: " + msg);
        }

        integration.execute(handler);
    }

    @Nonnull
    @Override
    public CompletableFuture<Object> replicate(@Nullable Object operation) {
        InternallyCompletableFuture<Object> future = new InternallyCompletableFuture<>();
        if (!status.isTerminal()) {
            integration.execute(new ReplicateTask(this, operation, future));
        } else {
            future.internalCompleteExceptionally(status.createException(getLocalEndpoint()));
        }

        return future;
    }

    @Nonnull
    @Override
    public CompletableFuture<Object> terminateGroup() {
        return replicate(new TerminateRaftGroupOp());
    }

    @Nonnull
    @Override
    public CompletableFuture<Long> changeMembership(@Nonnull RaftEndpoint endpoint, @Nonnull MembershipChangeMode mode,
                                                    long expectedGroupMembersCommitIndex) {
        InternallyCompletableFuture future = new InternallyCompletableFuture<>();
        if (!status.isTerminal()) {
            integration.execute(new MembershipChangeTask(this, future, endpoint, mode, expectedGroupMembersCommitIndex));
        } else {
            future.internalCompleteExceptionally(status.createException(getLocalEndpoint()));
        }

        return future;
    }

    @Nonnull
    @Override
    public CompletableFuture<Object> transferLeadership(@Nonnull RaftEndpoint endpoint) {
        InternallyCompletableFuture<Object> future = new InternallyCompletableFuture<>();
        if (!status.isTerminal()) {
            integration.execute(() -> initLeadershipTransfer(endpoint, future));
        } else {
            future.internalCompleteExceptionally(status.createException(getLocalEndpoint()));
        }

        return future;
    }

    @Nonnull
    @Override
    public CompletableFuture<Object> query(@Nullable Object operation, @Nonnull QueryPolicy queryPolicy) {
        InternallyCompletableFuture<Object> future = new InternallyCompletableFuture<>();
        if (!status.isTerminal()) {
            integration.execute(new QueryTask(this, operation, queryPolicy, future));
        } else {
            future.internalCompleteExceptionally(status.createException(getLocalEndpoint()));
        }

        return future;
    }

    /**
     * Returns a short string that represents identity
     * of the local Raft endpoint.
     */
    public String localEndpointName() {
        return localEndpointName;
    }

    /**
     * Updates status of the Raft node with the given status and notifies
     * {@link RaftIntegration#onRaftStateChange(RaftStateSummary)}).
     */
    public void setStatus(RaftNodeStatus newStatus) {
        if (status.isTerminal()) {
            throw new IllegalStateException("Cannot set status: " + newStatus + " since already " + this.status);
        }

        RaftNodeStatus prevStatus = this.status;
        this.status = newStatus;

        if (prevStatus != newStatus) {
            if (newStatus == ACTIVE) {
                LOGGER.info("{} Status is set to {}", localEndpointName, newStatus);
            } else {
                LOGGER.warn("{} Status is set to {}", localEndpointName, newStatus);
            }
        }

        integration.onRaftStateChange(state.summarize(newStatus));
    }

    /**
     * Returns a randomized leader election timeout in milliseconds
     * based on the leader election timeout configuration.
     *
     * @see RaftConfig#getLeaderElectionTimeoutMs()
     */
    public long getRandomizedLeaderElectionTimeoutMs() {
        return getRandomInt((int) config.getLeaderElectionTimeoutMs(),
                (int) (config.getLeaderElectionTimeoutMs() + LEADER_ELECTION_TIMEOUT_MILLIS_RANGE));
    }

    /**
     * Returns true if a new operation is allowed to be replicated. This method
     * must be invoked only when the local Raft node is the Raft group leader.
     * <p>
     * Replication is not allowed, when;
     * <ul>
     * <li>The local Raft node is terminating, terminated or stepped down.</li>
     * <li>The local Raft log has no more empty slots for uncommitted entries.</li>
     * <li>The given operation is a {@link RaftGroupOp} and there's an ongoing
     * membership change in group.</li>
     * <li>The operation is a membership change and there's no committed entry
     * in the current term yet.</li>
     * </ul>
     *
     * @see RaftNodeStatus
     * @see RaftGroupOp
     * @see RaftConfig#getUncommittedLogEntryCountToRejectNewAppends()
     * @see RaftIntegration#getOperationToAppendAfterLeaderElection()
     */
    public boolean canReplicateNewOperation(Object operation) {
        if (status.isTerminal()) {
            return false;
        }

        RaftLog log = state.log();
        long lastLogIndex = log.lastLogOrSnapshotIndex();
        long commitIndex = state.commitIndex();
        if (lastLogIndex - commitIndex >= config.getUncommittedLogEntryCountToRejectNewAppends()) {
            return false;
        }

        if (status == TERMINATING) {
            return false;
        } else if (status == UPDATING_GROUP_MEMBER_LIST) {
            return state.lastGroupMembers().isKnownMember(state.localEndpoint()) && !(operation instanceof RaftGroupOp);
        }

        if (operation instanceof UpdateRaftGroupMembersOp) {
            // the leader must have committed an entry in its term to make a membership change
            // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

            // last committed entry is either in the last snapshot or still in the log
            LogEntry lastCommittedEntry = commitIndex == log.snapshotIndex() ? log.snapshot() : log.getLogEntry(commitIndex);
            assert lastCommittedEntry != null;

            return lastCommittedEntry.term() == state.term();
        }

        return state.leadershipTransferState() == null;
    }

    /**
     * Returns true if a new query is currently allowed to be executed without
     * appending an entry to the Raft log. This method must be invoked only
     * when the local Raft node is the leader.
     * <p>
     * A new linearizable query execution is not allowed when:
     * <ul>
     * <li>The local Raft node is terminating, terminated or stepped down.</li>
     * <li>If the leader has not yet committed an entry in the current term.
     * See Section 6.4 of Raft Dissertation.</li>
     * <li>There are already a lot of queries waiting to be executed.</li>
     * </ul>
     *
     * @see RaftNodeStatus
     * @see RaftConfig#getUncommittedLogEntryCountToRejectNewAppends()
     */
    public boolean canQueryLinearizable() {
        if (status.isTerminal()) {
            return false;
        }

        long commitIndex = state.commitIndex();
        RaftLog log = state.log();

        // If the leader has not yet marked an entry from its current term committed, it waits until it has done so. (§6.4)
        // last committed entry is either in the last snapshot or still in the log
        LogEntry lastCommittedEntry = commitIndex == log.snapshotIndex() ? log.snapshot() : log.getLogEntry(commitIndex);
        assert lastCommittedEntry != null;

        if (lastCommittedEntry.term() != state.term()) {
            return false;
        }

        // We can execute multiple queries at one-shot without appending to the Raft log,
        // and we use the maxUncommittedEntryCount configuration parameter to upper-bound
        // the number of queries that are collected until the heartbeat round is done.
        QueryState queryState = state.leaderState().queryState();
        return queryState.queryCount() < config.getUncommittedLogEntryCountToRejectNewAppends();
    }

    /**
     * Returns true if the linearizable read optimization is enabled.
     */
    public boolean isLinearizableReadOptimizationEnabled() {
        return config.isLinearizableReadOptimizationEnabled();
    }

    private void scheduleLeaderFailureDetection() {
        integration.schedule(new LeaderFailureDetectionTask(), getRandomizedLeaderElectionTimeoutMs(), MILLISECONDS);
    }

    private void scheduleRaftStateSummaryPublishTask() {
        integration.schedule(new RaftStateSummaryPublishTask(), config.getRaftStateSummaryPublishPeriodSecs(), SECONDS);
    }

    private void scheduleHeartbeat() {
        broadcastAppendEntriesRequest();
        integration.schedule(new HeartbeatTask(), config.getLeaderHeartbeatPeriodMs(), MILLISECONDS);
    }

    /**
     * Sends the given Raft message to the given Raft endpoint.
     */
    public void send(RaftMsg msg, RaftEndpoint target) {
        integration.send(msg, target);
    }

    /**
     * Broadcasts append entries requests to all group members
     * according to their nextIndex parameters.
     */
    public void broadcastAppendEntriesRequest() {
        state.remoteMembers().forEach(this::sendAppendEntriesRequest);
        updateLastAppendEntriesTimestamp();
    }

    /**
     * Sends an append entries request to the given follower.
     * <p>
     * Log entries between follower's known nextIndex and the latest appended
     * entry index are sent as a batch, whose size can be at most
     * {@link RaftConfig#getAppendEntriesRequestMaxLogEntryCount()}.
     * <p>
     * If the given follower's nextIndex is behind the latest snapshot index,
     * then an {@link InstallSnapshotRequest} is sent.
     * <p>
     * If the leader doesn't know the given follower's matchIndex
     * (i.e., its {@code matchIndex == 0}), then an empty append entries
     * request is sent to save bandwidth until the leader discovers the real
     * matchIndex of the follower.
     */
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    public void sendAppendEntriesRequest(RaftEndpoint follower) {
        if (!integration.isReachable(follower)) {
            return;
        }

        RaftLog log = state.log();
        LeaderState leaderState = state.leaderState();
        FollowerState followerState = leaderState.getFollowerState(follower);
        if (followerState.isAppendEntriesRequestBackoffSet()) {
            // The follower still has not sent a response for the last append request.
            // We will send a new append request either when the follower sends a response
            // or a back-off timeout occurs.
            return;
        }

        long nextIndex = followerState.nextIndex();

        // if the first log entry to be sent is put into the snapshot, check if we still keep it in the log
        // if we still keep that log entry and its previous entry, we don't need to send a snapshot

        if (nextIndex <= log.snapshotIndex() && (!log.containsLogEntry(nextIndex) || (nextIndex > 1 && !log
                .containsLogEntry(nextIndex - 1)))) {
            RaftMsg request = new InstallSnapshotRequest(state.localEndpoint(), state.term(), log.snapshot(),
                    leaderState.queryRound());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(localEndpointName + " Sending " + request + " to " + follower + " since next index: " + nextIndex
                        + " <= snapshot index: " + log.snapshotIndex());
            }
            followerState.setMaxAppendEntriesRequestBackoff();
            scheduleAppendEntriesRequestBackoffResetTask();
            integration.send(request, follower);
            return;
        }

        int prevEntryTerm = 0;
        long prevEntryIndex = 0;
        LogEntry[] entries;
        boolean setAppendRequestBackoff = true;

        long lastLogIndex = log.lastLogOrSnapshotIndex();
        if (nextIndex > 1) {
            prevEntryIndex = nextIndex - 1;
            LogEntry prevEntry = (log.snapshotIndex() == prevEntryIndex) ? log.snapshot() : log.getLogEntry(prevEntryIndex);
            assert prevEntry != null : "Prev entry index: " + prevEntryIndex + ", snapshot: " + log.snapshotIndex();
            prevEntryTerm = prevEntry.term();

            long matchIndex = followerState.matchIndex();
            if (matchIndex == 0) {
                // Until the leader has discovered where it and the follower's logs match,
                // the leader can send AppendEntries with no entries (like heartbeats) to save bandwidth.
                // We still need to enable append request backoff here because we do not want to bombard
                // the follower before we learn its match index
                entries = new LogEntry[0];
            } else if (nextIndex <= lastLogIndex) {
                // Then, once the matchIndex immediately precedes the nextIndex,
                // the leader should begin to send the actual entries
                long end = min(nextIndex + config.getAppendEntriesRequestMaxLogEntryCount(), lastLogIndex);
                entries = log.getEntriesBetween(nextIndex, end);
            } else {
                // The follower has caught up with the leader. Sending an empty append request as a heartbeat...
                entries = new LogEntry[0];
                setAppendRequestBackoff = false;
            }
        } else if (nextIndex == 1 && lastLogIndex > 0) {
            // Entries will be sent to the follower for the first time...
            long end = min(nextIndex + config.getAppendEntriesRequestMaxLogEntryCount(), lastLogIndex);
            entries = log.getEntriesBetween(nextIndex, end);
        } else {
            // There is no entry in the Raft log. Sending an empty append request as a heartbeat...
            entries = new LogEntry[0];
            setAppendRequestBackoff = false;
        }

        RaftMsg request = new AppendEntriesRequest(state.localEndpoint(), state.term(), prevEntryTerm, prevEntryIndex,
                state.commitIndex(), entries, leaderState.queryRound());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(localEndpointName + " Sending " + request + " to " + follower + " with next index: " + nextIndex);
        }

        if (setAppendRequestBackoff) {
            followerState.setAppendEntriesRequestBackoff();
            scheduleAppendEntriesRequestBackoffResetTask();
        }

        send(request, follower);
    }

    /**
     * Applies the committed log entries between {@code lastApplied} and
     * {@code commitIndex}, if there's any available. If new entries are
     * applied, {@link RaftState}'s {@code lastApplied} field is also updated.
     *
     * @see RaftState#lastApplied()
     * @see RaftState#commitIndex()
     */
    public void applyLogEntries() {
        // Reject logs we've applied already
        long commitIndex = state.commitIndex();
        long lastApplied = state.lastApplied();

        if (commitIndex == lastApplied) {
            return;
        }

        // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
        assert commitIndex > lastApplied :
                "commit index: " + commitIndex + " cannot be smaller than last applied: " + lastApplied;

        // Apply all the preceding logs
        RaftLog raftLog = state.log();
        for (long idx = state.lastApplied() + 1; idx <= commitIndex; idx++) {
            LogEntry entry = raftLog.getLogEntry(idx);
            if (entry == null) {
                String msg = localEndpointName + " Failed to get log entry at index: " + idx;
                LOGGER.error(msg);
                throw new AssertionError(msg);
            }

            applyLogEntry(entry);

            // Update the lastApplied index
            state.lastApplied(idx);
        }

        assert (status != TERMINATED || commitIndex == raftLog.lastLogOrSnapshotIndex()) :
                "commit index: " + commitIndex + " must be equal to " + raftLog.lastLogOrSnapshotIndex() + " on termination.";

        if (state.role() == LEADER || state.role() == FOLLOWER) {
            tryTakeSnapshot();
        }
    }

    /**
     * Applies the log entry by executing its operation and sets execution
     * result to the related future if any available.
     */
    private void applyLogEntry(LogEntry entry) {
        LOGGER.debug("{} Processing {}", localEndpointName, entry);

        Object response = null;
        Object operation = entry.operation();
        if (operation instanceof RaftGroupOp) {
            if (operation instanceof TerminateRaftGroupOp) {
                setStatus(TERMINATED);
                integration.close();
            } else if (operation instanceof UpdateRaftGroupMembersOp) {
                if (state.lastGroupMembers().index() < entry.index()) {
                    setStatus(UPDATING_GROUP_MEMBER_LIST);
                    updateGroupMembers(entry.index(), ((UpdateRaftGroupMembersOp) operation).getMembers());
                }

                assert status == UPDATING_GROUP_MEMBER_LIST : "STATUS: " + status;
                assert state.lastGroupMembers().index() == entry.index();

                state.commitGroupMembers();
                UpdateRaftGroupMembersOp groupOp = (UpdateRaftGroupMembersOp) operation;
                if (groupOp.getEndpoint().equals(state.localEndpoint()) && groupOp.getMode() == MembershipChangeMode.REMOVE) {
                    setStatus(STEPPED_DOWN);
                    // If I am the leader, I may have some waiting futures whose operations are already committed
                    // but responses are not decided yet. When I leave the cluster after my shutdown, invocations
                    // of those futures will receive MemberLeftException and retry. However, if I have an invocation
                    // during the shutdown process, its future will not complete unless I notify it here.
                    // Although LeaderDemotedException is designed for another case, we use it here since
                    // invocations internally retry when they receive LeaderDemotedException.
                    invalidateFuturesUntil(entry.index() - 1, new LeaderDemotedException(state.localEndpoint(), null));
                    integration.close();
                } else {
                    setStatus(ACTIVE);
                }
                response = entry.index();
            } else {
                response = new IllegalArgumentException("Invalid Raft group operation: " + operation);
            }
        } else {
            try {
                response = integration.runOperation(operation, entry.index());
            } catch (Throwable t) {
                response = t;
            }
        }

        completeFuture(entry.index(), response);
    }

    /**
     * Updates the last append entries request timestamp to now
     */
    public void updateLastAppendEntriesTimestamp() {
        lastAppendEntriesRequestTimestamp = System.currentTimeMillis();
    }

    /**
     * Returns true if this node has received an append entries request
     * timestamp in a time less than the leader election timeout period.
     */
    public boolean isAppendEntriesRequestReceivedRecently() {
        return lastAppendEntriesRequestTimestamp > System.currentTimeMillis() - getRandomizedLeaderElectionTimeoutMs();
    }

    /**
     * Returns the Raft state
     */
    public RaftState state() {
        return state;
    }

    /**
     * Executes the given query operation sets execution result to the future.
     * <p>
     * Please note that the given operation must not make any mutation on the
     * state machine.
     */
    public void runQuery(Object operation, InternallyCompletableFuture<Object> resultFuture) {
        try {
            Object result = integration.runOperation(operation, state.commitIndex());
            resultFuture.internalComplete(result);
        } catch (Throwable t) {
            resultFuture.internalCompleteExceptionally(t);
        }
    }

    /**
     * Registers the given future with its {@code entryIndex}. This future will
     * be notified when the corresponding operation is committed or its log
     * entry is reverted.
     */
    public void registerFuture(long entryIndex, InternallyCompletableFuture<Object> future) {
        InternallyCompletableFuture<Object> f = futures.put(entryIndex, future);
        assert f == null : "Future object is already registered for entry index: " + entryIndex;
    }

    /**
     * Completes the denoted with the given result.
     */
    private void completeFuture(long entryIndex, Object result) {
        InternallyCompletableFuture<Object> f = futures.remove(entryIndex);
        if (f != null) {
            if (result instanceof Throwable) {
                f.internalCompleteExceptionally((Throwable) result);
            } else {
                f.internalComplete(result);
            }
        }
    }

    /**
     * Completes futures with {@link LeaderDemotedException} for
     * indices greater than or equal to the given index.
     * Note that {@code entryIndex} is inclusive.
     */
    public void invalidateFuturesFrom(long entryIndex) {
        Exception ex = new LeaderDemotedException(state.localEndpoint(), state.leader());
        int count = 0;
        Iterator<Entry<Long, InternallyCompletableFuture<Object>>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Long, InternallyCompletableFuture<Object>> entry = iterator.next();
            long index = entry.getKey();
            if (index >= entryIndex) {
                entry.getValue().internalCompleteExceptionally(ex);
                iterator.remove();
                count++;
            }
        }

        if (count > 0) {
            LOGGER.warn("{} Invalidated {} futures from log index: {}", localEndpointName, count, entryIndex);
        }
    }

    /**
     * Completes futures with the given exception for indices smaller than
     * or equal to the given index. Note that {@code entryIndex} is inclusive.
     */
    private void invalidateFuturesUntil(long entryIndex, RaftException ex) {
        int count = 0;
        Iterator<Entry<Long, InternallyCompletableFuture<Object>>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Long, InternallyCompletableFuture<Object>> entry = iterator.next();
            long index = entry.getKey();
            if (index <= entryIndex) {
                entry.getValue().internalCompleteExceptionally(ex);
                iterator.remove();
                count++;
            }
        }

        if (count > 0) {
            LOGGER.warn("{} Completed {} futures until log index: {} with {}", localEndpointName, count, entryIndex, ex);
        }
    }

    /**
     * Takes a snapshot if the advance in {@code commitIndex} is equal to
     * {@link RaftConfig#getCommitIndexAdvanceCountToTakeSnapshot()}.
     * <p>
     * Snapshot is not created if there's an ongoing membership change or
     * the Raft group is being terminated.
     */
    private void tryTakeSnapshot() {
        long commitIndex = state.commitIndex();
        if ((commitIndex - state.log().snapshotIndex()) < config.getCommitIndexAdvanceCountToTakeSnapshot()) {
            return;
        }

        if (status.isTerminal()) {
            // If the status is UPDATING_MEMBER_LIST or TERMINATING, it means the status is normally ACTIVE
            // and there is an appended but not-yet committed RaftGroupOp.
            // If the status is TERMINATED or STEPPED_DOWN, then there will not be any new appends.
            return;
        }

        RaftLog log = state.log();
        Object snapshot;
        try {
            snapshot = integration.takeSnapshot(commitIndex);
        } catch (Throwable t) {
            snapshot = t;
        }

        if (snapshot instanceof Throwable) {
            Throwable t = (Throwable) snapshot;
            LOGGER.error(localEndpointName + " Could not take snapshot at commit index: " + commitIndex, t);
            return;
        }

        int snapshotTerm = log.getLogEntry(commitIndex).term();
        RaftGroupMembers members = state.committedGroupMembers();
        SnapshotEntry snapshotEntry = new SnapshotEntry(snapshotTerm, commitIndex, snapshot, members.index(), members.members());

        long highestLogIndexToTruncate = commitIndex - maxLogEntryCountToKeepAfterSnapshot;
        LeaderState leaderState = state.leaderState();
        if (leaderState != null) {
            long[] matchIndices = leaderState.matchIndices();
            // Last slot is reserved for leader index and always zero.

            // If there is at least one follower with unknown match index,
            // its log can be close to the leader's log so we are keeping the old log entries.
            boolean allMatchIndicesKnown = Arrays.stream(matchIndices, 0, matchIndices.length - 1).noneMatch(i -> i == 0);

            if (allMatchIndicesKnown) {
                // Otherwise, we will keep the log entries until the minimum match index
                // that is bigger than (commitIndex - maxNumberOfLogsToKeepAfterSnapshot).
                // If there is no such follower (all of the minority followers are far behind),
                // then there is no need to keep the old log entries.
                highestLogIndexToTruncate = Arrays.stream(matchIndices)
                                                  // No need to keep any log entry if all followers are up to date
                                                  .filter(i -> i < commitIndex)
                                                  .filter(i -> i > commitIndex - maxLogEntryCountToKeepAfterSnapshot)
                                                  // We should not delete the smallest matchIndex
                                                  .map(i -> i - 1).sorted().findFirst().orElse(commitIndex);
            }
        }

        int truncatedEntryCount = log.setSnapshot(snapshotEntry, highestLogIndexToTruncate);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    localEndpointName + " " + snapshotEntry + " is taken. " + truncatedEntryCount + " entries are truncated.");
        }
    }

    /**
     * Installs the snapshot sent by the leader if it's not already installed.
     *
     * @return true if snapshot is installed, false otherwise.
     */
    public boolean installSnapshot(SnapshotEntry snapshot) {
        long commitIndex = state.commitIndex();
        if (commitIndex > snapshot.index()) {
            LOGGER.info("{} Ignored stale snapshot of commit index: {}, current commit index: {}", localEndpointName,
                    snapshot.index(), commitIndex);
            return false;
        } else if (commitIndex == snapshot.index()) {
            LOGGER.info("{} Ignored snapshot of commit index: {} since commit index is same.", localEndpointName,
                    snapshot.index());
            return true;
        }

        state.commitIndex(snapshot.index());
        int truncated = state.log().setSnapshot(snapshot);
        if (truncated > 0) {
            LOGGER.info("{} {} entries are truncated to install snapshot at commit index: {}", localEndpointName, truncated,
                    snapshot);
        }

        integration.restoreSnapshot(snapshot.operation(), snapshot.index());

        // If I am installing a snapshot, it means I am still present in the last member list,
        // but it is possible that the last entry I appended before the snapshot could be a membership change.
        // Because of this, I need to update my status.
        // Nevertheless, I may not be present in the restored member list, which is ok.

        setStatus(ACTIVE);
        state.restoreGroupMembers(snapshot.groupMembersLogIndex(), snapshot.groupMembers());
        onRaftNodeStateChange();

        state.lastApplied(snapshot.index());
        invalidateFuturesUntil(snapshot.index(), new OperationResultUnknownException(state.leader()));

        LOGGER.info("{} snapshot is installed at commit index: {}", localEndpointName, snapshot.index());

        return true;
    }

    private void onRaftNodeStateChange() {
        RaftStateSummary summary = state.summarize(status);
        if (LOGGER.isDebugEnabled()) {
            Object groupId = state.groupId();
            StringBuilder sb = new StringBuilder(localEndpointName).append(" Raft Group Members {").append("groupId: ")
                                                                   .append(groupId).append(", size:")
                                                                   .append(summary.getMembers().memberCount()).append(", term:")
                                                                   .append(summary.getTerm()).append(", logIndex:")
                                                                   .append(summary.getMembers().index()).append("} [");

            summary.getMembers().members().forEach(member -> {
                sb.append("\n\t").append(member.identifierString());
                if (state.localEndpoint().equals(member)) {
                    sb.append(" - ").append(state.role()).append(" this");
                } else if (member.equals(state.leader())) {
                    sb.append(" - ").append(LEADER);
                }
            });
            sb.append("\n]\n");
            LOGGER.debug(sb.toString());
        }

        integration.onRaftStateChange(summary);
    }

    /**
     * Updates Raft group members.
     *
     * @see RaftState#updateGroupMembers(long, Collection)
     */
    public void updateGroupMembers(long logIndex, Collection<RaftEndpoint> members) {
        state.updateGroupMembers(logIndex, members);
        onRaftNodeStateChange();
    }

    /**
     * Resets the Raft group members back to the committed Raft group members.
     *
     * @see RaftState#resetGroupMembers()
     */
    public void resetGroupMembers() {
        state.resetGroupMembers();
        onRaftNodeStateChange();
    }

    /**
     * Schedules a task to reset append entries request backoff periods,
     * if not scheduled already.
     *
     * @see RaftConfig#getAppendEntriesRequestBackoffTimeoutMs()
     */
    private void scheduleAppendEntriesRequestBackoffResetTask() {
        if (appendEntriesRequestBackoffResetTaskScheduled) {
            return;
        }

        appendEntriesRequestBackoffResetTaskScheduled = true;
        integration.schedule(appendRequestBackoffResetTask, config.getAppendEntriesRequestBackoffTimeoutMs(), MILLISECONDS);
    }

    /**
     * Switches this node to the follower role by clearing the known leader
     * endpoint and (pre) candidate states, and updating the term. If this Raft
     * node was leader before switching to the follower state, it may have some
     * queries waiting to be executed. Those queries are also failed with
     * {@link LeaderDemotedException}. After the state switch,
     * {@link RaftIntegration#onRaftStateChange(RaftStateSummary)} is called.
     *
     * @param term the new term to switch
     */
    public void toFollower(int term) {
        LeaderState leaderState = state.leaderState();
        if (leaderState != null) {
            leaderState.queryState().operations().stream().map(Map.Entry::getValue)
                       .forEach(f -> f.internalCompleteExceptionally(new LeaderDemotedException(state.localEndpoint(), null)));
        }

        state.toFollower(term);
        onRaftNodeStateChange();
    }

    /**
     * Updates the known leader endpoint and calls
     * {@link RaftIntegration#onRaftStateChange(RaftStateSummary)}.
     *
     * @param member the discovered leader endpoint
     */
    public void leader(RaftEndpoint member) {
        state.leader(member);
        onRaftNodeStateChange();
    }

    /**
     * Switches this Raft node to the leader role by performing the following
     * steps:
     * <ul>
     * <li>Setting the local endpoint as the current leader,</li>
     * <li>Clearing (pre)candidate states,</li>
     * <li>Initializing the leader state for the current members,</li>
     * <li>Appending an operation to the Raft log if enabled,</li>
     * <li>Scheduling the periodic heartbeat task,</li>
     * <li>Printing the member state,</li>
     * <li>Calling {@link RaftIntegration#onRaftStateChange(RaftStateSummary)}.</li>
     * </ul>
     */
    public void toLeader() {
        state.toLeader();
        appendEntryAfterLeaderElection();
        onRaftNodeStateChange();
        scheduleHeartbeat();
    }

    /**
     * Switches this Raft node to the candidate role and starts a new
     * leader election round.
     * {@link RaftIntegration#onRaftStateChange(RaftStateSummary)} is called
     * as well. Regular leader elections are non-disruptive, meaning that
     * leader stickiness will be considered by other Raft nodes when they
     * receive vote requests. A disruptive leader election occurs when
     * the current Raft group leader tries to transfer leadership to another
     * member.
     *
     * @param disruptive denotes if the leader election will be disruptive
     */
    public void toCandidate(boolean disruptive) {
        VoteRequest request = state.toCandidate(disruptive);
        LOGGER.info("{} Leader election started for term: {}, last log index: {}, last log term: {}", localEndpointName,
                request.term(), request.lastLogIndex(), request.lastLogTerm());
        onRaftNodeStateChange();

        state.remoteMembers().forEach(member -> send(request, member));

        integration.schedule(new LeaderElectionTimeoutTask(this), getRandomizedLeaderElectionTimeoutMs(), MILLISECONDS);
    }

    /**
     * Initiates the pre-voting step for the next term. The pre-voting
     * step is executed to check if other group members would vote for
     * this Raft node if it would start a new leader election.
     */
    public void preCandidate() {
        state.initPreCandidateState();
        int nextTerm = state.term() + 1;
        LogEntry lastEntry = state.log().lastLogOrSnapshotEntry();
        RaftMsg request = new PreVoteRequest(getLocalEndpoint(), nextTerm, lastEntry.term(), lastEntry.index());

        LOGGER.info("{} Pre-vote started for next term: {}, last log index: {}, last log term: {}", localEndpointName, nextTerm,
                lastEntry.index(), lastEntry.term());
        onRaftNodeStateChange();

        state.remoteMembers().forEach(member -> send(request, member));

        integration.schedule(new PreVoteTimeoutTask(this, state.term()), getRandomizedLeaderElectionTimeoutMs(), MILLISECONDS);
    }

    private void initLeadershipTransfer(RaftEndpoint targetEndpoint, InternallyCompletableFuture<Object> resultFuture) {
        if (checkLeadershipTransfer(targetEndpoint, resultFuture)) {
            return;
        }

        if (getLocalEndpoint().equals(targetEndpoint)) {
            LOGGER.warn("{} I am already the leader... There is no leadership transfer to myself.", localEndpointName);
            resultFuture.internalCompleteNull();
            return;
        }

        if (state.initLeadershipTransfer(targetEndpoint, resultFuture)) {
            transferLeadership();
        }
    }

    private boolean checkLeadershipTransfer(RaftEndpoint targetEndpoint, InternallyCompletableFuture<Object> resultFuture) {
        if (status.isTerminal()) {
            resultFuture.internalCompleteExceptionally(status.createException(getLocalEndpoint()));
            return true;
        }

        if (!getCommittedMembers().members().contains(targetEndpoint)) {
            resultFuture.internalCompleteExceptionally(new IllegalArgumentException(
                    "Cannot transfer leadership to " + targetEndpoint
                            + " because it is not in the committed group member list!"));
            return true;
        }

        if (getStatus() != RaftNodeStatus.ACTIVE) {
            resultFuture.internalCompleteExceptionally(new IllegalStateException(
                    "Cannot transfer leadership to " + targetEndpoint + " because the status is " + getStatus()));
            return true;
        }

        if (state.leaderState() == null) {
            resultFuture.internalCompleteExceptionally(new IllegalStateException(
                    "Cannot transfer leadership to " + targetEndpoint + " because I am not the leader!"));
            return true;
        }

        return false;
    }

    private void transferLeadership() {
        LeaderState leaderState = state.leaderState();

        if (leaderState == null) {
            LOGGER.debug("{} Not retrying leadership transfer since not leader...", localEndpointName);
            return;
        }

        LeadershipTransferState leadershipTransferState = state.leadershipTransferState();
        checkTrue(leadershipTransferState != null, "No leadership transfer state!");

        if (!leadershipTransferState.retry()) {
            String msg = localEndpointName + " Leadership transfer to " + leadershipTransferState.endpoint() + " timed out!";
            LOGGER.warn(msg);
            state.completeLeadershipTransfer(new TimeoutException(msg));
            return;
        }

        RaftEndpoint targetEndpoint = leadershipTransferState.endpoint();
        long delayMs = leadershipTransferState.retryDelay(getRandomizedLeaderElectionTimeoutMs());

        if (state.commitIndex() < state.log().lastLogOrSnapshotIndex()) {
            LOGGER.warn("{} Waiting until all appended entries to be committed before transferring leadership to {}",
                    localEndpointName, targetEndpoint);
            integration.schedule(this::transferLeadership, delayMs, MILLISECONDS);
            return;
        }

        if (leadershipTransferState.tryCount() > 1) {
            LOGGER.debug("{} Retrying leadership transfer to {}", localEndpointName, leadershipTransferState.endpoint());
        } else {
            LOGGER.info("{} Transferring leadership to {}", localEndpointName, leadershipTransferState.endpoint());
        }

        leaderState.getFollowerState(targetEndpoint).resetAppendEntriesRequestBackoff();
        sendAppendEntriesRequest(targetEndpoint);

        LogEntry entry = state.log().lastLogOrSnapshotEntry();
        send(new TriggerLeaderElectionRequest(getLocalEndpoint(), state.term(), entry.term(), entry.index()), targetEndpoint);

        integration.schedule(this::transferLeadership, delayMs, MILLISECONDS);
    }

    private void appendEntryAfterLeaderElection() {
        Object operation = integration.getOperationToAppendAfterLeaderElection();
        if (operation != null) {
            RaftLog log = state.log();
            log.appendEntries(new LogEntry(state.term(), log.lastLogOrSnapshotIndex() + 1, operation));
        }
    }

    /**
     * Scheduled only on the Raft group leader with
     * {@link RaftConfig#getLeaderHeartbeatPeriodMs()} delay to denote leader
     * liveliness by sending periodic append entries requests to followers.
     */
    private class HeartbeatTask
            extends RaftNodeStatusAwareTask {
        HeartbeatTask() {
            super(RaftNodeImpl.this);
        }

        @Override
        protected void doRun() {
            if (state.role() == LEADER) {
                if (lastAppendEntriesRequestTimestamp < (System.currentTimeMillis() - config.getLeaderHeartbeatPeriodMs())) {
                    broadcastAppendEntriesRequest();
                }

                scheduleHeartbeat();
            }
        }
    }

    /**
     * Checks whether currently there is a known leader endpoint and triggers
     * the pre-voting mechanism there is no known leader or the leader has
     * timed out.
     */
    private class LeaderFailureDetectionTask
            extends RaftNodeStatusAwareTask {
        LeaderFailureDetectionTask() {
            super(RaftNodeImpl.this);
        }

        @Override
        protected void doRun() {
            try {
                RaftEndpoint leader = state.leader();
                if (leader == null) {
                    if (state.role() == FOLLOWER) {
                        LOGGER.warn("{} We are FOLLOWER and there is no current leader. Will start new election round...",
                                localEndpointName);
                        runPreVoteTask();
                    }
                } else if (isLeaderTimedOut()) {
                    // Even though leader endpoint is reachable by raft-integration,
                    // leader itself may be crashed and another member may be restarted on the same endpoint.
                    LOGGER.warn("{} Current leader {}'s heartbeats are timed-out. Will start new election round...",
                            localEndpointName, leader);
                    resetLeaderAndStartElection();
                } else if (!state.committedGroupMembers().isKnownMember(leader)) {
                    LOGGER.warn("{} Current leader {} is not member anymore. Will start new election round...", localEndpointName,
                            leader);
                    resetLeaderAndStartElection();
                }
            } finally {
                scheduleLeaderFailureDetection();
            }
        }

        private boolean isLeaderTimedOut() {
            long missedHeartbeatThresholdMs = config.getMaxMissedLeaderHeartbeatCount() * config.getLeaderHeartbeatPeriodMs();
            return lastAppendEntriesRequestTimestamp + missedHeartbeatThresholdMs < System.currentTimeMillis();
        }

        final void resetLeaderAndStartElection() {
            leader(null);
            runPreVoteTask();
        }

        private void runPreVoteTask() {
            if (state.preCandidateState() == null) {
                new PreVoteTask(RaftNodeImpl.this, state.term()).run();
            }
        }
    }

    /**
     * If the append entries request backoff period is active for any follower,
     * this task will send a new append entries request on the backoff
     * completion.
     */
    private class AppendEntriesRequestBackoffResetTask
            extends RaftNodeStatusAwareTask {
        AppendEntriesRequestBackoffResetTask() {
            super(RaftNodeImpl.this);
        }

        @Override
        protected void doRun() {
            appendEntriesRequestBackoffResetTaskScheduled = false;
            LeaderState leaderState = state.leaderState();

            if (leaderState != null) {
                for (Entry<RaftEndpoint, FollowerState> entry : leaderState.getFollowerStates().entrySet()) {
                    FollowerState followerState = entry.getValue();
                    if (!followerState.isAppendEntriesRequestBackoffSet()) {
                        continue;
                    }

                    if (followerState.completeAppendEntriesRequestBackoffRound()) {
                        // This follower has not sent a response to the last append request.
                        // Send another append request
                        sendAppendEntriesRequest(entry.getKey());
                    }

                    // Schedule the task again, we still have backoff flag set followers
                    scheduleAppendEntriesRequestBackoffResetTask();
                }
            }
        }
    }

    private class RaftStateSummaryPublishTask
            extends RaftNodeStatusAwareTask {
        RaftStateSummaryPublishTask() {
            super(RaftNodeImpl.this);
        }

        @Override
        protected void doRun() {
            scheduleRaftStateSummaryPublishTask();
            integration.onRaftStateChange(state.summarize(status));
        }
    }

}
