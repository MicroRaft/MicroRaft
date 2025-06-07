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

package io.microraft.impl;

import static io.microraft.RaftNodeStatus.ACTIVE;
import static io.microraft.RaftNodeStatus.INITIAL;
import static io.microraft.RaftNodeStatus.TERMINATED;
import static io.microraft.RaftNodeStatus.UPDATING_RAFT_GROUP_MEMBER_LIST;
import static io.microraft.RaftNodeStatus.isTerminal;
import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEADER;
import static io.microraft.RaftRole.LEARNER;
import static io.microraft.impl.log.RaftLog.FIRST_VALID_LOG_INDEX;
import static io.microraft.impl.log.RaftLog.getLogCapacity;
import static io.microraft.impl.log.RaftLog.getMaxLogEntryCountToKeepAfterSnapshot;
import static io.microraft.model.log.SnapshotEntry.isNonInitial;
import static java.lang.Math.min;
import static java.util.Arrays.sort;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.shuffle;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.microraft.model.log.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.microraft.MembershipChangeMode;
import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.RaftNodeStatus;
import io.microraft.exception.CannotReplicateException;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.exception.LaggingCommitIndexException;
import io.microraft.exception.NotLeaderException;
import io.microraft.exception.RaftException;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.impl.handler.AppendEntriesFailureResponseHandler;
import io.microraft.impl.handler.AppendEntriesRequestHandler;
import io.microraft.impl.handler.AppendEntriesSuccessResponseHandler;
import io.microraft.impl.handler.InstallSnapshotRequestHandler;
import io.microraft.impl.handler.InstallSnapshotResponseHandler;
import io.microraft.impl.handler.PreVoteRequestHandler;
import io.microraft.impl.handler.PreVoteResponseHandler;
import io.microraft.impl.handler.TriggerLeaderElectionHandler;
import io.microraft.impl.handler.VoteRequestHandler;
import io.microraft.impl.handler.VoteResponseHandler;
import io.microraft.impl.log.RaftLog;
import io.microraft.impl.report.RaftLogStatsImpl;
import io.microraft.impl.report.RaftNodeReportImpl;
import io.microraft.impl.state.FollowerState;
import io.microraft.impl.state.LeaderState;
import io.microraft.impl.state.QueryState;
import io.microraft.impl.state.RaftGroupMembersState;
import io.microraft.impl.state.RaftState;
import io.microraft.impl.state.RaftTermState;
import io.microraft.impl.state.QueryState.QueryContainer;
import io.microraft.impl.statemachine.InternalCommitAware;
import io.microraft.impl.statemachine.NoOp;
import io.microraft.impl.task.HeartbeatTask;
import io.microraft.impl.task.LeaderBackoffResetTask;
import io.microraft.impl.task.LeaderElectionTimeoutTask;
import io.microraft.impl.task.FlushTask;
import io.microraft.impl.task.MembershipChangeTask;
import io.microraft.impl.task.PreVoteTask;
import io.microraft.impl.task.PreVoteTimeoutTask;
import io.microraft.impl.task.QueryTask;
import io.microraft.impl.task.RaftStateSummaryPublishTask;
import io.microraft.impl.task.ReplicateTask;
import io.microraft.impl.task.TransferLeadershipTask;
import io.microraft.impl.util.OrderedFuture;
import io.microraft.lifecycle.RaftNodeLifecycleAware;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.groupop.RaftGroupOp;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesRequest.AppendEntriesRequestBuilder;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.model.message.InstallSnapshotResponse;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.model.message.PreVoteResponse;
import io.microraft.model.message.RaftMessage;
import io.microraft.model.message.TriggerLeaderElectionRequest;
import io.microraft.model.message.VoteRequest;
import io.microraft.model.message.VoteResponse;
import io.microraft.persistence.NopRaftStore;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;
import io.microraft.report.RaftGroupMembers;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReport.RaftNodeReportReason;
import io.microraft.report.RaftNodeReportListener;
import io.microraft.statemachine.StateMachine;
import io.microraft.transport.Transport;

/**
 * Implementation of {@link RaftNode}.
 * <p>
 * Each Raft node runs in a single-threaded manner with an event-based approach.
 * Raft node uses {@link RaftNodeExecutor} to run its tasks,
 * {@link StateMachine} to execute committed operations on the user-supplied
 * state machine, and {@link RaftStore} to persist internal Raft state to stable
 * storage.
 */
public final class RaftNodeImpl implements RaftNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);
    private static final int LEADER_ELECTION_TIMEOUT_NOISE_MILLIS = 100;
    private static final long LEADER_BACKOFF_RESET_TASK_PERIOD_MILLIS = 250;
    private static final int MIN_BACKOFF_ROUNDS = 4;

    private final Object groupId;
    private final RaftState state;
    private final RaftConfig config;
    private final Transport transport;
    private final RaftNodeExecutor executor;
    private final StateMachine stateMachine;
    private final RaftModelFactory modelFactory;
    private final RaftStore store;
    private final RaftNodeReportListener raftNodeReportListener;
    private final String localEndpointStr;

    private final Random random;
    private final Clock clock;

    private final long leaderHeartbeatTimeoutMillis;
    private final int commitCountToTakeSnapshot;
    private final int appendEntriesRequestBatchSize;
    private final int maxPendingLogEntryCount;
    private final int maxLogEntryCountToKeepAfterSnapshot;
    private final int maxBackoffRounds;

    private Runnable leaderBackoffResetTask;
    private Runnable leaderFlushTask;

    private final List<RaftNodeLifecycleAware> lifecycleAwareComponents = new ArrayList<>();
    private final List<RaftNodeLifecycleAware> startedLifecycleAwareComponents = new ArrayList<>();

    private long lastLeaderHeartbeatTimestamp;
    private volatile RaftNodeStatus status = INITIAL;

    private int takeSnapshotCount;
    private int installSnapshotCount;

    @SuppressWarnings("checkstyle:executablestatementcount")
    RaftNodeImpl(Object groupId, RaftEndpoint localEndpoint, RaftGroupMembersView initialGroupMembers,
            RaftConfig config, RaftNodeExecutor executor, StateMachine stateMachine, Transport transport,
            RaftModelFactory modelFactory, RaftStore store, RaftNodeReportListener raftNodeReportListener,
            Random random, Clock clock) {
        requireNonNull(localEndpoint);
        this.groupId = requireNonNull(groupId);
        this.transport = requireNonNull(transport);
        this.executor = requireNonNull(executor);
        this.stateMachine = requireNonNull(stateMachine);
        this.modelFactory = requireNonNull(modelFactory);
        this.store = requireNonNull(store);
        this.raftNodeReportListener = requireNonNull(raftNodeReportListener);
        this.config = requireNonNull(config);
        this.localEndpointStr = localEndpoint.getId() + "<" + groupId + ">";
        this.leaderHeartbeatTimeoutMillis = SECONDS.toMillis(config.getLeaderHeartbeatTimeoutSecs());
        this.commitCountToTakeSnapshot = config.getCommitCountToTakeSnapshot();
        this.appendEntriesRequestBatchSize = config.getAppendEntriesRequestBatchSize();
        this.maxPendingLogEntryCount = config.getMaxPendingLogEntryCount();
        this.maxLogEntryCountToKeepAfterSnapshot = getMaxLogEntryCountToKeepAfterSnapshot(commitCountToTakeSnapshot);
        int logCapacity = getLogCapacity(commitCountToTakeSnapshot, maxPendingLogEntryCount);
        this.state = RaftState.create(groupId, localEndpoint, initialGroupMembers, logCapacity, store, modelFactory);
        this.maxBackoffRounds = getMaxBackoffRounds(config);
        this.random = requireNonNull(random);
        this.clock = requireNonNull(clock);
        populateLifecycleAwareComponents();
    }

    @SuppressWarnings("checkstyle:executablestatementcount")
    RaftNodeImpl(Object groupId, RestoredRaftState restoredState, RaftConfig config, RaftNodeExecutor executor,
            StateMachine stateMachine, Transport transport, RaftModelFactory modelFactory, RaftStore store,
            RaftNodeReportListener raftNodeReportListener, Random random, Clock clock) {
        requireNonNull(store);
        this.groupId = requireNonNull(groupId);
        this.transport = requireNonNull(transport);
        this.executor = requireNonNull(executor);
        this.stateMachine = requireNonNull(stateMachine);
        this.modelFactory = requireNonNull(modelFactory);
        this.store = requireNonNull(store);
        this.raftNodeReportListener = requireNonNull(raftNodeReportListener);
        this.config = requireNonNull(config);
        this.localEndpointStr = restoredState.getLocalEndpointPersistentState().getLocalEndpoint().getId() + "<"
                + groupId + ">";
        this.leaderHeartbeatTimeoutMillis = SECONDS.toMillis(config.getLeaderHeartbeatTimeoutSecs());
        this.commitCountToTakeSnapshot = config.getCommitCountToTakeSnapshot();
        this.appendEntriesRequestBatchSize = config.getAppendEntriesRequestBatchSize();
        this.maxPendingLogEntryCount = config.getMaxPendingLogEntryCount();
        this.maxLogEntryCountToKeepAfterSnapshot = getMaxLogEntryCountToKeepAfterSnapshot(commitCountToTakeSnapshot);
        int logCapacity = getLogCapacity(commitCountToTakeSnapshot, maxPendingLogEntryCount);
        this.state = RaftState.restore(groupId, restoredState, logCapacity, store, modelFactory);
        this.maxBackoffRounds = getMaxBackoffRounds(config);
        this.random = requireNonNull(random);
        this.clock = requireNonNull(clock);
        populateLifecycleAwareComponents();
    }

    private void populateLifecycleAwareComponents() {
        for (Object component : List.of(executor, transport, stateMachine, store, modelFactory,
                raftNodeReportListener)) {
            if (component instanceof RaftNodeLifecycleAware) {
                lifecycleAwareComponents.add((RaftNodeLifecycleAware) component);
            }
        }

        shuffle(lifecycleAwareComponents);
    }

    private int getMaxBackoffRounds(RaftConfig config) {
        long durationSecs;
        if (config.getLeaderHeartbeatPeriodSecs() == 1 && config.getLeaderHeartbeatTimeoutSecs() > 1) {
            durationSecs = 2;
        } else {
            durationSecs = config.getLeaderHeartbeatPeriodSecs();
        }

        return (int) (SECONDS.toMillis(durationSecs) / LEADER_BACKOFF_RESET_TASK_PERIOD_MILLIS);
    }

    /**
     * Returns true if a new operation is allowed to be replicated. This method must
     * be invoked only when the local Raft node is the Raft group leader.
     * <p>
     * Replication is not allowed, when;
     * <ul>
     * <li>The local Raft log has no more empty slots for pending entries.</li>
     * <li>The given operation is a {@link RaftGroupOp} and there's an ongoing
     * membership change in group.</li>
     * <li>The operation is a membership change and there's no committed entry in
     * the current term yet.</li>
     * </ul>
     *
     * @param operation
     *            the operation to check for replication
     *
     * @return true if the given operation can be replicated, false otherwise
     *
     * @see RaftNodeStatus
     * @see RaftGroupOp
     * @see RaftConfig#getMaxPendingLogEntryCount()
     * @see StateMachine#getNewTermOperation()
     */
    public boolean canReplicateNewOperation(Object operation) {
        RaftLog log = state.log();
        long lastLogIndex = log.lastLogOrSnapshotIndex();
        long commitIndex = state.commitIndex();
        if (lastLogIndex - commitIndex >= maxPendingLogEntryCount) {
            return false;
        }

        if (status == UPDATING_RAFT_GROUP_MEMBER_LIST) {
            return (state.effectiveGroupMembers().isKnownMember(getLocalEndpoint())
                    && !(operation instanceof RaftGroupOp));
        }

        if (operation instanceof UpdateRaftGroupMembersOp) {
            // the leader must have committed an entry in its term to make a membership
            // change
            // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

            // last committed entry is either in the last snapshot or still in the log
            BaseLogEntry lastCommittedEntry = commitIndex == log.snapshotIndex()
                    ? log.snapshotEntry()
                    : log.getLogEntry(commitIndex);
            assert lastCommittedEntry != null;

            return lastCommittedEntry.getTerm() == state.term();
        }

        return state.leadershipTransferState() == null;
    }

    /**
     * Returns true if a new query is currently allowed to be executed without
     * appending an entry to the Raft log. This method must be invoked only when the
     * local Raft node is the leader.
     * <p>
     * A new linearizable query execution is not allowed when:
     * <ul>
     * <li>If the leader has not yet committed an entry in the current term. See
     * Section 6.4 of Raft Dissertation.</li>
     * <li>There are already a lot of queries waiting to be executed.</li>
     * </ul>
     *
     * @return true if a new query can be executed with the linearizability
     *         guarantee without appending an entry to the Raft log.
     *
     * @see RaftNodeStatus
     * @see RaftConfig#getMaxPendingLogEntryCount()
     */
    public boolean canQueryLinearizable() {
        long commitIndex = state.commitIndex();
        RaftLog log = state.log();

        // If the leader has not yet marked an entry from its current term committed, it
        // waits until it has done so.
        // (§6.4)
        // last committed entry is either in the last snapshot or still in the log
        BaseLogEntry lastCommittedEntry = commitIndex == log.snapshotIndex()
                ? log.snapshotEntry()
                : log.getLogEntry(commitIndex);
        assert lastCommittedEntry != null;

        if (lastCommittedEntry.getTerm() != state.term()) {
            return false;
        }

        // We can execute multiple queries at one-shot without appending to the Raft
        // log,
        // and we use the maxPendingLogEntryCount configuration parameter to upper-bound
        // the number of queries that are collected until the heartbeat round is done.
        QueryState queryState = state.leaderState().queryState();
        return queryState.queryCount() < maxPendingLogEntryCount;
    }

    public void sendSnapshotChunk(RaftEndpoint follower, long snapshotIndex, int requestedSnapshotChunkIndex) {
        // this node can be a leader or a follower!

        LeaderState leaderState = state.leaderState();
        FollowerState followerState = null;
        if (leaderState != null) {
            followerState = leaderState.getFollowerStateOrNull(follower);
            if (followerState == null) {
                LOGGER.warn("{} follower: {} not found to send snapshot chunk.", localEndpointStr, follower.getId());
                return;
            }
        }

        SnapshotEntry snapshotEntry = state.log().snapshotEntry();
        SnapshotChunk snapshotChunk = null;
        Collection<RaftEndpoint> snapshottedMembers;

        if (snapshotEntry.getIndex() == snapshotIndex) {
            List<SnapshotChunk> snapshotChunks = (List<SnapshotChunk>) snapshotEntry.getOperation();
            snapshotChunk = snapshotChunks.get(requestedSnapshotChunkIndex);
            if (leaderState != null && snapshotEntry.getTerm() < state.term()) {
                // I am the new leader but there is no new snapshot yet.
                // So I'll send my own snapshotted members list.
                snapshottedMembers = getSnapshottedMembers(leaderState, snapshotEntry);
            } else {
                snapshottedMembers = Collections.emptyList();
            }

            LOGGER.info("{} sending snapshot chunk: {} to {} for snapshot index: {}", localEndpointStr,
                    requestedSnapshotChunkIndex, follower.getId(), snapshotIndex);
        } else if (snapshotEntry.getIndex() > snapshotIndex) {
            if (leaderState == null) {
                return;
            }

            // there is a new snapshot. I'll send a new snapshotted members list.
            snapshottedMembers = getSnapshottedMembers(leaderState, snapshotEntry);

            LOGGER.info(
                    "{} sending empty snapshot chunk list to {} because requested snapshot index: "
                            + "{} is smaller than the current snapshot index: {}",
                    localEndpointStr, follower.getId(), snapshotIndex, snapshotEntry.getIndex());
        } else {
            LOGGER.error(
                    "{} requested snapshot index: {} for snapshot chunk indices: {} from {} is bigger than "
                            + "current snapshot index: {}",
                    localEndpointStr, snapshotIndex, requestedSnapshotChunkIndex, follower, snapshotEntry.getIndex());
            return;
        }

        RaftMessage request = modelFactory.createInstallSnapshotRequestBuilder().setGroupId(getGroupId())
                .setSender(getLocalEndpoint()).setTerm(state.term()).setSenderLeader(leaderState != null)
                .setSnapshotTerm(snapshotEntry.getTerm()).setSnapshotIndex(snapshotEntry.getIndex())
                .setTotalSnapshotChunkCount(snapshotEntry.getSnapshotChunkCount()).setSnapshotChunk(snapshotChunk)
                .setSnapshottedMembers(snapshottedMembers).setGroupMembersView(snapshotEntry.getGroupMembersView())
                .setQuerySequenceNumber(
                        (leaderState != null) ? leaderState.querySequenceNumber(state.isVotingMember(follower)) : 0)
                .setFlowControlSequenceNumber(followerState != null ? enableBackoff(followerState) : 0).build();

        send(follower, request);

        if (followerState != null) {
            scheduleLeaderRequestBackoffResetTask(leaderState);
        }
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

    @Nonnull
    @Override
    public RaftConfig getConfig() {
        return config;
    }

    @Nonnull
    @Override
    public RaftTermState getTerm() {
        // volatile read
        return state.termState();
    }

    @Nonnull
    @Override
    public RaftNodeStatus getStatus() {
        // volatile read
        return status;
    }

    /**
     * Updates status of the Raft node with the given status.
     *
     * @param newStatus
     *            the new status to set on the local Raft node
     */
    public void setStatus(RaftNodeStatus newStatus) {
        if (isTerminal(status)) {
            throw new IllegalStateException("Cannot set status: " + newStatus + " since already " + this.status);
        } else if (this.status == newStatus) {
            return;
        }

        this.status = newStatus;

        if (newStatus == ACTIVE) {
            LOGGER.info("{} Status is set to {}", localEndpointStr, newStatus);
        } else {
            LOGGER.warn("{} Status is set to {}", localEndpointStr, newStatus);
        }

        publishRaftNodeReport(RaftNodeReportReason.STATUS_CHANGE);
    }

    @Nonnull
    @Override
    public RaftGroupMembersState getInitialMembers() {
        return state.initialMembers();
    }

    @Nonnull
    @Override
    public RaftGroupMembersState getCommittedMembers() {
        return state.committedGroupMembers();
    }

    @Nonnull
    @Override
    public RaftGroupMembersState getEffectiveMembers() {
        return state.effectiveGroupMembers();
    }

    @Nonnull
    @Override
    public CompletableFuture<Ordered<Object>> start() {
        OrderedFuture<Object> future = new OrderedFuture<>();
        if (status != INITIAL) {
            future.fail(new IllegalStateException("Cannot start RaftNode when " + status));
            return future;
        }

        executor.execute(() -> {
            if (status != INITIAL) {
                future.fail(
                        new IllegalStateException("Cannot start RaftNode of `" + localEndpointStr + " when " + status));
                return;
            }

            LOGGER.info("{} Starting for {} with {} members: {} and voting members; {}.", localEndpointStr, groupId,
                    state.memberCount(), state.members(), state.votingMembers());

            Throwable failure = null;
            try {
                initTasks();
                startComponents();
                initRestoredState();
                state.persistInitialState(modelFactory.createRaftGroupMembersViewBuilder());

                // the status could be UPDATING_GROUP_MEMBER_LIST after
                // restoring Raft state so we only switch to ACTIVE only if
                // the status is INITIAL.
                if (status == INITIAL) {
                    setStatus(ACTIVE);
                }

                LOGGER.info(
                        "{} -> committed members: {} effective members: {} initial members: {} leader election quorum: {} is local member voting? {} initial voting members: {}",
                        localEndpointStr, state.committedGroupMembers(), state.effectiveGroupMembers(),
                        state.initialMembers(), state.leaderElectionQuorumSize(),
                        state.initialMembers().getVotingMembers().contains(state.localEndpoint()),
                        state.initialMembers().getVotingMembers());
                if (state.committedGroupMembers().getLogIndex() == 0 && state.effectiveGroupMembers().getLogIndex() == 0
                        && state.initialMembers().getVotingMembers().contains(state.localEndpoint())
                        && state.leaderElectionQuorumSize() == 1) {
                    // this node is starting for the first time as a singleton Raft group
                    LOGGER.info("{} is the single voting member in the Raft group.", localEndpointStr());
                    toSingletonLeader();
                } else {
                    LOGGER.info("{} started.", localEndpointStr);
                    runPreVote();
                }
            } catch (Throwable t) {
                failure = t;
                LOGGER.error(localEndpointStr + " could not start.", t);

                setStatus(TERMINATED);
                terminateComponents();
            } finally {
                if (failure == null) {
                    future.completeNull(state.commitIndex());
                } else {
                    future.fail(failure);
                }
            }
        });

        return future;
    }

    public RaftNodeReportListener getRaftNodeReportListener() {
        return raftNodeReportListener;
    }

    private void initTasks() {
        if (!(store instanceof NopRaftStore)) {
            leaderFlushTask = new FlushTask(this);
        }
        leaderBackoffResetTask = new LeaderBackoffResetTask(this);
        executor.schedule(new HeartbeatTask(this), config.getLeaderHeartbeatPeriodSecs(), SECONDS);
        executor.schedule(new RaftStateSummaryPublishTask(this), config.getRaftNodeReportPublishPeriodSecs(), SECONDS);
    }

    private void startComponents() {
        for (RaftNodeLifecycleAware component : lifecycleAwareComponents) {
            startedLifecycleAwareComponents.add(component);
            component.onRaftNodeStart();
        }
    }

    private void terminateComponents() {
        for (RaftNodeLifecycleAware component : startedLifecycleAwareComponents) {
            try {
                component.onRaftNodeTerminate();
            } catch (Throwable t) {
                LOGGER.error(localEndpointStr + " failure during termination of " + component, t);
            }
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<Ordered<Object>> terminate() {
        if (isTerminal(status)) {
            return CompletableFuture.completedFuture(null);
        }

        OrderedFuture<Object> future = new OrderedFuture<>();

        executor.execute(() -> {
            if (isTerminal(status)) {
                future.completeNull(state.commitIndex());
                return;
            }

            Throwable failure = null;
            boolean shouldTerminate = (status != INITIAL);
            try {
                if (shouldTerminate) {
                    toFollower(state.term());
                }
                setStatus(TERMINATED);
                state.invalidateScheduledQueries();
            } catch (Throwable t) {
                failure = t;
                LOGGER.error("Failure during termination of " + localEndpointStr, t);
                if (status != TERMINATED) {
                    setStatus(TERMINATED);
                }
            } finally {
                if (shouldTerminate) {
                    terminateComponents();
                }

                if (failure == null) {
                    future.completeNull(state.commitIndex());
                } else {
                    future.fail(failure);
                }
            }
        });

        return future;
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public void handle(@Nonnull RaftMessage message) {
        if (isTerminal(status)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.warn("{} will not handle {} because {}", localEndpointStr, message, status);
            } else {
                LOGGER.warn("{} will not handle {} because {}", localEndpointStr, message.getClass().getSimpleName(),
                        status);
            }

            return;
        }

        Runnable handler;
        if (message instanceof AppendEntriesRequest) {
            handler = new AppendEntriesRequestHandler(this, (AppendEntriesRequest) message);
        } else if (message instanceof AppendEntriesSuccessResponse) {
            handler = new AppendEntriesSuccessResponseHandler(this, (AppendEntriesSuccessResponse) message);
        } else if (message instanceof AppendEntriesFailureResponse) {
            handler = new AppendEntriesFailureResponseHandler(this, (AppendEntriesFailureResponse) message);
        } else if (message instanceof InstallSnapshotRequest) {
            handler = new InstallSnapshotRequestHandler(this, (InstallSnapshotRequest) message);
        } else if (message instanceof InstallSnapshotResponse) {
            handler = new InstallSnapshotResponseHandler(this, (InstallSnapshotResponse) message);
        } else if (message instanceof VoteRequest) {
            handler = new VoteRequestHandler(this, (VoteRequest) message);
        } else if (message instanceof VoteResponse) {
            handler = new VoteResponseHandler(this, (VoteResponse) message);
        } else if (message instanceof PreVoteRequest) {
            handler = new PreVoteRequestHandler(this, (PreVoteRequest) message);
        } else if (message instanceof PreVoteResponse) {
            handler = new PreVoteResponseHandler(this, (PreVoteResponse) message);
        } else if (message instanceof TriggerLeaderElectionRequest) {
            handler = new TriggerLeaderElectionHandler(this, (TriggerLeaderElectionRequest) message);
        } else {
            throw new IllegalArgumentException("Invalid Raft msg: " + message);
        }

        try {
            executor.execute(handler);
        } catch (Throwable t) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error(localEndpointStr + " could not handle " + message, t);
            }
        }
    }

    @Nonnull
    @Override
    public <T> CompletableFuture<Ordered<T>> replicate(@Nonnull Object operation) {
        OrderedFuture<T> future = new OrderedFuture<>();
        return executeIfRunning(new ReplicateTask(this, requireNonNull(operation), future), future);
    }

    @Nonnull
    @Override
    public <T> CompletableFuture<Ordered<List<T>>> replicate(@Nonnull List<Object> operations) {
        BatchOperation batchOperation = new BatchOperation(operations);
        OrderedFuture<List<T>> future = new OrderedFuture<>();

        return executeIfRunning(new ReplicateTask(this, batchOperation, future), future);
    }

    @Nonnull
    @Override
    public <T> CompletableFuture<Ordered<T>> query(@Nonnull Object operation, @Nonnull QueryPolicy queryPolicy,
            Optional<Long> minCommitIndex, Optional<Duration> timeout) {
        OrderedFuture<T> future = new OrderedFuture<>();
        Runnable task = new QueryTask(this, requireNonNull(operation), queryPolicy,
                Math.max(minCommitIndex.orElse(0L), 0L), timeout, future);
        return executeIfRunning(task, future);
    }

    @Nonnull
    @Override
    public CompletableFuture<Ordered<Object>> waitFor(long minCommitIndex, Duration timeout) {
        return query(NoOp.INSTANCE, QueryPolicy.EVENTUAL_CONSISTENCY, Optional.of(minCommitIndex),
                Optional.of(timeout));
    }

    @Nonnull
    @Override
    public CompletableFuture<Ordered<RaftGroupMembers>> changeMembership(@Nonnull RaftEndpoint endpoint,
            @Nonnull MembershipChangeMode mode, long expectedGroupMembersCommitIndex) {
        OrderedFuture<RaftGroupMembers> future = new OrderedFuture<>();
        Runnable task = new MembershipChangeTask(this, future, requireNonNull(endpoint), requireNonNull(mode),
                expectedGroupMembersCommitIndex);
        return executeIfRunning(task, future);
    }

    @Nonnull
    @Override
    public CompletableFuture<Ordered<Object>> transferLeadership(@Nonnull RaftEndpoint endpoint) {
        requireNonNull(endpoint);
        OrderedFuture<Object> future = new OrderedFuture<>();
        Runnable task = new TransferLeadershipTask(this, endpoint, future);
        return executeIfRunning(task, future);
    }

    @Nonnull
    @Override
    public CompletableFuture<Ordered<RaftNodeReport>> getReport() {
        OrderedFuture<RaftNodeReport> future = new OrderedFuture<>();
        return executeIfRunning(() -> {
            try {
                future.complete(state.commitIndex(), newReport(RaftNodeReportReason.API_CALL));
            } catch (Throwable t) {
                future.fail(t);
            }
        }, future);
    }

    @Nonnull
    @Override
    public CompletableFuture<Ordered<RaftNodeReport>> takeSnapshot() {
        OrderedFuture<RaftNodeReport> future = new OrderedFuture<>();
        return executeIfRunning(() -> {
            try {
                if (status == INITIAL || isTerminal(status)) {
                    future.fail(newNotRunningException());
                    return;
                }
                if (state.commitIndex() < FIRST_VALID_LOG_INDEX) {
                    future.fail(new IllegalStateException(
                            localEndpointStr + " cannot take a snapshot before committing a log entry!"));
                }
                applyLogEntries();
                if (isTerminal(status)) {
                    LOGGER.warn("{} cannot take snapshot since it is {}", localEndpointStr, status);
                    future.fail(newNotRunningException());
                    return;
                }

                RaftNodeReport report = null;
                if (state.log().snapshotIndex() < state.lastApplied()) {
                    takeSnapshot(state.log(), state.lastApplied());
                    report = newReport(RaftNodeReportReason.TAKE_SNAPSHOT);
                    LOGGER.info("{} took a snapshot via manual trigger at log index: {}", localEndpointStr,
                            state.lastApplied());
                }
                future.complete(state.lastApplied(), report);
            } catch (Throwable t) {
                future.fail(t);
            }
        }, future);
    }

    private RaftNodeReportImpl newReport(RaftNodeReportReason reason) {
        Map<RaftEndpoint, Long> heartbeatTimestamps = state.leaderState() != null
                ? state.leaderState().responseTimestamps()
                : Collections.emptyMap();
        Optional<Long> quorumTimestamp = getQuorumHeartbeatTimestamp();
        // non-empty if this node is not leader and received at least one heartbeat from
        // the leader.
        Optional<Long> leaderHeartbeatTimestamp = (quorumTimestamp.isEmpty() && this.lastLeaderHeartbeatTimestamp > 0)
                ? Optional.of(Math.min(this.lastLeaderHeartbeatTimestamp, clock.millis()))
                : Optional.empty();

        return new RaftNodeReportImpl(requireNonNull(reason), groupId, state.localEndpoint(), state.initialMembers(),
                state.committedGroupMembers(), state.effectiveGroupMembers(), state.role(), status, state.termState(),
                newLogReport(), heartbeatTimestamps, quorumTimestamp, leaderHeartbeatTimestamp);
    }

    private RaftLogStatsImpl newLogReport() {
        LeaderState leaderState = state.leaderState();
        Map<RaftEndpoint, Long> followerMatchIndices;
        if (leaderState != null) {
            followerMatchIndices = leaderState.getFollowerStates().entrySet().stream()
                    .collect(toMap(Entry::getKey, e -> e.getValue().matchIndex()));
        } else {
            followerMatchIndices = emptyMap();
        }

        return new RaftLogStatsImpl(state.commitIndex(), state.log().lastLogOrSnapshotEntry(),
                state.log().snapshotEntry(), takeSnapshotCount, installSnapshotCount, followerMatchIndices);
    }

    private <T> OrderedFuture<T> executeIfRunning(Runnable task, OrderedFuture<T> future) {
        if (!isTerminal(status)) {
            executor.execute(task);
        } else {
            future.fail(newNotRunningException());
        }

        return future;
    }

    public RaftException newNotLeaderException() {
        return new NotLeaderException(getLocalEndpoint(), isTerminal(status) ? null : getLeaderEndpoint());
    }

    private RuntimeException newNotRunningException() {
        return new IllegalStateException(localEndpointStr + " is not running!");
    }

    public RaftException newLaggingCommitIndexException(long minCommitIndex) {
        assert minCommitIndex > state.commitIndex()
                : "Cannot create LaggingCommitIndexException since min commit index: " + minCommitIndex
                        + " is not greater than commit index: " + state.commitIndex();
        return new LaggingCommitIndexException(state.commitIndex(), minCommitIndex, state.leader());
    }

    /**
     * Returns the leader Raft endpoint currently known by the local Raft node. The
     * returned leader information might be stale.
     *
     * @return the leader Raft endpoint currently known by the local Raft node
     */
    @Nullable
    public RaftEndpoint getLeaderEndpoint() {
        // volatile read
        return state.leader();
    }

    /**
     * Schedules a task to reset append entries request backoff periods, if not
     * scheduled already.
     *
     * @param leaderState
     *            the leader state to check and set the backoff task scheduling
     *            state.
     */
    public void scheduleLeaderRequestBackoffResetTask(LeaderState leaderState) {
        if (!leaderState.isRequestBackoffResetTaskScheduled()) {
            executor.schedule(leaderBackoffResetTask, LEADER_BACKOFF_RESET_TASK_PERIOD_MILLIS, MILLISECONDS);
            leaderState.requestBackoffResetTaskScheduled(true);
        }
    }

    public RaftNodeExecutor getExecutor() {
        return executor;
    }

    /**
     * Applies the committed log entries between {@code lastApplied} and
     * {@code commitIndex}, if there's any available. If new entries are applied,
     * {@link RaftState}'s {@code lastApplied} field is also updated.
     *
     * @see RaftState#lastApplied()
     * @see RaftState#commitIndex()
     */
    public void applyLogEntries() {
        assert state.commitIndex() >= state.lastApplied() : localEndpointStr + " commit index: " + state.commitIndex()
                + " cannot be smaller than last applied: " + state.lastApplied();

        assert state.role() == LEADER || state.role() == FOLLOWER || state.role() == LEARNER
                : localEndpointStr + " trying to apply log entries in role: " + state.role();

        // Apply all committed but not-yet-applied log entries
        RaftLog log = state.log();

        while (state.lastApplied() < state.commitIndex()) {
            for (long logIndex = state.lastApplied() + 1,
                    nextSnapshotIndex = log.snapshotIndex() - (log.snapshotIndex() % commitCountToTakeSnapshot)
                            + commitCountToTakeSnapshot,
                    applyUntil = min(state.commitIndex(), nextSnapshotIndex); logIndex <= applyUntil; logIndex++) {
                LogEntry entry = log.getLogEntry(logIndex);
                if (entry == null) {
                    String msg = localEndpointStr + " failed to get log entry at index: " + logIndex;
                    LOGGER.error(msg);
                    throw new AssertionError(msg);
                }

                applyLogEntry(entry);
            }

            if (state.lastApplied() % commitCountToTakeSnapshot == 0 && !isTerminal(status)) {
                // If the status is terminal, then there will be no new append or commit.
                takeSnapshot(log, state.lastApplied());
            }
        }

        assert (status != TERMINATED || state.commitIndex() == log.lastLogOrSnapshotIndex())
                : localEndpointStr + " commit index: " + state.commitIndex() + " must be equal to "
                        + log.lastLogOrSnapshotIndex() + " on termination.";
    }

    /**
     * Applies the log entry by executing its operation and sets execution result to
     * the related future if any available.
     */
    private void applyLogEntry(LogEntry entry) {
        LOGGER.debug("{} Processing {}", localEndpointStr, entry);

        long logIndex = entry.getIndex();
        Object operation = entry.getOperation();
        Object response;

        final UpdateRaftGroupMembersOp groupOp;

        if (operation instanceof BatchOperation) {
            groupOp = ((BatchOperation) operation).getGroupOpToApply().orElse(null);
        } else if (operation instanceof RaftGroupOp) {
            if (operation instanceof UpdateRaftGroupMembersOp) {
                groupOp = (UpdateRaftGroupMembersOp) operation;
            } else {
                throw new IllegalArgumentException("Invalid Raft group operation: " + operation);
            }
        } else {
            groupOp = null;
        }

        Supplier<Object> applyGroupOp = () -> {
            assert groupOp != null;

            if (state.effectiveGroupMembers().getLogIndex() < logIndex) {
                setStatus(UPDATING_RAFT_GROUP_MEMBER_LIST);
                updateGroupMembers(logIndex, groupOp.getMembers(), groupOp.getVotingMembers());
            }

            // TODO(szymon): Why is this enforced?
            // TODO(szymon): Don't we crash without a proper response if we get a group op
            // in stable state (state == ACTIVE) with a lower log index.
            assert status == UPDATING_RAFT_GROUP_MEMBER_LIST : localEndpointStr + " STATUS: " + status;
            assert state.effectiveGroupMembers().getLogIndex() == logIndex
                    : localEndpointStr + " effective group members log index: "
                            + state.effectiveGroupMembers().getLogIndex() + " applied log index: " + logIndex;

            state.commitGroupMembers();

            if (groupOp.getEndpoint().equals(getLocalEndpoint())
                    && groupOp.getMode() == MembershipChangeMode.REMOVE_MEMBER) {
                setStatus(TERMINATED);
            } else {
                setStatus(ACTIVE);
            }

            if (stateMachine instanceof InternalCommitAware) {
                ((InternalCommitAware) stateMachine).onInternalCommit(logIndex);
            }

            return state.committedGroupMembers();
        };

        try {
            if (operation instanceof BatchOperation) {
                var stateMachineOperations = ((BatchOperation) operation).getStateMachineOperations();

                List<Object> responsesFromStateMachine;
                if (!stateMachineOperations.isEmpty()) {
                    responsesFromStateMachine = stateMachine.runBatch(logIndex, stateMachineOperations);
                    assert responsesFromStateMachine.size() == stateMachineOperations.size();
                } else {
                    responsesFromStateMachine = List.of();
                }

                var responses = new LinkedList<>();
                for (var op : ((BatchOperation) operation).getOperations()) {
                    if (op instanceof RaftGroupOp) {
                        responses.addLast(op == groupOp ? applyGroupOp.get() : null);
                    } else {
                        responses.addLast(responsesFromStateMachine.remove(0));
                    }
                }

                response = responses;
            } else {
                response = operation == groupOp ? applyGroupOp.get() : stateMachine.runOperation(logIndex, operation);
            }
        } catch (Exception t) {
            LOGGER.error("{} failed.",
                    localEndpointStr + " execution of " + operation + " at commit index: " + logIndex, t);
            response = t;
        }

        state.lastApplied(logIndex);
        state.completeFuture(logIndex, response);
    }

    /**
     * Updates the last leader heartbeat timestamp to now
     */
    public void leaderHeartbeatReceived() {
        lastLeaderHeartbeatTimestamp = Math.max(lastLeaderHeartbeatTimestamp, clock.millis());
    }

    /**
     * Returns the internal Raft state
     *
     * @return the internal Raft state
     */
    public RaftState state() {
        return state;
    }

    private void takeSnapshot(RaftLog log, long snapshotIndex) {
        if (snapshotIndex == log.snapshotIndex()) {
            LOGGER.warn("{} is skipping to take snapshot at index: {} because it is the latest snapshot index.",
                    localEndpointStr, snapshotIndex);
            return;
        }

        LOGGER.debug("{} is taking snapshot at index: {}", localEndpointStr, snapshotIndex);
        List<Object> chunkObjects = new ArrayList<>();
        try {
            stateMachine.takeSnapshot(snapshotIndex, chunkObjects::add);
        } catch (Throwable t) {
            throw new RaftException(localEndpointStr + " Could not take snapshot at applied index: " + snapshotIndex,
                    state.leader(), t);
        }

        ++takeSnapshotCount;

        int snapshotTerm = log.getLogEntry(snapshotIndex).getTerm();
        RaftGroupMembersView groupMembersView = state.committedGroupMembers()
                .populate(modelFactory.createRaftGroupMembersViewBuilder());
        List<SnapshotChunk> snapshotChunks = new ArrayList<>();
        for (int chunkIndex = 0, chunkCount = chunkObjects.size(); chunkIndex < chunkCount; chunkIndex++) {
            SnapshotChunk snapshotChunk = modelFactory.createSnapshotChunkBuilder().setTerm(snapshotTerm)
                    .setIndex(snapshotIndex).setOperation(chunkObjects.get(chunkIndex))
                    .setSnapshotChunkIndex(chunkIndex).setSnapshotChunkCount(chunkCount)
                    .setGroupMembersView(groupMembersView).build();

            snapshotChunks.add(snapshotChunk);

            try {
                store.persistSnapshotChunk(snapshotChunk);
            } catch (IOException e) {
                throw new RaftException(
                        "Persist failed at snapshot index: " + snapshotIndex + ", chunk index: " + chunkIndex,
                        getLeaderEndpoint(), e);
            }
        }

        try {
            store.flush();
        } catch (IOException e) {
            throw new RaftException("Flush failed at snapshot index: " + snapshotIndex, getLeaderEndpoint(), e);
        }

        // we flushed the snapshot to the storage.
        // it is safe to modify the memory state now.

        SnapshotEntry snapshotEntry = modelFactory.createSnapshotEntryBuilder().setTerm(snapshotTerm)
                .setIndex(snapshotIndex).setSnapshotChunks(snapshotChunks).setGroupMembersView(groupMembersView)
                .build();

        long highestLogIndexToTruncate = findHighestLogIndexToTruncateUntilSnapshotIndex(snapshotIndex);
        // the following call will also modify the persistent state
        // to truncate stale log entries. we will schedule an async flush
        // task below.
        int truncatedEntryCount = log.setSnapshot(snapshotEntry, highestLogIndexToTruncate);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(localEndpointStr + " " + snapshotEntry + " is taken. " + truncatedEntryCount + " entries are "
                    + "truncated.");
        } else {
            LOGGER.info("{} took snapshot at term: {} and log index: {} and truncated {} entries.", localEndpointStr,
                    snapshotEntry.getTerm(), snapshotEntry.getIndex(), truncatedEntryCount);
        }

        publishRaftNodeReport(RaftNodeReportReason.TAKE_SNAPSHOT);

        // this will flush the truncation of the stale log entries
        // asynchronously. if this node is the leader, it can append new log
        // entries in the meantime, this task will flush them to the storage.
        executor.submit(new FlushTask(this));
    }

    private long findHighestLogIndexToTruncateUntilSnapshotIndex(long snapshotIndex) {
        long limit = Math.max(FIRST_VALID_LOG_INDEX, snapshotIndex - maxLogEntryCountToKeepAfterSnapshot);
        long truncationIndex = limit;
        LeaderState leaderState = state.leaderState();
        if (leaderState != null) {
            long[] matchIndices = leaderState.matchIndices(state.remoteVotingMembers());
            // Last slot is reserved for the leader and always zero.
            // If there is at least one follower with unknown match index,
            // its log can be close to the leader's log so we are keeping the old log
            // entries.
            boolean allMatchIndicesKnown = Arrays.stream(matchIndices, 0, matchIndices.length - 1)
                    .noneMatch(i -> i == 0);

            if (allMatchIndicesKnown) {
                // Otherwise, we will keep the log entries until the minimum match index
                // that is bigger than (commitIndex - maxNumberOfLogsToKeepAfterSnapshot).
                // If there is no such follower (all of the minority followers are far behind),
                // then there is no need to keep the old log entries.
                truncationIndex = Arrays.stream(matchIndices)
                        // No need to keep any log entry if all followers are up to date
                        .filter(i -> i < snapshotIndex).filter(i -> i > limit)
                        // We should not delete the smallest matchIndex
                        .map(i -> i - 1).sorted().findFirst().orElse(snapshotIndex);
            }
        }

        return truncationIndex;
    }

    /**
     * Installs the snapshot sent by the leader if it's not already installed. This
     * method assumes that the given snapshot is already persisted and flushed to
     * the storage.
     *
     * @param snapshotEntry
     *            the snapshot entry object to install to the local Raft node
     */
    public void installSnapshot(SnapshotEntry snapshotEntry) {
        long commitIndex = state.commitIndex();

        if (commitIndex >= snapshotEntry.getIndex()) {
            throw new IllegalArgumentException("Cannot install snapshot at index: " + snapshotEntry.getIndex()
                    + " because the current commit index is: " + commitIndex);
        }

        RaftLog log = state.log();
        int truncated = log.setSnapshot(snapshotEntry);

        // local state is updated here after log.setSnapshot() because
        // the storage might fail.
        state.commitIndex(snapshotEntry.getIndex());
        state.snapshotChunkCollector(null);

        if (truncated > 0) {
            LOGGER.info("{} {} entries are truncated to install snapshot at commit index: {}", localEndpointStr,
                    truncated, snapshotEntry.getIndex());
        }

        List<Object> chunkOperations = ((List<SnapshotChunk>) snapshotEntry.getOperation()).stream()
                .map(SnapshotChunk::getOperation).collect(toList());
        stateMachine.installSnapshot(snapshotEntry.getIndex(), chunkOperations);

        ++installSnapshotCount;
        publishRaftNodeReport(RaftNodeReportReason.INSTALL_SNAPSHOT);

        // If I am installing a snapshot, it means I am still present
        // in the last member list, but it is possible that the last entry
        // I appended before the snapshot could be a membership change.
        // Because of this, I need to update my status. Nevertheless, I may
        // not be present in the restored member list, which is ok.

        setStatus(ACTIVE);
        if (state.installGroupMembers(snapshotEntry.getGroupMembersView())) {
            publishRaftNodeReport(RaftNodeReportReason.GROUP_MEMBERS_CHANGE);
        }

        state.lastApplied(snapshotEntry.getIndex());
        LOGGER.info("{} snapshot is installed at commit index: {}", localEndpointStr, snapshotEntry.getIndex());

        state.invalidateFuturesUntil(snapshotEntry.getIndex(), new IndeterminateStateException(state.leader()));
        tryRunScheduledQueries();

        // log.setSnapshot() truncates stale log entries from disk.
        // we are submitting an async flush task here to flush those
        // changes to the storage.
        executor.submit(new FlushTask(this));
    }

    /**
     * Updates Raft group members.
     *
     * @param logIndex
     *            the log index on which the given Raft group members are appended
     * @param members
     *            the list of all Raft endpoints in the Raft group
     * @param votingMembers
     *            the list of voting Raft endpoints in the Raft group (must be a
     *            subset of the "members" parameter)
     *
     * @see RaftState#updateGroupMembers(long, Collection, Collection, long)
     */
    public void updateGroupMembers(long logIndex, Collection<RaftEndpoint> members,
            Collection<RaftEndpoint> votingMembers) {
        state.updateGroupMembers(logIndex, members, votingMembers, clock.millis());
        publishRaftNodeReport(RaftNodeReportReason.GROUP_MEMBERS_CHANGE);
    }

    /**
     * Reverts the Raft group members back to the committed Raft group members.
     *
     * @see RaftState#revertGroupMembers()
     */
    public void revertGroupMembers() {
        state.revertGroupMembers();
        publishRaftNodeReport(RaftNodeReportReason.GROUP_MEMBERS_CHANGE);
    }

    /**
     * Publishes a new Raft node report for the given reason
     *
     * @param reason
     *            the underlying reason that triggers this RaftNodeReport publish
     *
     * @see RaftNodeReport
     */
    public void publishRaftNodeReport(RaftNodeReportReason reason) {
        RaftNodeReportImpl report = newReport(reason);
        if ((reason == RaftNodeReportReason.STATUS_CHANGE || reason == RaftNodeReportReason.ROLE_CHANGE
                || reason == RaftNodeReportReason.GROUP_MEMBERS_CHANGE)) {
            Object groupId = state.groupId();
            RaftGroupMembers members = report.getEffectiveMembers();
            StringBuilder sb = new StringBuilder(localEndpointStr).append(" lastLogIndex: ")
                    .append(report.getLog().getLastLogOrSnapshotIndex()).append(", commitIndex: ")
                    .append(report.getLog().getCommitIndex()).append(", snapshotIndex: ")
                    .append(report.getLog().getLastSnapshotIndex()).append(", Raft Group Members {").append("groupId: ")
                    .append(groupId).append(", size: ").append(members.getMembers().size()).append(", term: ")
                    .append(report.getTerm().getTerm()).append(", logIndex: ").append(members.getLogIndex())
                    .append("} [");

            members.getMembers().forEach(member -> {
                sb.append("\n\t").append(member.getId());
                if (getLocalEndpoint().equals(member)) {
                    sb.append(" - ").append(state.role()).append(" this (").append(status).append(")");
                } else if (member.equals(state.leader())) {
                    sb.append(" - ").append(LEADER);
                }
            });
            sb.append("\n] reason: ").append(reason).append("\n");
            LOGGER.info(sb.toString());
        }

        try {
            raftNodeReportListener.accept(report);
        } catch (Throwable t) {
            LOGGER.error(localEndpointStr + "'s listener: " + raftNodeReportListener + " failed for " + report, t);
        }
    }

    /**
     * Updates the known leader endpoint.
     *
     * @param member
     *            the discovered leader endpoint
     */
    public void leader(RaftEndpoint member) {
        state.leader(member);
        publishRaftNodeReport(RaftNodeReportReason.ROLE_CHANGE);
    }

    /**
     * Switches this Raft node to the leader role by performing the following steps:
     * <ul>
     * <li>Setting the local endpoint as the current leader,</li>
     * <li>Clearing (pre)candidate states,</li>
     * <li>Initializing the leader state for the current members,</li>
     * <li>Appending an operation to the Raft log if enabled.</li>
     * </ul>
     */
    public void toLeader() {
        state.toLeader(clock.millis());
        appendNewTermEntry();
        broadcastAppendEntriesRequest();
        publishRaftNodeReport(RaftNodeReportReason.ROLE_CHANGE);
    }

    /**
     * Broadcasts append entries requests to all group members according to their
     * nextIndex parameters.
     */
    public void broadcastAppendEntriesRequest() {
        for (RaftEndpoint follower : state.remoteMembers()) {
            sendAppendEntriesRequest(follower);
        }
    }

    /**
     * Sends an append entries request to the given follower.
     * <p>
     * Log entries between follower's known nextIndex and the latest appended entry
     * index are sent as a batch, whose size can be at most
     * {@link RaftConfig#getAppendEntriesRequestBatchSize()}.
     * <p>
     * If the given follower's nextIndex is behind the latest snapshot index, then
     * an {@link InstallSnapshotRequest} is sent.
     * <p>
     * If the leader doesn't know the given follower's matchIndex (i.e., its
     * {@code matchIndex == 0}), then an empty append entries request is sent to
     * save bandwidth until the leader discovers the real matchIndex of the
     * follower.
     *
     * @param target
     *            the Raft endpoint to send the request
     */
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength",})
    public void sendAppendEntriesRequest(RaftEndpoint target) {
        RaftLog log = state.log();
        LeaderState leaderState = state.leaderState();
        FollowerState followerState = leaderState.getFollowerStateOrNull(target);

        if (followerState == null) {
            LOGGER.warn("{} follower/learner: {} not found to send append entries request.", localEndpointStr,
                    target.getId());
            return;
        } else if (followerState.isRequestBackoffSet()) {
            // The target still has not sent a response for the last append request.
            // We will send a new append request either when the follower sends a response
            // or a back-off timeout occurs.
            return;
        }

        long nextIndex = followerState.nextIndex();
        // we never send query sequencer number to learners
        // since they are excluded from the replication quorum.
        long querySequenceNumber = leaderState.querySequenceNumber(state.isVotingMember(target));

        // if the first log entry to be sent is put into the snapshot, check if we still
        // keep it in the log
        // if we still keep that log entry and its previous entry, we don't need to send
        // a snapshot
        if (nextIndex <= log.snapshotIndex()
                && (!log.containsLogEntry(nextIndex) || (nextIndex > 1 && !log.containsLogEntry(nextIndex - 1)))) {
            // We send an empty request to notify the target so that it could
            // trigger the actual snapshot installation process...
            SnapshotEntry snapshotEntry = log.snapshotEntry();
            List<RaftEndpoint> snapshottedMembers = getSnapshottedMembers(leaderState, snapshotEntry);
            RaftMessage request = modelFactory.createInstallSnapshotRequestBuilder().setGroupId(getGroupId())
                    .setSender(getLocalEndpoint()).setTerm(state.term()).setSenderLeader(true)
                    .setSnapshotTerm(snapshotEntry.getTerm()).setSnapshotIndex(snapshotEntry.getIndex())
                    .setTotalSnapshotChunkCount(snapshotEntry.getSnapshotChunkCount()).setSnapshotChunk(null)
                    .setSnapshottedMembers(snapshottedMembers).setGroupMembersView(snapshotEntry.getGroupMembersView())
                    .setQuerySequenceNumber(querySequenceNumber)
                    .setFlowControlSequenceNumber(enableBackoff(followerState)).build();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(localEndpointStr + " Sending " + request + " to " + target.getId() + " since next index: "
                        + nextIndex + " <= snapshot index: " + log.snapshotIndex());
            }

            send(target, request);
            scheduleLeaderRequestBackoffResetTask(leaderState);

            return;
        }

        AppendEntriesRequestBuilder requestBuilder = modelFactory.createAppendEntriesRequestBuilder()
                .setGroupId(getGroupId()).setSender(getLocalEndpoint()).setTerm(state.term())
                .setCommitIndex(state.commitIndex()).setQuerySequenceNumber(querySequenceNumber);
        List<LogEntry> entries;
        boolean backoff = true;
        long lastLogIndex = log.lastLogOrSnapshotIndex();
        if (nextIndex > 1) {
            long prevEntryIndex = nextIndex - 1;
            requestBuilder.setPreviousLogIndex(prevEntryIndex);
            BaseLogEntry prevEntry = (log.snapshotIndex() == prevEntryIndex)
                    ? log.snapshotEntry()
                    : log.getLogEntry(prevEntryIndex);
            assert prevEntry != null
                    : localEndpointStr + " prev entry index: " + prevEntryIndex + ", snapshot: " + log.snapshotIndex();
            requestBuilder.setPreviousLogTerm(prevEntry.getTerm());

            long matchIndex = followerState.matchIndex();
            if (matchIndex == 0) {
                // Until the leader has discovered where it and the target's logs match,
                // the leader can send AppendEntries with no entries (like heartbeats) to save
                // bandwidth.
                // We still need to enable append request backoff here because we do not want to
                // bombard
                // the follower before we learn its match index.
                entries = emptyList();
            } else if (nextIndex <= lastLogIndex) {
                // Then, once the matchIndex immediately precedes the nextIndex,
                // the leader should begin to send the actual entries.
                entries = log.getLogEntriesBetween(nextIndex,
                        min(nextIndex + appendEntriesRequestBatchSize, lastLogIndex));
            } else {
                // The target has caught up with the leader. Sending an empty append request as
                // a heartbeat...
                entries = emptyList();
                // amortize the cost of multiple queries.
                backoff = leaderState.queryState().queryCount() > 0;
            }
        } else if (nextIndex == 1 && lastLogIndex > 0) {
            // Entries will be sent to the target for the first time...
            entries = log.getLogEntriesBetween(nextIndex, min(nextIndex + appendEntriesRequestBatchSize, lastLogIndex));
        } else {
            // There is no entry in the Raft log. Sending an empty append request as a
            // heartbeat...
            entries = emptyList();
            // amortize cost of multiple queries.
            backoff = leaderState.queryState().queryCount() > 0;
        }

        if (backoff) {
            requestBuilder.setFlowControlSequenceNumber(enableBackoff(followerState));
        }

        RaftMessage request = requestBuilder.setLogEntries(entries).build();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(localEndpointStr + " Sending " + request + " to " + target.getId() + " with next index: "
                    + nextIndex);
        }

        send(target, request);

        if (backoff) {
            scheduleLeaderRequestBackoffResetTask(leaderState);
        }

        if (entries.size() > 0 && entries.get(entries.size() - 1).getIndex() > leaderState.flushedLogIndex()) {
            // TODO(basri): we can skip this if the target is a learner...

            // If I am sending any non-flushed entry to the target, I should
            // trigger the flush task. I hope that I will flush before
            // receiving append entries responses from half of the followers...
            // This is a very critical optimization because it makes the leader
            // and followers flush in parallel...
            submitLeaderFlushTask(leaderState);
        }
    }

    private List<RaftEndpoint> getSnapshottedMembers(LeaderState leaderState, SnapshotEntry snapshotEntry) {
        if (!config.isTransferSnapshotsFromFollowersEnabled()) {
            return List.of(state.localEndpoint());
        }

        long now = clock.millis();
        List<RaftEndpoint> snapshottedMembers = new ArrayList<>();
        snapshottedMembers.add(state.localEndpoint());
        for (Entry<RaftEndpoint, FollowerState> e : leaderState.getFollowerStates().entrySet()) {
            RaftEndpoint follower = e.getKey();
            FollowerState followerState = e.getValue();
            if (followerState.matchIndex() > snapshotEntry.getIndex() && transport.isReachable(follower)
                    && !isLeaderHeartbeatTimeoutElapsed(followerState.responseTimestamp(), now)) {
                snapshottedMembers.add(follower);
            }
        }

        return snapshottedMembers;
    }

    private long enableBackoff(FollowerState followerState) {
        return followerState.setRequestBackoff(MIN_BACKOFF_ROUNDS, maxBackoffRounds);
    }

    /**
     * Sends the given Raft message to the given Raft endpoint.
     *
     * @param target
     *            the target Raft endpoint to send the given Raft message
     * @param message
     *            the Raft message to send
     */
    public void send(RaftEndpoint target, RaftMessage message) {
        try {
            transport.send(target, message);
        } catch (Throwable t) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error("Could not send " + message + " to " + target, t);
            } else {
                LOGGER.error("Could not send " + message.getClass().getSimpleName() + " to " + target, t);
            }
        }
    }

    /**
     * Returns true if the leader flush task is submitted either by the current call
     * or a previous call of this method. Returns false if the leader flush task is
     * not submitted, because this Raft node is created with {@link NopRaftStore}.
     *
     * @param leaderState
     *            the leader state to set the flush task scheduling state
     *
     * @return true if the leader flush task is submitted either by the current call
     *         or a previous call of this method
     */
    public boolean submitLeaderFlushTask(LeaderState leaderState) {
        if (leaderFlushTask == null) {
            return false;
        }

        if (!leaderState.isFlushTaskSubmitted()) {
            executor.submit(leaderFlushTask);
            leaderState.flushTaskSubmitted(true);
        }

        return true;
    }

    private void appendNewTermEntry() {
        Object operation = stateMachine.getNewTermOperation();
        if (operation != null) {
            // this null check is on purpose. this operation can be null in tests.
            RaftLog log = state.log();
            LogEntry entry = modelFactory.createLogEntryBuilder().setTerm(state.term())
                    .setIndex(log.lastLogOrSnapshotIndex() + 1).setOperation(operation).build();
            log.appendEntry(entry);
        }
    }

    /**
     * Switches this Raft node to the candidate role for the next term and starts a
     * new leader election round. Regular leader elections are sticky, meaning that
     * leader stickiness will be considered by other Raft nodes when they receive
     * vote requests. A non-sticky leader election occurs when the current Raft
     * group leader tries to transfer leadership to another member.
     *
     * @param sticky
     *            the parameter to pass on to the vote requests which are going to
     *            be sent to the followers.
     */
    public void toCandidate(boolean sticky) {
        state.toCandidate();
        BaseLogEntry lastLogEntry = state.log().lastLogOrSnapshotEntry();

        LOGGER.info("{} Leader election started for term: {}, last log index: {}, last log term: {}", localEndpointStr,
                state.term(), lastLogEntry.getIndex(), lastLogEntry.getTerm());

        publishRaftNodeReport(RaftNodeReportReason.ROLE_CHANGE);

        RaftMessage request = modelFactory.createVoteRequestBuilder().setGroupId(getGroupId())
                .setSender(getLocalEndpoint()).setTerm(state.term()).setLastLogTerm(lastLogEntry.getTerm())
                .setLastLogIndex(lastLogEntry.getIndex()).setSticky(sticky).build();

        for (RaftEndpoint member : state.remoteVotingMembers()) {
            send(member, request);
        }

        executor.schedule(new LeaderElectionTimeoutTask(this), getLeaderElectionTimeoutMs(), MILLISECONDS);
    }

    /**
     * Returns the leader election timeout with a small and randomised extension.
     *
     * @return the leader election timeout with a small and randomised extension.
     *
     * @see RaftConfig#getLeaderElectionTimeoutMillis()
     */
    public long getLeaderElectionTimeoutMs() {
        return (((int) config.getLeaderElectionTimeoutMillis()) + random.nextInt(LEADER_ELECTION_TIMEOUT_NOISE_MILLIS));
    }

    /**
     * Initiates the pre-voting step for the next term. The pre-voting step is
     * executed to check if other group members would vote for this Raft node if it
     * would start a new leader election.
     */
    public void preCandidate() {
        state.initPreCandidateState();
        int nextTerm = state.term() + 1;
        BaseLogEntry entry = state.log().lastLogOrSnapshotEntry();

        RaftMessage request = modelFactory.createPreVoteRequestBuilder().setGroupId(getGroupId())
                .setSender(getLocalEndpoint()).setTerm(nextTerm).setLastLogTerm(entry.getTerm())
                .setLastLogIndex(entry.getIndex()).build();

        LOGGER.info("{} Pre-vote started for next term: {}, last log index: {}, last log term: {}", localEndpointStr,
                nextTerm, entry.getIndex(), entry.getTerm());

        for (RaftEndpoint member : state.remoteVotingMembers()) {
            send(member, request);
        }

        executor.schedule(new PreVoteTimeoutTask(this, state.term()), getLeaderElectionTimeoutMs(), MILLISECONDS);
    }

    public RaftException newCannotReplicateException() {
        return new CannotReplicateException(isTerminal(status) ? null : getLeaderEndpoint());
    }

    private long findQuorumMatchIndex() {
        LeaderState leaderState = state.leaderState();
        long[] indices = leaderState.matchIndices(state.remoteVotingMembers());

        // if the leader is leaving, it should not count its vote for quorum...
        if (state.isKnownMember(getLocalEndpoint())) {
            // Raft dissertation Section 10.2.1:
            // The leader may even commit an entry before it has been written to its own
            // disk,
            // if a majority of followers have written it to their disks; this is still
            // safe.
            long leaderLogIndex = leaderFlushTask == null
                    ? state.log().lastLogOrSnapshotIndex()
                    : leaderState.flushedLogIndex();
            indices[indices.length - 1] = leaderLogIndex;
        } else {
            // Remove the last empty slot reserved for leader index
            indices = Arrays.copyOf(indices, indices.length - 1);
        }

        sort(indices);

        // 4 nodes: [0, 1, 2, 3] => Qlr = 2, quorum index = 2
        // 5 nodes: [0, 1, 2, 3, 4] => Qlr = 3, quorum index = 2

        long quorumMatchIndex = indices[state.votingMemberCount() - state.logReplicationQuorumSize()];
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(localEndpointStr + " Quorum match index: " + quorumMatchIndex + ", indices: "
                    + Arrays.toString(indices));
        }

        return quorumMatchIndex;
    }

    public boolean tryAdvanceCommitIndex() {
        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥
        // N, and log[N].term ==
        // currentTerm:
        // set commitIndex = N (§5.3, §5.4)
        long quorumMatchIndex = findQuorumMatchIndex();
        long commitIndex = state.commitIndex();
        RaftLog log = state.log();
        for (; quorumMatchIndex > commitIndex; quorumMatchIndex--) {
            // Only log entries from the leader’s current term are committed by counting
            // replicas; once an entry
            // from the current term has been committed in this way, then all prior entries
            // are committed indirectly
            // because of the Log Matching Property.
            LogEntry entry = log.getLogEntry(quorumMatchIndex);
            if (entry.getTerm() == state.term()) {
                commitEntries(quorumMatchIndex);
                return true;
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(localEndpointStr + " cannot commit " + entry + " since an entry from the current term: "
                        + state.term() + " is needed.");
            }
        }
        return false;
    }

    private void commitEntries(long commitIndex) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(localEndpointStr + " Setting commit index: " + commitIndex);
        }

        state.commitIndex(commitIndex);
        applyLogEntries();
        // the leader might have left the Raft group, but still we can send
        // an append request at this point
        broadcastAppendEntriesRequest();
        if (status != TERMINATED) {
            // the leader is still part of the Raft group
            tryRunQueries();
            tryRunScheduledQueries();
        } else {
            // the leader has left the Raft group
            state.invalidateScheduledQueries();
            toFollower(state.term());
            terminateComponents();
        }
    }

    public void tryAckQuery(long querySequenceNumber, RaftEndpoint sender) {
        LeaderState leaderState = state.leaderState();
        if (leaderState == null) {
            return;
        } else if (!state.isVotingMember(sender)) {
            return;
        }

        QueryState queryState = leaderState.queryState();
        if (queryState.tryAck(querySequenceNumber, sender)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(localEndpointStr() + " ack from " + sender.getId() + " for query sequence number: "
                        + querySequenceNumber);
            }

            tryRunQueries();
        }
    }

    /**
     * Returns a short string that represents identity of the local Raft endpoint.
     *
     * @return a short string that represents identity of the local Raft endpoint
     */
    public String localEndpointStr() {
        return localEndpointStr;
    }

    public void tryRunQueries() {
        LeaderState leaderState = state.leaderState();
        if (leaderState == null) {
            return;
        }

        QueryState queryState = leaderState.queryState();
        long commitIndex = state.commitIndex();
        if (!queryState.isQuorumAckReceived(commitIndex, state.logReplicationQuorumSize())) {
            return;
        }

        Collection<QueryContainer> operations = queryState.queries();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(localEndpointStr + " running " + operations.size() + " queries at commit index: " + commitIndex
                    + ", query sequence number: " + queryState.querySequenceNumber());
        }

        for (QueryContainer query : operations) {
            query.run(commitIndex, stateMachine);
        }

        queryState.reset();
    }

    public void tryRunScheduledQueries() {
        long lastApplied = state.lastApplied();
        Collection<QueryContainer> queries = state.collectScheduledQueriesToExecute();
        for (QueryContainer query : queries) {
            query.run(lastApplied, stateMachine);
        }

        if (queries.size() > 0 && LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} executed {} waiting queries at log index: {}.", localEndpointStr, queries.size(),
                    lastApplied);
        }
    }

    /**
     * Executes the given query operation and sets execution result to the future if
     * the current commit index is greater than or equal to the given commit index.
     * <p>
     * Please note that the given operation must not make any mutation on the state
     * machine.
     *
     * @param operation
     *            the query object to be executed
     * @param minCommitIndex
     *            the minimum commit index that the local Raft node must satisfy
     * @param future
     *            the future object to notify with the result
     *
     * @throws LaggingCommitIndexException
     *             if the current commit index is smaller than the given commit
     *             index
     */
    public void runOrScheduleQuery(QueryContainer query, long minCommitIndex, Optional<Duration> timeout) {
        try {
            long lastApplied = state.lastApplied();
            if (lastApplied >= minCommitIndex) {
                query.run(lastApplied, stateMachine);
            } else if (timeout.isPresent()) {
                long timeoutNanos = timeout.get().toNanos();
                if (timeoutNanos <= 0) {
                    query.fail(newLaggingCommitIndexException(minCommitIndex));
                } else {
                    state.addScheduledQuery(minCommitIndex, query);
                    executor.schedule(() -> {
                        try {
                            if (state.removeScheduledQuery(minCommitIndex, query)) {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug(
                                            "{} query waiting to be executed at commit index: {} timed out! Current commit index: {}",
                                            localEndpointStr, minCommitIndex, state.commitIndex());
                                }
                                query.fail(newLaggingCommitIndexException(minCommitIndex));
                            }
                        } catch (Throwable t) {
                            LOGGER.error(localEndpointStr + " timing out of query for expected commit index: "
                                    + minCommitIndex + " failed.", t);
                            query.fail(t);
                        }
                    }, timeoutNanos, NANOSECONDS);
                }
            } else {
                query.fail(newLaggingCommitIndexException(minCommitIndex));
            }
        } catch (Throwable t) {
            LOGGER.error(localEndpointStr + " query scheduling failed with {}", t);
            query.fail(t);
        }
    }

    public boolean isLeaderHeartbeatTimeoutElapsed() {
        return isLeaderHeartbeatTimeoutElapsed(lastLeaderHeartbeatTimestamp, clock.millis());
    }

    private boolean isLeaderHeartbeatTimeoutElapsed(long timestamp) {
        return isLeaderHeartbeatTimeoutElapsed(timestamp, clock.millis());
    }

    private boolean isLeaderHeartbeatTimeoutElapsed(long timestamp, long now) {
        return now - timestamp >= leaderHeartbeatTimeoutMillis;
    }

    private void initRestoredState() {
        SnapshotEntry snapshotEntry = state.log().snapshotEntry();
        if (isNonInitial(snapshotEntry)) {
            List<Object> chunkOperations = ((List<SnapshotChunk>) snapshotEntry.getOperation()).stream()
                    .map(SnapshotChunk::getOperation).collect(toList());
            stateMachine.installSnapshot(snapshotEntry.getIndex(), chunkOperations);
            publishRaftNodeReport(RaftNodeReportReason.INSTALL_SNAPSHOT);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.info(localEndpointStr + " restored " + snapshotEntry);
            } else {
                LOGGER.info(localEndpointStr + " restored snapshot commitIndex=" + snapshotEntry.getIndex());
            }
        }

        applyRestoredRaftGroupOps(snapshotEntry);
    }

    private void applyRestoredRaftGroupOps(SnapshotEntry snapshot) {
        // If there is a single Raft group operation after the last snapshot,
        // here we cannot know if the that operation is committed or not so we
        // just "prepare" the operation without committing it.
        // If there are multiple Raft group operations, it is definitely known
        // that all the operations up to the last Raft group operation are
        // committed, but the last Raft group operation may not be committed.
        // This conclusion boils down to the fact that once you append a Raft
        // group operation, you cannot append a new one before committing
        // the former.

        RaftLog log = state.log();
        LogEntry committedEntry = null;
        LogEntry lastAppliedEntry = null;

        for (long i = snapshot != null ? snapshot.getIndex() + 1 : 1; i <= log.lastLogOrSnapshotIndex(); i++) {
            LogEntry entry = log.getLogEntry(i);
            assert entry != null : localEndpointStr + " missing log entry at index: " + i;
            if (entry.getOperation() instanceof RaftGroupOp) {
                committedEntry = lastAppliedEntry;
                lastAppliedEntry = entry;
            }
        }

        if (committedEntry != null) {
            state.commitIndex(committedEntry.getIndex());
            applyLogEntries();
        }

        if (lastAppliedEntry != null) {
            if (lastAppliedEntry.getOperation() instanceof UpdateRaftGroupMembersOp) {
                setStatus(UPDATING_RAFT_GROUP_MEMBER_LIST);
                UpdateRaftGroupMembersOp groupOp = (UpdateRaftGroupMembersOp) lastAppliedEntry.getOperation();
                updateGroupMembers(lastAppliedEntry.getIndex(), groupOp.getMembers(), groupOp.getVotingMembers());
            } else {
                throw new IllegalStateException("Invalid Raft group op restored: " + lastAppliedEntry);
            }
        }
    }

    public RaftModelFactory getModelFactory() {
        return modelFactory;
    }

    public boolean demoteToFollowerIfQuorumHeartbeatTimeoutElapsed() {
        Optional<Long> quorumTimestamp = getQuorumHeartbeatTimestamp();
        if (quorumTimestamp.isEmpty()) {
            return true;
        }

        boolean demoteToFollower = isLeaderHeartbeatTimeoutElapsed(quorumTimestamp.get());
        if (demoteToFollower) {
            LOGGER.warn(
                    "{} Demoting to {} since not received append entries responses from majority recently. Latest quorum timestamp: {}",
                    localEndpointStr, FOLLOWER, quorumTimestamp.get());
            toFollower(state.term());
        }

        return demoteToFollower;
    }

    private Optional<Long> getQuorumHeartbeatTimestamp() {
        LeaderState leaderState = state.leaderState();
        if (leaderState == null) {
            return Optional.empty();
        }

        return Optional.of(leaderState.quorumResponseTimestamp(state.logReplicationQuorumSize(), clock.millis()));
    }

    /**
     * Switches this node to the follower role by clearing the known leader endpoint
     * and (pre) candidate states, and updating the term. If this Raft node was
     * leader before switching to the follower state, it may have some queries
     * waiting to be executed. Those queries are also failed with
     * {@link NotLeaderException}.
     *
     * @param term
     *            the new term to switch
     */
    public void toFollower(int term) {
        state.toFollower(term);
        publishRaftNodeReport(RaftNodeReportReason.ROLE_CHANGE);
    }

    public void runPreVote() {
        new PreVoteTask(this, state.term()).run();
    }

    /**
     * Switches this Raft node directly to the leader role for the next term.
     *
     * @see #toLeader()
     */
    public void toSingletonLeader() {
        state.toCandidate();
        BaseLogEntry lastLogEntry = state.log().lastLogOrSnapshotEntry();

        LOGGER.info("{} Leader election started for term: {}, last log index: {}, last log term: {}", localEndpointStr,
                state.term(), lastLogEntry.getIndex(), lastLogEntry.getTerm());

        publishRaftNodeReport(RaftNodeReportReason.ROLE_CHANGE);

        toLeader();

        LOGGER.info("{} We are the LEADER!", localEndpointStr());

        if (leaderFlushTask != null) {
            leaderFlushTask.run();
        } else {
            tryAdvanceCommitIndex();
        }
    }

    public Clock getClock() {
        return clock;
    }

}
