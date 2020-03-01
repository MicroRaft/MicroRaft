package com.hazelcast.raft;

import java.io.Serializable;

import static com.hazelcast.raft.impl.util.Preconditions.checkPositive;

/**
 * Contains the configuration parameters for the Raft consensus algorithm
 * implementation. Some of the parameters are related to the Raft consensus
 * algorithm itself and while some others are for fine-tuning the
 * implementation.
 * <p>
 * This is an immutable class.
 *
 * @author mdogan
 * @author metanet
 */
public class RaftConfig
        implements Serializable {

    private static final long serialVersionUID = -4666425647369466671L;

    /**
     * The default value for {@link #leaderElectionTimeoutMs}
     */
    public static final long DEFAULT_LEADER_ELECTION_TIMEOUT_MS = 2000;

    /**
     * The default value for leader @link #leaderHeartbeatPeriodMs}
     */
    public static final long DEFAULT_LEADER_HEARTBEAT_PERIOD_MS = 5000;

    /**
     * The default value for @link #appendRequestMaxEntryCount}
     */
    public static final int DEFAULT_APPEND_ENTRIES_REQUEST_MAX_LOG_ENTRY_COUNT = 100;

    /**
     * The default value for {@link #commitIndexAdvanceCountToTakeSnapshot}
     */
    public static final int DEFAULT_COMMIT_INDEX_ADVANCE_COUNT_TO_TAKE_SNAPSHOT = 10000;

    /**
     * The default value for {@link #uncommittedLogEntryCountToRejectNewAppends}
     */
    public static final int DEFAULT_UNCOMMITTED_LOG_ENTRY_COUNT_TO_REJECT_NEW_APPENDS = 100;

    /**
     * The default value for {@link #maxMissedLeaderHeartbeatCount}
     */
    public static final int DEFAULT_MAX_MISSED_LEADER_HEARTBEAT_COUNT = 3;

    /**
     * The default value for {@link #appendEntriesRequestBackoffTimeoutMs}
     */
    public static final long DEFAULT_APPEND_ENTRIES_REQUEST_BACKOFF_TIMEOUT_MS = 100;

    /**
     * The default value for {@link #linearizableReadOptimizationEnabled}
     */
    public static final boolean DEFAULT_LINEARIZABLE_READ_OPTIMIZATION_ENABLED = true;

    /**
     * The default value for {@link #raftStateSummaryPublishPeriodSecs}
     */
    public static final int DEFAULT_RAFT_STATE_SUMMARY_PUBLISH_PERIOD_SECS = 5;

    /**
     * The config object with default configuration.
     */
    public static final RaftConfig DEFAULT_RAFT_CONFIG = new RaftConfigBuilder().build();

    /**
     * Creates a new Raft config builder
     */
    public static RaftConfigBuilder builder() {
        return new RaftConfigBuilder();
    }

    /**
     * Leader election timeout in milliseconds. If a candidate cannot win
     * majority votes in a timely manner, a new election round is initiated.
     * See "Section 5.2: Leader Election" in the Raft paper.
     */
    private final long leaderElectionTimeoutMs;

    /**
     * Duration for a leader to send periodic append entries requests to
     * followers as periodic heartbeats. See "Section 5.2: Leader Election"
     * in the Raft paper.
     */
    private final long leaderHeartbeatPeriodMs;

    /**
     * Maximum number of missed leader heartbeats for a follower to trigger
     * a new leader election. The "election timeout" parameter described in
     * "Section 5.2: Leader Election" in the Raf paper is calculated by
     * multiplying {@link #leaderElectionTimeoutMs} with this field.
     */
    private final int maxMissedLeaderHeartbeatCount;

    /**
     * Maximum number of Raft log entries that can be sent as a batch
     * in a single append entries request.
     */
    private final int appendEntriesRequestMaxLogEntryCount;

    /**
     * Exact number of new commits in the Raft log to take a new snapshot after
     * the last snapshot.
     */
    private final int commitIndexAdvanceCountToTakeSnapshot;

    /**
     * Maximum number of uncommitted Raft log entries in the leader before
     * temporarily rejecting new requests from callers. A Raft leader node can
     * append new entries while there are uncommitted entries in its Raft log.
     */
    private final int uncommittedLogEntryCountToRejectNewAppends;

    /**
     * Timeout in milliseconds to wait for sending a new append entries request
     * without receiving a response for the previous request. After a Raft
     * group leader sends an append entries request to a follower, it will not
     * send a new append entries request until either the follower responds
     * to the former request tor this timeout occurs.
     */
    private final long appendEntriesRequestBackoffTimeoutMs;

    /**
     * If enabled, {@link QueryPolicy#LINEARIZABLE} reads are committed to
     * the majority without appending an entry to the Raft log. It is
     * recommended to keep this optimization enabled.
     * <p>
     * See Section 6.4 of the Raft dissertation for more information about
     * the linearizable read optimization.
     */
    private final boolean linearizableReadOptimizationEnabled;

    /**
     * Denotes how frequently Raft nodes publish summary of their internal
     * Raft states.
     */
    private final int raftStateSummaryPublishPeriodSecs;

    public RaftConfig(long leaderElectionTimeoutMs, long leaderHeartbeatPeriodMs, int maxMissedLeaderHeartbeatCount,
                      int appendEntriesRequestMaxLogEntryCount, int commitIndexAdvanceCountToTakeSnapshot,
                      int uncommittedLogEntryCountToRejectNewAppends, long appendEntriesRequestBackoffTimeoutMs,
                      boolean linearizableReadOptimizationEnabled, int raftStateSummaryPublishPeriodSecs) {
        this.leaderElectionTimeoutMs = leaderElectionTimeoutMs;
        this.leaderHeartbeatPeriodMs = leaderHeartbeatPeriodMs;
        this.maxMissedLeaderHeartbeatCount = maxMissedLeaderHeartbeatCount;
        this.appendEntriesRequestMaxLogEntryCount = appendEntriesRequestMaxLogEntryCount;
        this.commitIndexAdvanceCountToTakeSnapshot = commitIndexAdvanceCountToTakeSnapshot;
        this.uncommittedLogEntryCountToRejectNewAppends = uncommittedLogEntryCountToRejectNewAppends;
        this.appendEntriesRequestBackoffTimeoutMs = appendEntriesRequestBackoffTimeoutMs;
        this.linearizableReadOptimizationEnabled = linearizableReadOptimizationEnabled;
        this.raftStateSummaryPublishPeriodSecs = raftStateSummaryPublishPeriodSecs;
    }

    /**
     * @see #leaderElectionTimeoutMs
     */
    public long getLeaderElectionTimeoutMs() {
        return leaderElectionTimeoutMs;
    }

    /**
     * @see #leaderHeartbeatPeriodMs
     */
    public long getLeaderHeartbeatPeriodMs() {
        return leaderHeartbeatPeriodMs;
    }

    /**
     * @see #appendEntriesRequestMaxLogEntryCount
     */
    public int getAppendEntriesRequestMaxLogEntryCount() {
        return appendEntriesRequestMaxLogEntryCount;
    }

    /**
     * @see #commitIndexAdvanceCountToTakeSnapshot
     */
    public int getCommitIndexAdvanceCountToTakeSnapshot() {
        return commitIndexAdvanceCountToTakeSnapshot;
    }

    /**
     * @see #uncommittedLogEntryCountToRejectNewAppends
     */
    public int getUncommittedLogEntryCountToRejectNewAppends() {
        return uncommittedLogEntryCountToRejectNewAppends;
    }

    /**
     * @see #maxMissedLeaderHeartbeatCount
     */
    public int getMaxMissedLeaderHeartbeatCount() {
        return maxMissedLeaderHeartbeatCount;
    }

    /**
     * @see #appendEntriesRequestBackoffTimeoutMs
     */
    public long getAppendEntriesRequestBackoffTimeoutMs() {
        return appendEntriesRequestBackoffTimeoutMs;
    }

    /**
     * @see #linearizableReadOptimizationEnabled
     */
    public boolean isLinearizableReadOptimizationEnabled() {
        return linearizableReadOptimizationEnabled;
    }

    /**
     * @see #raftStateSummaryPublishPeriodSecs
     */
    public int getRaftStateSummaryPublishPeriodSecs() {
        return raftStateSummaryPublishPeriodSecs;
    }

    /**
     * Builder for Raft config
     */
    public static class RaftConfigBuilder {

        private long leaderElectionTimeoutMs = DEFAULT_LEADER_ELECTION_TIMEOUT_MS;
        private long leaderHeartbeatPeriodMs = DEFAULT_LEADER_HEARTBEAT_PERIOD_MS;
        private int maxMissedLeaderHeartbeatCount = DEFAULT_MAX_MISSED_LEADER_HEARTBEAT_COUNT;
        private int appendEntriesRequestMaxLogEntryCount = DEFAULT_APPEND_ENTRIES_REQUEST_MAX_LOG_ENTRY_COUNT;
        private int commitIndexAdvanceCountToTakeSnapshot = DEFAULT_COMMIT_INDEX_ADVANCE_COUNT_TO_TAKE_SNAPSHOT;
        private int uncommittedLogEntryCountToRejectNewAppends = DEFAULT_UNCOMMITTED_LOG_ENTRY_COUNT_TO_REJECT_NEW_APPENDS;
        private long appendEntriesRequestBackoffTimeoutMs = DEFAULT_APPEND_ENTRIES_REQUEST_BACKOFF_TIMEOUT_MS;
        private boolean linearizableReadOptimizationEnabled = DEFAULT_LINEARIZABLE_READ_OPTIMIZATION_ENABLED;
        private int raftStateSummaryPublishPeriodSecs = DEFAULT_RAFT_STATE_SUMMARY_PUBLISH_PERIOD_SECS;

        private RaftConfigBuilder() {
        }

        /**
         * @see RaftConfig#leaderElectionTimeoutMs
         */
        public RaftConfigBuilder setLeaderElectionTimeoutMs(long leaderElectionTimeoutMs) {
            checkPositive(leaderElectionTimeoutMs,
                    "leader election timeout in ms: " + leaderElectionTimeoutMs + " must be positive!");
            this.leaderElectionTimeoutMs = leaderElectionTimeoutMs;
            return this;
        }

        /**
         * @see RaftConfig#leaderHeartbeatPeriodMs
         */
        public RaftConfigBuilder setLeaderHeartbeatPeriodMs(long leaderHeartbeatPeriodMs) {
            checkPositive(leaderHeartbeatPeriodMs,
                    "leader heartbeat period in ms: " + leaderHeartbeatPeriodMs + " must be positive!");
            this.leaderHeartbeatPeriodMs = leaderHeartbeatPeriodMs;
            return this;
        }

        /**
         * @see RaftConfig#appendEntriesRequestMaxLogEntryCount
         */
        public RaftConfigBuilder setAppendEntriesRequestMaxLogEntryCount(int appendEntriesRequestMaxLogEntryCount) {
            checkPositive(appendEntriesRequestMaxLogEntryCount,
                    "append request max entry count: " + appendEntriesRequestMaxLogEntryCount + " must be positive!");
            this.appendEntriesRequestMaxLogEntryCount = appendEntriesRequestMaxLogEntryCount;
            return this;
        }

        /**
         * @see RaftConfig#commitIndexAdvanceCountToTakeSnapshot
         */
        public RaftConfigBuilder setCommitIndexAdvanceCountToTakeSnapshot(int commitIndexAdvanceCountToTakeSnapshot) {
            checkPositive(commitIndexAdvanceCountToTakeSnapshot,
                    "commit index advance count to snapshot: " + commitIndexAdvanceCountToTakeSnapshot + " must be positive!");
            this.commitIndexAdvanceCountToTakeSnapshot = commitIndexAdvanceCountToTakeSnapshot;
            return this;
        }

        /**
         * @see RaftConfig#uncommittedLogEntryCountToRejectNewAppends
         */
        public RaftConfigBuilder setUncommittedLogEntryCountToRejectNewAppends(int uncommittedLogEntryCountToRejectNewAppends) {
            checkPositive(uncommittedLogEntryCountToRejectNewAppends,
                    "uncommitted entry count to reject new appends: " + uncommittedLogEntryCountToRejectNewAppends
                            + " must be positive!");
            this.uncommittedLogEntryCountToRejectNewAppends = uncommittedLogEntryCountToRejectNewAppends;
            return this;
        }

        /**
         * @see RaftConfig#maxMissedLeaderHeartbeatCount
         */
        public RaftConfigBuilder setMaxMissedLeaderHeartbeatCount(int maxMissedLeaderHeartbeatCount) {
            checkPositive(maxMissedLeaderHeartbeatCount, "max missed leader heartbeat count must be positive!");
            this.maxMissedLeaderHeartbeatCount = maxMissedLeaderHeartbeatCount;
            return this;
        }

        /**
         * @see RaftConfig#appendEntriesRequestBackoffTimeoutMs
         */
        public RaftConfigBuilder setAppendEntriesRequestBackoffTimeoutMs(long appendEntriesRequestBackoffTimeoutMs) {
            checkPositive(appendEntriesRequestBackoffTimeoutMs, "append entries request backoff timeout must be positive!");
            this.appendEntriesRequestBackoffTimeoutMs = appendEntriesRequestBackoffTimeoutMs;
            return this;
        }

        /**
         * @see RaftConfig#linearizableReadOptimizationEnabled
         */
        public RaftConfigBuilder setLinearizableReadOptimizationEnabled(boolean linearizableReadOptimizationEnabled) {
            this.linearizableReadOptimizationEnabled = linearizableReadOptimizationEnabled;
            return this;
        }

        public RaftConfigBuilder setRaftStateSummaryPublishPeriodSecs(int raftStateSummaryPublishPeriodSecs) {
            checkPositive(raftStateSummaryPublishPeriodSecs, "raft state summary publish period seconds must be positive!");
            this.raftStateSummaryPublishPeriodSecs = raftStateSummaryPublishPeriodSecs;
            return this;
        }

        /**
         * Builds the Raft config object
         */
        public RaftConfig build() {
            return new RaftConfig(leaderElectionTimeoutMs, leaderHeartbeatPeriodMs, maxMissedLeaderHeartbeatCount,
                    appendEntriesRequestMaxLogEntryCount, commitIndexAdvanceCountToTakeSnapshot,
                    uncommittedLogEntryCountToRejectNewAppends, appendEntriesRequestBackoffTimeoutMs,
                    linearizableReadOptimizationEnabled, raftStateSummaryPublishPeriodSecs);
        }

    }

}
