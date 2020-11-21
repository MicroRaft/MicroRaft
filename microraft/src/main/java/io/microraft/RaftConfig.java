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

package io.microraft;

import io.microraft.exception.CannotReplicateException;
import io.microraft.report.RaftNodeReport;

import java.io.Serializable;

/**
 * Contains the configuration parameters for MicroRaft's implementation.
 * <p>
 * RaftConfig is an immutable configuration class. You can use a RaftConfigBuilder to build a RaftConfig object.
 */
public final class RaftConfig
        implements Serializable {

    /**
     * The default value for {@link #leaderElectionTimeoutMillis}.
     */
    public static final long DEFAULT_LEADER_ELECTION_TIMEOUT_MILLIS = 1000;

    /**
     * The default value for {@link #leaderHeartbeatTimeoutSecs}
     */
    public static final long DEFAULT_LEADER_HEARTBEAT_TIMEOUT_SECS = 10;

    /**
     * The default value for {@link #leaderHeartbeatPeriodSecs}.
     */
    public static final long DEFAULT_LEADER_HEARTBEAT_PERIOD_SECS = 2;

    /**
     * The default value for {@link #maxPendingLogEntryCount}.
     */
    public static final int DEFAULT_MAX_PENDING_LOG_ENTRY_COUNT = 5000;

    /**
     * The default value for {@link #appendEntriesRequestBatchSize}.
     */
    public static final int DEFAULT_APPEND_ENTRIES_REQUEST_BATCH_SIZE = 1000;

    /**
     * The default value for {@link #commitCountToTakeSnapshot}.
     */
    public static final int DEFAULT_COMMIT_COUNT_TO_TAKE_SNAPSHOT = 50000;

    /**
     * The default value for {@link #transferSnapshotsFromFollowersEnabled}
     */
    public static final boolean DEFAULT_TRANSFER_SNAPSHOTS_FROM_FOLLOWERS_ENABLED = true;

    /**
     * The default value for {@link #raftNodeReportPublishPeriodSecs}.
     */
    public static final int DEFAULT_RAFT_NODE_REPORT_PUBLISH_PERIOD_SECS = 10;

    /**
     * The config object with default configuration.
     */
    public static final RaftConfig DEFAULT_RAFT_CONFIG = new RaftConfigBuilder().build();
    /**
     * Duration of leader election rounds in milliseconds. If a candidate cannot win majority votes before this timeout elapses, a
     * new leader election round is started. See "Section 5.2: Leader Election" in the Raft paper.
     */
    private final long leaderElectionTimeoutMillis;
    /**
     * Duration in seconds for a follower to decide on failure of the current leader and start a new leader election round. If
     * this duration is too short, a leader could be considered as failed unnecessarily in case of a small hiccup. If it is too
     * long, it takes longer to detect an actual failure.
     * <p>
     * Even though there is a single "election timeout" parameter in the Raft paper for both timing-out a leader election round
     * and detecting failure of the leader, MicroRaft uses two different parameters for these cases.
     * <p>
     * You can set {@link #leaderElectionTimeoutMillis} and this field to the same duration to align with the "election timeout"
     * definition in the Raft paper.
     */
    private final long leaderHeartbeatTimeoutSecs;
    /**
     * Duration in seconds for a Raft leader node to send periodic heartbeat requests to its followers in order to denote its
     * liveliness. Periodic heartbeat requests are actually append entries requests and can contain log entries. A heartbeat
     * request is not sent to a follower if an append entries request has been sent to that follower recently.
     */
    private final long leaderHeartbeatPeriodSecs;
    /**
     * Maximum number of pending log entries in the leader's Raft log before temporarily rejecting new requests of clients. This
     * configuration enables a back pressure mechanism to prevent OOME when a Raft leader cannot keep up with the requests sent by
     * the clients. When the "pending log entries buffer" whose capacity is specified with this configuration field is filled, new
     * requests fail with {@link CannotReplicateException} to slow down the clients. You can configure this field by considering
     * the degree of concurrency of your clients.
     */
    private final int maxPendingLogEntryCount;

    /**
     * In MicroRaft, a leader Raft node sends log entries to its followers in batches to improve the throughput. This
     * configuration parameter specifies the maximum number of Raft log entries that can be sent as a batch in a single append
     * entries request.
     */
    private final int appendEntriesRequestBatchSize;
    /**
     * Number of new commits to initiate a new snapshot after the last snapshot taken by a Raft node. This value must be
     * configured wisely as it effects performance of the system in multiple ways. If a small value is set, it means that
     * snapshots are taken too frequently and Raft nodes keep a very short Raft log. If snapshot objects are large and the Raft
     * state is persisted to disk, this can create an unnecessary overhead on IO performance. Moreover, a Raft leader can send too
     * many snapshots to slow followers which can create a network overhead. On the other hand, if a very large value is set, it
     * can create a memory overhead since Raft log entries are going to be kept in memory until the next snapshot.
     */
    private final int commitCountToTakeSnapshot;

    /**
     * If enabled, when a Raft follower falls far behind the Raft leader and needs to install a snapshot, it transfers the
     * snapshot chunks from both the Raft leader and other followers in parallel. This is a safe optimization because in MicroRaft
     * snapshots are taken at the same log indices on all Raft nodes.
     */
    private final boolean transferSnapshotsFromFollowersEnabled;

    /**
     * Denotes how frequently a Raft node publishes a report of its internal Raft state. {@link RaftNodeReport} objects can be
     * used for monitoring a running Raft group.
     */
    private final int raftNodeReportPublishPeriodSecs;

    public RaftConfig(long leaderElectionTimeoutMillis, long leaderHeartbeatPeriodSecs, long leaderHeartbeatTimeoutSecs,
                      int appendEntriesRequestBatchSize, int commitCountToTakeSnapshot, int maxPendingLogEntryCount,
                      boolean transferSnapshotsFromFollowersEnabled, int raftNodeReportPublishPeriodSecs) {
        this.leaderElectionTimeoutMillis = leaderElectionTimeoutMillis;
        this.leaderHeartbeatPeriodSecs = leaderHeartbeatPeriodSecs;
        this.leaderHeartbeatTimeoutSecs = leaderHeartbeatTimeoutSecs;
        this.appendEntriesRequestBatchSize = appendEntriesRequestBatchSize;
        this.commitCountToTakeSnapshot = commitCountToTakeSnapshot;
        this.maxPendingLogEntryCount = maxPendingLogEntryCount;
        this.transferSnapshotsFromFollowersEnabled = transferSnapshotsFromFollowersEnabled;
        this.raftNodeReportPublishPeriodSecs = raftNodeReportPublishPeriodSecs;
    }

    /**
     * Creates a new Raft config builder.
     *
     * @return the builder to populate the parameters for RaftConfig.
     */
    public static RaftConfigBuilder newBuilder() {
        return new RaftConfigBuilder();
    }

    private static void checkPositive(long value, String errorMessage) {
        if (value <= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * @return the leader election timeout in milliseconds
     *
     * @see #leaderElectionTimeoutMillis
     */
    public long getLeaderElectionTimeoutMillis() {
        return leaderElectionTimeoutMillis;
    }

    /**
     * @return leader heartbeat timeout seconds
     *
     * @see #leaderHeartbeatTimeoutSecs
     */
    public long getLeaderHeartbeatTimeoutSecs() {
        return leaderHeartbeatTimeoutSecs;
    }

    /**
     * @return the leader election heartbeat period in seconds
     *
     * @see #leaderHeartbeatPeriodSecs
     */
    public long getLeaderHeartbeatPeriodSecs() {
        return leaderHeartbeatPeriodSecs;
    }

    /**
     * @return the max pending log entry count
     *
     * @see #maxPendingLogEntryCount
     */
    public int getMaxPendingLogEntryCount() {
        return maxPendingLogEntryCount;
    }

    /**
     * @return the append entries request batch size
     *
     * @see #appendEntriesRequestBatchSize
     */
    public int getAppendEntriesRequestBatchSize() {
        return appendEntriesRequestBatchSize;
    }

    /**
     * @return the commit count to take snapshot
     *
     * @see #commitCountToTakeSnapshot
     */
    public int getCommitCountToTakeSnapshot() {
        return commitCountToTakeSnapshot;
    }

    /**
     * @return true if the transfer snapshots from followers enabled
     *
     * @see #transferSnapshotsFromFollowersEnabled
     */
    public boolean isTransferSnapshotsFromFollowersEnabled() {
        return transferSnapshotsFromFollowersEnabled;
    }

    /**
     * @return the raft node report publish period in seconds
     *
     * @see #raftNodeReportPublishPeriodSecs
     */
    public int getRaftNodeReportPublishPeriodSecs() {
        return raftNodeReportPublishPeriodSecs;
    }

    @Override public String toString() {
        return "RaftConfig{" + "leaderElectionTimeoutMillis=" + leaderElectionTimeoutMillis + ", leaderHeartbeatTimeoutSecs="
               + leaderHeartbeatTimeoutSecs + ", leaderHeartbeatPeriodSecs=" + leaderHeartbeatPeriodSecs
               + ", maxPendingLogEntryCount=" + maxPendingLogEntryCount + ", appendEntriesRequestBatchSize="
               + appendEntriesRequestBatchSize + ", commitCountToTakeSnapshot=" + commitCountToTakeSnapshot
               + ", transferSnapshotsFromFollowersEnabled=" + transferSnapshotsFromFollowersEnabled
               + ", raftNodeReportPublishPeriodSecs=" + raftNodeReportPublishPeriodSecs + '}';
    }

    /**
     * Builder for Raft config
     */
    public static final class RaftConfigBuilder {

        private long leaderElectionTimeoutMillis = DEFAULT_LEADER_ELECTION_TIMEOUT_MILLIS;
        private long leaderHeartbeatPeriodSecs = DEFAULT_LEADER_HEARTBEAT_PERIOD_SECS;
        private long leaderHeartbeatTimeoutSecs = DEFAULT_LEADER_HEARTBEAT_TIMEOUT_SECS;
        private int appendEntriesRequestBatchSize = DEFAULT_APPEND_ENTRIES_REQUEST_BATCH_SIZE;
        private int commitCountToTakeSnapshot = DEFAULT_COMMIT_COUNT_TO_TAKE_SNAPSHOT;
        private int maxPendingLogEntryCount = DEFAULT_MAX_PENDING_LOG_ENTRY_COUNT;
        private boolean transferSnapshotsFromFollowersEnabled = DEFAULT_TRANSFER_SNAPSHOTS_FROM_FOLLOWERS_ENABLED;
        private int raftNodeReportPublishPeriodSecs = DEFAULT_RAFT_NODE_REPORT_PUBLISH_PERIOD_SECS;

        private RaftConfigBuilder() {
        }

        /**
         * @param leaderElectionTimeoutMillis
         *         the leader election timeout in milliseconds value to set
         *
         * @return the builder object for fluent calls
         *
         * @see RaftConfig#leaderElectionTimeoutMillis
         */
        public RaftConfigBuilder setLeaderElectionTimeoutMillis(long leaderElectionTimeoutMillis) {
            checkPositive(leaderElectionTimeoutMillis, "leader election timeout millis must be positive!");
            this.leaderElectionTimeoutMillis = leaderElectionTimeoutMillis;
            return this;
        }

        /**
         * @param leaderHeartbeatTimeoutSecs
         *         the leader heartbeat timeout in seconds value to set
         *
         * @return the builder object for fluent calls
         *
         * @see RaftConfig#leaderHeartbeatTimeoutSecs
         */
        public RaftConfigBuilder setLeaderHeartbeatTimeoutSecs(long leaderHeartbeatTimeoutSecs) {
            checkPositive(leaderHeartbeatTimeoutSecs, "leader heartbeat timeout secs must be positive!");
            this.leaderHeartbeatTimeoutSecs = leaderHeartbeatTimeoutSecs;
            return this;
        }

        /**
         * @param leaderHeartbeatPeriodSecs
         *         the leader heartbeat period in seconds value to set
         *
         * @return the builder object for fluent calls
         *
         * @see RaftConfig#leaderHeartbeatPeriodSecs
         */
        public RaftConfigBuilder setLeaderHeartbeatPeriodSecs(long leaderHeartbeatPeriodSecs) {
            checkPositive(leaderHeartbeatPeriodSecs, "leader heartbeat period secs must be positive!");
            this.leaderHeartbeatPeriodSecs = leaderHeartbeatPeriodSecs;
            return this;
        }

        /**
         * @param appendEntriesRequestBatchSize
         *         the append entries request batch size value to set
         *
         * @return the builder object for fluent calls
         *
         * @see RaftConfig#appendEntriesRequestBatchSize
         */
        public RaftConfigBuilder setAppendEntriesRequestBatchSize(int appendEntriesRequestBatchSize) {
            checkPositive(appendEntriesRequestBatchSize, "append entries request batch size must be positive!");
            this.appendEntriesRequestBatchSize = appendEntriesRequestBatchSize;
            return this;
        }

        /**
         * @param commitCountToTakeSnapshot
         *         the commit count to take snapshot value to set
         *
         * @return the builder object for fluent calls
         *
         * @see RaftConfig#commitCountToTakeSnapshot
         */
        public RaftConfigBuilder setCommitCountToTakeSnapshot(int commitCountToTakeSnapshot) {
            checkPositive(commitCountToTakeSnapshot, "commit count to take snapshot must be positive!");
            this.commitCountToTakeSnapshot = commitCountToTakeSnapshot;
            return this;
        }

        /**
         * @param maxPendingLogEntryCount
         *         the max pending log entry count value to set
         *
         * @return the builder object for fluent calls
         *
         * @see RaftConfig#maxPendingLogEntryCount
         */
        public RaftConfigBuilder setMaxPendingLogEntryCount(int maxPendingLogEntryCount) {
            checkPositive(maxPendingLogEntryCount, "max pending entry count to reject new appends must be positive!");
            this.maxPendingLogEntryCount = maxPendingLogEntryCount;
            return this;
        }

        /**
         * @param transferSnapshotsFromFollowersEnabled
         *         the transfer snapshot from followers value to set
         *
         * @return the builder object for fluent calls
         *
         * @see #transferSnapshotsFromFollowersEnabled
         */
        public RaftConfigBuilder setTransferSnapshotsFromFollowersEnabled(boolean transferSnapshotsFromFollowersEnabled) {
            this.transferSnapshotsFromFollowersEnabled = transferSnapshotsFromFollowersEnabled;
            return this;
        }

        /**
         * @param raftNodeReportPublishPeriodSecs
         *         the raft node report publish period value to set
         *
         * @return the builder object for fluent calls
         *
         * @see #raftNodeReportPublishPeriodSecs
         */
        public RaftConfigBuilder setRaftNodeReportPublishPeriodSecs(int raftNodeReportPublishPeriodSecs) {
            checkPositive(raftNodeReportPublishPeriodSecs, "raft node state snapshot publish period seconds must be positive!");
            this.raftNodeReportPublishPeriodSecs = raftNodeReportPublishPeriodSecs;
            return this;
        }

        /**
         * Builds the RaftConfig object.
         *
         * @return the RaftConfig object.
         */
        public RaftConfig build() {
            if (leaderHeartbeatTimeoutSecs < leaderHeartbeatPeriodSecs) {
                throw new IllegalArgumentException("leader heartbeat timeout secs: " + leaderHeartbeatTimeoutSecs
                                                   + " cannot be smaller than leader heartbeat timeout period secs: "
                                                   + leaderHeartbeatPeriodSecs);
            }

            return new RaftConfig(leaderElectionTimeoutMillis, leaderHeartbeatPeriodSecs, leaderHeartbeatTimeoutSecs,
                                  appendEntriesRequestBatchSize, commitCountToTakeSnapshot, maxPendingLogEntryCount,
                                  transferSnapshotsFromFollowersEnabled, raftNodeReportPublishPeriodSecs);
        }

        @Override public String toString() {
            return "RaftConfigBuilder{" + "leaderElectionTimeoutMillis=" + leaderElectionTimeoutMillis
                   + ", leaderHeartbeatPeriodSecs=" + leaderHeartbeatPeriodSecs + ", leaderHeartbeatTimeoutSecs="
                   + leaderHeartbeatTimeoutSecs + ", appendEntriesRequestBatchSize=" + appendEntriesRequestBatchSize
                   + ", commitCountToTakeSnapshot=" + commitCountToTakeSnapshot + ", maxPendingLogEntryCount="
                   + maxPendingLogEntryCount + ", transferSnapshotsFromFollowersEnabled=" + transferSnapshotsFromFollowersEnabled
                   + ", raftNodeReportPublishPeriodSecs=" + raftNodeReportPublishPeriodSecs + '}';
        }
    }

}
