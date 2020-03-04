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

package io.microraft;

import java.io.Serializable;

/**
 * Contains the configuration parameters for MicroRaft's algorithm. Some of
 * the parameters are related to the Raft consensus algorithm itself and while
 * some others are for fine-tuning the implementation.
 * <p>
 * This is an immutable class.
 *
 * @author mdogan
 * @author metanet
 */
public class RaftConfig
        implements Serializable {

    /**
     * The default value for {@link #leaderElectionTimeoutMillis}.
     */
    public static final long DEFAULT_LEADER_ELECTION_TIMEOUT_MILLIS = 1000;

    /**
     * The default value for {@link #leaderHeartbeatPeriodMillis}.
     */
    public static final long DEFAULT_LEADER_HEARTBEAT_PERIOD_MILLIS = 2000;

    /**
     * The default value for {@link #leaderHeartbeatTimeoutMillis}
     */
    public static final long DEFAULT_LEADER_HEARTBEAT_TIMEOUT_MILLIS = 10000;

    /**
     * The default value for {@link #appendEntriesRequestBatchSize}.
     */
    public static final int DEFAULT_APPEND_ENTRIES_REQUEST_BATCH_SIZE = 1000;

    /**
     * The default value for {@link #commitCountToTakeSnapshot}.
     */
    public static final int DEFAULT_COMMIT_COUNT_TO_TAKE_SNAPSHOT = 50000;

    /**
     * The default value for {@link #maxUncommittedLogEntryCount}.
     */
    public static final int DEFAULT_MAX_UNCOMMITTED_LOG_ENTRY_COUNT = 1000;

    /**
     * The default value for {@link #leaderBackoffDurationMillis}.
     */
    public static final long DEFAULT_LEADER_BACKOFF_DURATION_MILLIS = 100;

    /**
     * The default value for {@link #raftNodeReportPublishPeriodSecs}.
     */
    public static final int DEFAULT_RAFT_NODE_REPORT_PUBLISH_PERIOD_SECS = 10;

    /**
     * The config object with default configuration.
     */
    public static final RaftConfig DEFAULT_RAFT_CONFIG = new RaftConfigBuilder().build();
    /**
     * Leader election timeout in milliseconds. If a candidate cannot win
     * majority votes before this timeout elapses, a new election round is
     * started. See "Section 5.2: Leader Election" in the Raft paper.
     */
    private final long leaderElectionTimeoutMillis;
    /**
     * Duration in milliseconds for a Raft leader node to send periodic
     * heartbeat messages to its followers in order to denote its liveliness.
     * See "Section 5.2: Leader Election" in the Raft paper. Periodic heartbeat
     * messages are actually append entries requests and can contain log
     * entries for the lagging followers. If a too small value is set,
     * heartbeat messages are sent from Raft leaders to followers too
     * frequently and it can cause an unnecessary usage of CPU and network.
     */
    private final long leaderHeartbeatPeriodMillis;
    /**
     * Duration in milliseconds for a follower to decide on failure of
     * the current leader and start a new leader election round. If this
     * duration is too small, a leader could be considered as failed
     * unnecessarily in case of a small hiccup. If it is too large, it takes
     * longer to detect an actual failure.
     * <p>
     * Even though there is a single "election timeout" parameter in the Raft
     * paper for both timing-out a leader election round and detecting failure
     * of the leader, MicroRaft uses two different parameters for these cases.
     * <p>
     * You can set {@link #leaderElectionTimeoutMillis} and this field to
     * the same value to align with the "election timeout" definition
     * in the Raft paper.
     */
    private final long leaderHeartbeatTimeoutMillis;
    /**
     * Maximum number of Raft log entries that can be sent as a batch
     * in a single append entries request.
     */
    private final int appendEntriesRequestBatchSize;
    /**
     * Number of new commits to initiate a new snapshot after the last snapshot
     * taken by a Raft node. This value must be configured wisely as it effects
     * performance of the system in multiple ways. If a small value is set, it
     * means that snapshots are taken too frequently and Raft nodes keep a very
     * short Raft log. If snapshot objects are large and the Raft state is
     * persisted to disk, this can create an unnecessary overhead on IO
     * performance. Moreover, a Raft leader can send too many snapshots to
     * slow followers which can create a network overhead. On the other hand,
     * if a very large value is set, it can create a memory overhead since Raft
     * log entries are going to be kept in memory until the next snapshot.
     */
    private final int commitCountToTakeSnapshot;
    /**
     * Maximum number of uncommitted log entries in the leader's Raft log
     * before temporarily rejecting new requests of clients. Since a Raft
     * leader sends log entries to followers in batches, it accumulates
     * incoming requests in order to improve the throughput. You can configure
     * this field by considering the degree of concurrency of the clients.
     * For instance, if there are at most 1000 clients sending requests
     * to a Raft leader, this field can be set to 1000 so that clients do not
     * get retry responses unnecessarily.
     */
    private final int maxUncommittedLogEntryCount;
    /**
     * Duration in milliseconds to apply backoff on the Raft leader for append
     * entries and install snapshot requests. In MicroRaft, a Raft leader runs
     * a separate replication pipeline for each follower. After it sends
     * an append entries or an install snapshot request to a follower, it does
     * not send a subsequent request either until the follower responds to
     * the previous request or this backoff duration elapses.
     * <p>
     * This configuration decides the initial backoff duration. The backoff
     * duration grows exponentially per follower if a follower remains
     * unresponsive.
     */
    private final long leaderBackoffDurationMillis;
    /**
     * Denotes how frequently a Raft node publishes a report of its internal
     * Raft state.
     */
    private final int raftNodeReportPublishPeriodSecs;

    public RaftConfig(long leaderElectionTimeoutMillis, long leaderHeartbeatPeriodMillis, long leaderHeartbeatTimeoutMillis,
                      int appendEntriesRequestBatchSize, int commitCountToTakeSnapshot, int maxUncommittedLogEntryCount,
                      long leaderBackoffDurationMillis, int raftNodeReportPublishPeriodSecs) {
        this.leaderElectionTimeoutMillis = leaderElectionTimeoutMillis;
        this.leaderHeartbeatPeriodMillis = leaderHeartbeatPeriodMillis;
        this.leaderHeartbeatTimeoutMillis = leaderHeartbeatTimeoutMillis;
        this.appendEntriesRequestBatchSize = appendEntriesRequestBatchSize;
        this.commitCountToTakeSnapshot = commitCountToTakeSnapshot;
        this.maxUncommittedLogEntryCount = maxUncommittedLogEntryCount;
        this.leaderBackoffDurationMillis = leaderBackoffDurationMillis;
        this.raftNodeReportPublishPeriodSecs = raftNodeReportPublishPeriodSecs;
    }

    /**
     * Creates a new Raft config builder
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
     * @see #leaderElectionTimeoutMillis
     */
    public long getLeaderElectionTimeoutMillis() {
        return leaderElectionTimeoutMillis;
    }

    /**
     * @see #leaderHeartbeatPeriodMillis
     */
    public long getLeaderHeartbeatPeriodMillis() {
        return leaderHeartbeatPeriodMillis;
    }

    /**
     * @see #appendEntriesRequestBatchSize
     */
    public int getAppendEntriesRequestBatchSize() {
        return appendEntriesRequestBatchSize;
    }

    /**
     * @see #commitCountToTakeSnapshot
     */
    public int getCommitCountToTakeSnapshot() {
        return commitCountToTakeSnapshot;
    }

    /**
     * @see #maxUncommittedLogEntryCount
     */
    public int getMaxUncommittedLogEntryCount() {
        return maxUncommittedLogEntryCount;
    }

    /**
     * @see #leaderHeartbeatTimeoutMillis
     */
    public long getLeaderHeartbeatTimeoutMillis() {
        return leaderHeartbeatTimeoutMillis;
    }

    /**
     * @see #leaderBackoffDurationMillis
     */
    public long getLeaderBackoffDurationMillis() {
        return leaderBackoffDurationMillis;
    }

    /**
     * @see #raftNodeReportPublishPeriodSecs
     */
    public int getRaftNodeReportPublishPeriodSecs() {
        return raftNodeReportPublishPeriodSecs;
    }

    /**
     * Builder for Raft config
     */
    public static class RaftConfigBuilder {

        private long leaderElectionTimeoutMillis = DEFAULT_LEADER_ELECTION_TIMEOUT_MILLIS;
        private long leaderHeartbeatPeriodMillis = DEFAULT_LEADER_HEARTBEAT_PERIOD_MILLIS;
        private long leaderHeartbeatTimeoutMillis = DEFAULT_LEADER_HEARTBEAT_TIMEOUT_MILLIS;
        private int appendEntriesRequestBatchSize = DEFAULT_APPEND_ENTRIES_REQUEST_BATCH_SIZE;
        private int commitCountToTakeSnapshot = DEFAULT_COMMIT_COUNT_TO_TAKE_SNAPSHOT;
        private int maxUncommittedLogEntryCount = DEFAULT_MAX_UNCOMMITTED_LOG_ENTRY_COUNT;
        private long leaderBackoffDurationMillis = DEFAULT_LEADER_BACKOFF_DURATION_MILLIS;
        private int raftNodeReportPublishPeriodSecs = DEFAULT_RAFT_NODE_REPORT_PUBLISH_PERIOD_SECS;

        private RaftConfigBuilder() {
        }

        /**
         * @see RaftConfig#leaderElectionTimeoutMillis
         */
        public RaftConfigBuilder setLeaderElectionTimeoutMillis(long leaderElectionTimeoutMillis) {
            checkPositive(leaderElectionTimeoutMillis, "leader election timeout millis must be positive!");
            this.leaderElectionTimeoutMillis = leaderElectionTimeoutMillis;
            return this;
        }

        /**
         * @see RaftConfig#leaderHeartbeatPeriodMillis
         */
        public RaftConfigBuilder setLeaderHeartbeatPeriodMillis(long leaderHeartbeatPeriodMillis) {
            checkPositive(leaderHeartbeatPeriodMillis, "leader heartbeat period millis must be positive!");
            this.leaderHeartbeatPeriodMillis = leaderHeartbeatPeriodMillis;
            return this;
        }

        /**
         * @see RaftConfig#appendEntriesRequestBatchSize
         */
        public RaftConfigBuilder setAppendEntriesRequestBatchSize(int appendEntriesRequestBatchSize) {
            checkPositive(appendEntriesRequestBatchSize, "append entries request batch size must be positive!");
            this.appendEntriesRequestBatchSize = appendEntriesRequestBatchSize;
            return this;
        }

        /**
         * @see RaftConfig#commitCountToTakeSnapshot
         */
        public RaftConfigBuilder setCommitCountToTakeSnapshot(int commitCountToTakeSnapshot) {
            checkPositive(commitCountToTakeSnapshot, "commit count to take snapshot must be positive!");
            this.commitCountToTakeSnapshot = commitCountToTakeSnapshot;
            return this;
        }

        /**
         * @see RaftConfig#maxUncommittedLogEntryCount
         */
        public RaftConfigBuilder setMaxUncommittedLogEntryCount(int maxUncommittedLogEntryCount) {
            checkPositive(maxUncommittedLogEntryCount, "max uncommitted entry count to reject new appends must be positive!");
            this.maxUncommittedLogEntryCount = maxUncommittedLogEntryCount;
            return this;
        }

        /**
         * @see RaftConfig#leaderHeartbeatTimeoutMillis
         */
        public RaftConfigBuilder setLeaderHeartbeatTimeoutMillis(long leaderHeartbeatTimeoutMillis) {
            checkPositive(leaderHeartbeatTimeoutMillis, "leader heartbeat timeout millis must be positive!");
            this.leaderHeartbeatTimeoutMillis = leaderHeartbeatTimeoutMillis;
            return this;
        }

        /**
         * @see RaftConfig#leaderBackoffDurationMillis
         */
        public RaftConfigBuilder setLeaderBackoffDurationMillis(long leaderBackoffDurationMillis) {
            checkPositive(leaderBackoffDurationMillis, "leader backoff duration millis must be positive!");
            this.leaderBackoffDurationMillis = leaderBackoffDurationMillis;
            return this;
        }

        public RaftConfigBuilder setRaftNodeReportPublishPeriodSecs(int raftNodeReportPublishPeriodSecs) {
            checkPositive(raftNodeReportPublishPeriodSecs, "raft node state snapshot publish period seconds must be positive!");
            this.raftNodeReportPublishPeriodSecs = raftNodeReportPublishPeriodSecs;
            return this;
        }

        /**
         * Builds the Raft config object
         */
        public RaftConfig build() {
            if (leaderHeartbeatTimeoutMillis < leaderHeartbeatPeriodMillis) {
                throw new IllegalArgumentException("leader heartbeat timeout millis: " + leaderHeartbeatTimeoutMillis
                        + " cannot be smaller than leader heartbeat timeout period millis: " + leaderHeartbeatPeriodMillis);
            }

            return new RaftConfig(leaderElectionTimeoutMillis, leaderHeartbeatPeriodMillis, leaderHeartbeatTimeoutMillis,
                    appendEntriesRequestBatchSize, commitCountToTakeSnapshot, maxUncommittedLogEntryCount,
                    leaderBackoffDurationMillis, raftNodeReportPublishPeriodSecs);
        }

    }

}
