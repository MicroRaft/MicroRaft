/*
 * Copyright (c) 2020, MicroRaft.
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

/**
 * Contains the YAML config field names to populate {@link RaftConfig}
 */
public final class YamlRaftConfigFields {

    /**
     * Container object name for RaftConfig fields
     */
    public static final String RAFT_CONFIG_CONTAINER_NAME = "raft";

    /**
     * Field name of {@link RaftConfig#getLeaderElectionTimeoutMillis()}
     */
    public static final String LEADER_ELECTION_TIMEOUT_MILLIS_FIELD_NAME = "leader-election-timeout-millis";

    /**
     * Field name of {@link RaftConfig#getLeaderHeartbeatPeriodSecs()}
     */
    public static final String LEADER_HEARTBEAT_PERIOD_SECS_FIELD_NAME = "leader-heartbeat-period-secs";

    /**
     * Field name of {@link RaftConfig#getLeaderElectionTimeoutMillis()}
     */
    public static final String LEADER_HEARTBEAT_TIMEOUT_SECS_FIELD_NAME = "leader-heartbeat-timeout-secs";

    /**
     * Field name of {@link RaftConfig#getAppendEntriesRequestBatchSize()}
     */
    public static final String APPEND_ENTRIES_REQUEST_BATCH_SIZE_FIELD_NAME = "append-entries-request-batch-size";

    /**
     * Field name of {@link RaftConfig#getCommitCountToTakeSnapshot()}
     */
    public static final String COMMIT_COUNT_TO_TAKE_SNAPSHOT_FIELD_NAME = "commit-count-to-take-snapshot";

    /**
     * Field name of {@link RaftConfig#getMaxPendingLogEntryCount()}
     */
    public static final String MAX_PENDING_LOG_ENTRY_COUNT_FIELD_NAME = "max-pending-log-entry-count";

    /**
     * Field name of {@link RaftConfig#isTransferSnapshotsFromFollowersEnabled()}
     */
    public static final String TRANSFER_SNAPSHOTS_FROM_FOLLOWERS_ENABLED_FIELD_NAME = "transfer-snapshots-from-followers-enabled";

    /**
     * Field name of {@link RaftConfig#getRaftNodeReportPublishPeriodSecs()}
     */
    public static final String RAFT_NODE_REPORT_PUBLISH_PERIOD_SECS_FIELD_NAME = "raft-node-report-publish-period-secs";

    private YamlRaftConfigFields() {
    }

}
