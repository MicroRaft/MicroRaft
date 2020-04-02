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

import com.typesafe.config.Config;
import io.microraft.RaftConfig.RaftConfigBuilder;

import javax.annotation.Nonnull;

import static com.typesafe.config.ConfigException.WrongType;
import static io.microraft.HoconRaftConfigFields.APPEND_ENTRIES_REQUEST_BATCH_SIZE_FIELD_NAME;
import static io.microraft.HoconRaftConfigFields.COMMIT_COUNT_TO_TAKE_SNAPSHOT_FIELD_NAME;
import static io.microraft.HoconRaftConfigFields.LEADER_ELECTION_TIMEOUT_MILLIS_FIELD_NAME;
import static io.microraft.HoconRaftConfigFields.LEADER_HEARTBEAT_PERIOD_SECS_FIELD_NAME;
import static io.microraft.HoconRaftConfigFields.LEADER_HEARTBEAT_TIMEOUT_SECS_FIELD_NAME;
import static io.microraft.HoconRaftConfigFields.MAX_UNCOMMITTED_LOG_ENTRY_COUNT_FIELD_NAME;
import static io.microraft.HoconRaftConfigFields.RAFT_CONFIG_CONTAINER_NAME;
import static io.microraft.HoconRaftConfigFields.RAFT_NODE_REPORT_PUBLISH_PERIOD_SECS_FIELD_NAME;
import static io.microraft.HoconRaftConfigFields.TRANSFER_SNAPSHOTS_FROM_FOLLOWERS_ENABLED_FIELD_NAME;
import static java.util.Objects.requireNonNull;

/**
 * {@link RaftConfig} parser for HOCON files
 */
public final class HoconRaftConfigParser {

    /*

        A sample HOCON string is below:
        ---
        raft {
          leader-election-timeout-millis: 1000
          leader-heartbeat-timeout-secs: 10
          leader-heartbeat-period-secs: 2
          max-uncommitted-log-entry-count: 5000
          append-entries-request-batch-size: 1000
          commit-count-to-take-snapshot: 50000
          transfer-snapshots-from-followers-enabled: false
          raft-node-report-publish-period-secs: 10
        }

     */
    private HoconRaftConfigParser() {
    }

    /**
     * Parses the given config object to populate RaftConfig
     *
     * @return the created RaftConfig object
     *
     * @throws NullPointerException
     *         if the given config object is null
     * @throws IllegalArgumentException
     *         if the given config object has no
     *         "raft.*" field
     * @throws WrongType
     *         if a configuration value has wrong type
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    public static RaftConfig parseRaftConfig(@Nonnull Config config) {
        requireNonNull(config);
        if (!config.hasPath(RAFT_CONFIG_CONTAINER_NAME)) {
            throw new IllegalArgumentException("No raft config provided!");
        }

        RaftConfigBuilder builder = RaftConfig.newBuilder();

        if (config.hasPath(LEADER_ELECTION_TIMEOUT_MILLIS_FIELD_NAME)) {
            builder.setLeaderElectionTimeoutMillis(config.getLong(LEADER_ELECTION_TIMEOUT_MILLIS_FIELD_NAME));
        }

        if (config.hasPath(LEADER_HEARTBEAT_PERIOD_SECS_FIELD_NAME)) {
            builder.setLeaderHeartbeatPeriodSecs(config.getLong(LEADER_HEARTBEAT_PERIOD_SECS_FIELD_NAME));
        }

        if (config.hasPath(LEADER_HEARTBEAT_TIMEOUT_SECS_FIELD_NAME)) {
            builder.setLeaderHeartbeatTimeoutSecs(config.getLong(LEADER_HEARTBEAT_TIMEOUT_SECS_FIELD_NAME));
        }

        if (config.hasPath(APPEND_ENTRIES_REQUEST_BATCH_SIZE_FIELD_NAME)) {
            builder.setAppendEntriesRequestBatchSize(config.getInt(APPEND_ENTRIES_REQUEST_BATCH_SIZE_FIELD_NAME));
        }

        if (config.hasPath(COMMIT_COUNT_TO_TAKE_SNAPSHOT_FIELD_NAME)) {
            builder.setCommitCountToTakeSnapshot(config.getInt(COMMIT_COUNT_TO_TAKE_SNAPSHOT_FIELD_NAME));
        }

        if (config.hasPath(MAX_UNCOMMITTED_LOG_ENTRY_COUNT_FIELD_NAME)) {
            builder.setMaxUncommittedLogEntryCount(config.getInt(MAX_UNCOMMITTED_LOG_ENTRY_COUNT_FIELD_NAME));
        }

        if (config.hasPath(TRANSFER_SNAPSHOTS_FROM_FOLLOWERS_ENABLED_FIELD_NAME)) {
            builder.setTransferSnapshotsFromFollowersEnabled(
                    config.getBoolean(TRANSFER_SNAPSHOTS_FROM_FOLLOWERS_ENABLED_FIELD_NAME));
        }

        if (config.hasPath(RAFT_NODE_REPORT_PUBLISH_PERIOD_SECS_FIELD_NAME)) {
            builder.setRaftNodeReportPublishPeriodSecs(config.getInt(RAFT_NODE_REPORT_PUBLISH_PERIOD_SECS_FIELD_NAME));
        }

        return builder.build();
    }

}
