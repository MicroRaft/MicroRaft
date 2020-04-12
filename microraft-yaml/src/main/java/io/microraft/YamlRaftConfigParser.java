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

import io.microraft.RaftConfig.RaftConfigBuilder;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.microraft.YamlRaftConfigFields.APPEND_ENTRIES_REQUEST_BATCH_SIZE_FIELD_NAME;
import static io.microraft.YamlRaftConfigFields.COMMIT_COUNT_TO_TAKE_SNAPSHOT_FIELD_NAME;
import static io.microraft.YamlRaftConfigFields.LEADER_ELECTION_TIMEOUT_MILLIS_FIELD_NAME;
import static io.microraft.YamlRaftConfigFields.LEADER_HEARTBEAT_PERIOD_SECS_FIELD_NAME;
import static io.microraft.YamlRaftConfigFields.LEADER_HEARTBEAT_TIMEOUT_SECS_FIELD_NAME;
import static io.microraft.YamlRaftConfigFields.MAX_UNCOMMITTED_LOG_ENTRY_COUNT_FIELD_NAME;
import static io.microraft.YamlRaftConfigFields.RAFT_CONFIG_CONTAINER_NAME;
import static io.microraft.YamlRaftConfigFields.RAFT_NODE_REPORT_PUBLISH_PERIOD_SECS_FIELD_NAME;
import static io.microraft.YamlRaftConfigFields.TRANSFER_SNAPSHOTS_FROM_FOLLOWERS_ENABLED_FIELD_NAME;
import static java.util.Objects.requireNonNull;

/**
 * {@link RaftConfig} parser for YAML files.
 */
public final class YamlRaftConfigParser {

    /*
        A sample YAML string is below:
        ---
        raft:
         leader-election-timeout-millis: 750
         leader-heartbeat-period-secs: 15
         leader-heartbeat-timeout-secs: 45
         append-entries-request-batch-size: 750
         commit-count-to-take-snapshot: 7500
         max-uncommitted-log-entry-count: 1500
         transfer-snapshots-from-followers-enabled: false
         raft-node-report-publish-period-secs: 20

     */
    private YamlRaftConfigParser() {
    }

    /**
     * Loads a parameter map from the yaml object with the given string and
     * populates a {@link RaftConfig} object from the returned parameter map.
     *
     * @return the created RaftConfig object
     *
     * @throws NullPointerException
     *         if no yaml object passed
     * @throws NullPointerException
     *         if no string passed
     * @throws NullPointerException
     *         if no RaftConfig present in the string
     * @throws ClassCastException
     *         if a configuration value has wrong type
     * @see RaftConfig
     */
    public static RaftConfig parseString(Yaml yaml, String string) {
        requireNonNull(yaml, "No yaml object!");
        requireNonNull(string, "No yaml string!");

        return parse(yaml.load(string));
    }

    /**
     * Loads a parameter map from the yaml object with the given reader and
     * populates a {@link RaftConfig} object from the returned parameter map.
     *
     * @return the created RaftConfig object
     *
     * @throws NullPointerException
     *         if no yaml object passed
     * @throws NullPointerException
     *         if no reader passed
     * @throws NullPointerException
     *         if no RaftConfig present in the reader
     * @throws ClassCastException
     *         if a configuration value has wrong type
     * @see RaftConfig
     */
    public static RaftConfig parseReader(Yaml yaml, Reader reader) {
        requireNonNull(yaml, "No yaml object!");
        requireNonNull(reader, "No reader!");

        Map<String, Object> parameters = yaml.load(reader);
        return parse(parameters);
    }

    /**
     * Loads a parameter map from the yaml object with the given file and
     * populates a {@link RaftConfig} object from the returned parameter map.
     *
     * @return the created RaftConfig object
     *
     * @throws IOException
     *         if an error occurs during reading the file
     * @throws NullPointerException
     *         if no yaml object passed
     * @throws NullPointerException
     *         if no reader passed
     * @throws NullPointerException
     *         if no RaftConfig present in the reader
     * @throws ClassCastException
     *         if a configuration value has wrong type
     * @see RaftConfig
     */
    public static RaftConfig parseFile(Yaml yaml, String filePath)
            throws IOException {
        return parseFile(yaml, new File(filePath));
    }

    /**
     * Loads a parameter map from the yaml object with the given file and
     * populates a {@link RaftConfig} object from the returned parameter map.
     *
     * @return the created RaftConfig object
     *
     * @throws IOException
     *         if an error occurs during reading the file
     * @throws NullPointerException
     *         if no yaml object passed
     * @throws NullPointerException
     *         if no reader passed
     * @throws NullPointerException
     *         if no RaftConfig present in the reader
     * @throws ClassCastException
     *         if a configuration value has wrong type
     * @see RaftConfig
     */
    public static RaftConfig parseFile(Yaml yaml, File file)
            throws IOException {
        requireNonNull(yaml, "No yaml object!");
        requireNonNull(file, "No file!");

        try (Reader reader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
            return parseReader(yaml, reader);
        }
    }

    /**
     * Loads a parameter map from the yaml object with the given stream and
     * populates a {@link RaftConfig} object from the returned parameter map.
     *
     * @return the created RaftConfig object
     *
     * @throws NullPointerException
     *         if no yaml object passed
     * @throws NullPointerException
     *         if no input stream passed
     * @throws NullPointerException
     *         if no RaftConfig present in the stream
     * @throws ClassCastException
     *         if a configuration value has wrong type
     * @see RaftConfig
     */
    public static RaftConfig parseInputStream(Yaml yaml, InputStream inputStream) {
        requireNonNull(yaml, "No yaml object!");
        requireNonNull(inputStream, "No input stream!");

        return parse(yaml.load(inputStream));
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private static RaftConfig parse(Map<String, Object> map) {
        requireNonNull(map, "RaftConfig not provided!");
        Map<String, Object> params = (Map<String, Object>) map.get(RAFT_CONFIG_CONTAINER_NAME);
        requireNonNull(params, "RaftConfig not provided!");

        RaftConfigBuilder builder = RaftConfig.newBuilder();

        Integer leaderElectionTimeoutMillis = (Integer) params.get(LEADER_ELECTION_TIMEOUT_MILLIS_FIELD_NAME);
        if (leaderElectionTimeoutMillis != null) {
            builder.setLeaderElectionTimeoutMillis(leaderElectionTimeoutMillis);
        }

        Integer leaderHeartbeatPeriodSecs = (Integer) params.get(LEADER_HEARTBEAT_PERIOD_SECS_FIELD_NAME);
        if (leaderHeartbeatPeriodSecs != null) {
            builder.setLeaderHeartbeatPeriodSecs(leaderHeartbeatPeriodSecs);
        }

        Integer leaderHeartbeatTimeoutSecs = (Integer) params.get(LEADER_HEARTBEAT_TIMEOUT_SECS_FIELD_NAME);
        if (leaderHeartbeatTimeoutSecs != null) {
            builder.setLeaderHeartbeatTimeoutSecs(leaderHeartbeatTimeoutSecs);
        }

        Integer appendEntriesRequestBatchSize = (Integer) params.get(APPEND_ENTRIES_REQUEST_BATCH_SIZE_FIELD_NAME);
        if (appendEntriesRequestBatchSize != null) {
            builder.setAppendEntriesRequestBatchSize(appendEntriesRequestBatchSize);
        }

        Integer commitCountToTakeSnapshot = (Integer) params.get(COMMIT_COUNT_TO_TAKE_SNAPSHOT_FIELD_NAME);
        if (commitCountToTakeSnapshot != null) {
            builder.setCommitCountToTakeSnapshot(commitCountToTakeSnapshot);
        }

        Integer maxUncommittedLogEntryCount = (Integer) params.get(MAX_UNCOMMITTED_LOG_ENTRY_COUNT_FIELD_NAME);
        if (maxUncommittedLogEntryCount != null) {
            builder.setMaxUncommittedLogEntryCount(maxUncommittedLogEntryCount);
        }

        Boolean transferSnapshotsFromFollowersEnabled = (Boolean) params
                .get(TRANSFER_SNAPSHOTS_FROM_FOLLOWERS_ENABLED_FIELD_NAME);
        if (transferSnapshotsFromFollowersEnabled != null) {
            builder.setTransferSnapshotsFromFollowersEnabled(transferSnapshotsFromFollowersEnabled);
        }

        Integer raftNodeReportPublishPeriodSecs = (Integer) params.get(RAFT_NODE_REPORT_PUBLISH_PERIOD_SECS_FIELD_NAME);
        if (raftNodeReportPublishPeriodSecs != null) {
            builder.setRaftNodeReportPublishPeriodSecs(raftNodeReportPublishPeriodSecs);
        }

        return builder.build();
    }

}
