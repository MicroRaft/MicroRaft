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

package io.microraft.report;

import io.microraft.RaftEndpoint;

import java.util.Map;

/**
 * Contains statistics about a Raft node's Raft log.
 */
public interface RaftLogStats {

    /**
     * Returns index of the highest log entry known to be committed.
     *
     * @return index of the highest log entry known to be committed
     */
    long getCommitIndex();

    /**
     * Returns the last term in the Raft log, either from the last log entry or from the last locally taken or installed
     * snapshot.
     *
     * @return the last term in the Raft log, either from the last log entry or from the last locally taken or installed
     *         snapshot
     */
    int getLastLogOrSnapshotTerm();

    /**
     * Returns the last log entry index in the Raft log, either from the last log entry or from the last locally taken
     * or installed snapshot.
     *
     * @return the last log entry index in the Raft log, either from the last log entry or from the last locally taken
     *         or installed snapshot
     */
    long getLastLogOrSnapshotIndex();

    /**
     * Returns the term of the last locally taken or installed snapshot.
     *
     * @return the term of the last locally taken or installed snapshot
     */
    int getLastSnapshotTerm();

    /**
     * Returns the log index of the last locally taken or installed snapshot.
     *
     * @return the log index of the last locally taken or installed snapshot
     */
    long getLastSnapshotIndex();

    /**
     * Returns the number of snapshots are taken by a Raft node.
     *
     * @return the number of snapshots are taken by a Raft node
     */
    int getTakeSnapshotCount();

    /**
     * Returns the number of snapshots installed by a Raft node.
     *
     * @return the number of snapshots installed by a Raft node
     */
    int getInstallSnapshotCount();

    /**
     * Returns the indices of the last known appended Raft log entries on the followers.
     * <p>
     * This map is non-empty only for the leader Raft node. Followers return an empty map.
     *
     * @return the indices of the last known appended Raft log entries on the followers
     */
    Map<RaftEndpoint, Long> getFollowerMatchIndices();

}
