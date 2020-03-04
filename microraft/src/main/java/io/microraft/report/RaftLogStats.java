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

/**
 * Contains statistics about a Raft node's local Raft log.
 *
 * @author metanet
 */
public interface RaftLogStats {

    /**
     * Returns index of the highest log entry known to be committed.
     */
    long getCommitIndex();

    /**
     * Returns the last term in the Raft log, either from the last log entry
     * or from the last locally taken or installed snapshot.
     */
    long getLastLogOrSnapshotTerm();

    /**
     * Returns the last log entry index in the Raft log, either from the last
     * log entry or from the last locally taken or installed snapshot.
     */
    long getLastLogOrSnapshotIndex();

    /**
     * Returns term of the last locally taken or installed snapshot.
     */
    long getLastSnapshotTerm();

    /**
     * Returns log index of the last locally taken or installed snapshot.
     */
    long getLastSnapshotIndex();

    /**
     * Returns how many times a new snapshot is taken by a Raft node.
     */
    int getTakeSnapshotCount();

    /**
     * Returns how many times a new snapshot is installed at a Raft node.
     */
    int getInstallSnapshotCount();

}
