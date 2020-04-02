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

package io.microraft.impl.report;

import io.microraft.model.log.BaseLogEntry;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.report.RaftLogStats;

import static java.util.Objects.requireNonNull;

/**
 * Contains statistics about a Raft node's local Raft log.
 *
 * @author metanet
 */
public final class RaftLogStatsImpl
        implements RaftLogStats {

    private final long commitIndex;
    private final long lastLogOrSnapshotTerm;
    private final long lastLogOrSnapshotIndex;
    private final long snapshotTerm;
    private final long snapshotIndex;
    private final int takeSnapshotCount;
    private final int installSnapshotCount;

    public RaftLogStatsImpl(long commitIndex, BaseLogEntry lastLogOrSnapshotEntry, SnapshotEntry snapshotEntry,
                            int takeSnapshotCount, int installSnapshotCount) {
        requireNonNull(lastLogOrSnapshotEntry);
        requireNonNull(snapshotEntry);
        this.commitIndex = commitIndex;
        this.lastLogOrSnapshotTerm = lastLogOrSnapshotEntry.getTerm();
        this.lastLogOrSnapshotIndex = lastLogOrSnapshotEntry.getIndex();
        this.snapshotTerm = snapshotEntry.getTerm();
        this.snapshotIndex = snapshotEntry.getIndex();
        this.takeSnapshotCount = takeSnapshotCount;
        this.installSnapshotCount = installSnapshotCount;
    }

    @Override
    public long getCommitIndex() {
        return commitIndex;
    }

    @Override
    public long getLastLogOrSnapshotTerm() {
        return lastLogOrSnapshotTerm;
    }

    @Override
    public long getLastLogOrSnapshotIndex() {
        return lastLogOrSnapshotIndex;
    }

    @Override
    public long getLastSnapshotTerm() {
        return snapshotTerm;
    }

    @Override
    public long getLastSnapshotIndex() {
        return snapshotIndex;
    }

    @Override
    public int getTakeSnapshotCount() {
        return takeSnapshotCount;
    }

    @Override
    public int getInstallSnapshotCount() {
        return installSnapshotCount;
    }

    @Override
    public String toString() {
        return "RaftLogReport{" + "commitIndex=" + commitIndex + ", lastLogOrSnapshotTerm=" + lastLogOrSnapshotTerm
                + ", lastLogOrSnapshotIndex=" + lastLogOrSnapshotIndex + ", snapshotTerm=" + snapshotTerm + ", snapshotIndex="
                + snapshotIndex + ", takeSnapshotCount=" + takeSnapshotCount + ", installSnapshotCount=" + installSnapshotCount
                + '}';
    }

}
