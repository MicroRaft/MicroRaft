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

package io.microraft.persistence;

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.SnapshotEntry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Contains restored state of a {@link RaftNode}. All the fields in this class
 * are persisted via {@link RaftStore}.
 *
 * @author mdogan
 * @author metanet
 * @author mtopolnik
 */
public class RestoredRaftState {

    private final RaftEndpoint localEndpoint;
    private final Collection<RaftEndpoint> initialMembers;
    private final int term;
    private final RaftEndpoint votedEndpoint;
    private final SnapshotEntry snapshotEntry;
    private final List<LogEntry> entries;

    public RestoredRaftState(@Nonnull RaftEndpoint localEndpoint, @Nonnull Collection<RaftEndpoint> initialMembers, int term,
                             @Nullable RaftEndpoint votedEndpoint, @Nullable SnapshotEntry snapshotEntry,
                             @Nonnull List<LogEntry> entries) {
        this.localEndpoint = localEndpoint;
        this.initialMembers = initialMembers;
        this.term = term;
        this.votedEndpoint = votedEndpoint;
        this.snapshotEntry = snapshotEntry;
        this.entries = entries;
    }

    @Nonnull
    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    @Nonnull
    public Collection<RaftEndpoint> getInitialMembers() {
        return initialMembers;
    }

    public int getTerm() {
        return term;
    }

    @Nullable
    public RaftEndpoint getVotedEndpoint() {
        return votedEndpoint;
    }

    @Nullable
    public SnapshotEntry getSnapshotEntry() {
        return snapshotEntry;
    }

    @Nonnull
    public List<LogEntry> getLogEntries() {
        return entries;
    }

}
