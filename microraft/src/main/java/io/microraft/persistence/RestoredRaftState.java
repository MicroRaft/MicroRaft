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

package io.microraft.persistence;

import io.microraft.RaftNode;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Contains restored state of a {@link RaftNode}. All the fields in this class
 * are persisted via {@link RaftStore}.
 */
public final class RestoredRaftState {

    private final RaftEndpointPersistentState localEndpointPersistentState;
    private final RaftGroupMembersView initialGroupMembers;
    private final RaftTermPersistentState termPersistentState;
    private final SnapshotEntry snapshotEntry;
    private final List<LogEntry> entries;

    public RestoredRaftState(@Nonnull RaftEndpointPersistentState localEndpointPersistentState,
            @Nonnull RaftGroupMembersView initialGroupMembers, @Nonnull RaftTermPersistentState termPersistentState,
            @Nullable SnapshotEntry snapshotEntry, @Nonnull List<LogEntry> entries) {
        this.localEndpointPersistentState = requireNonNull(localEndpointPersistentState);
        this.initialGroupMembers = requireNonNull(initialGroupMembers);
        this.termPersistentState = termPersistentState;
        this.snapshotEntry = snapshotEntry;
        this.entries = requireNonNull(entries);
    }

    @Nonnull
    public RaftEndpointPersistentState getLocalEndpointPersistentState() {
        return localEndpointPersistentState;
    }

    @Nonnull
    public RaftGroupMembersView getInitialGroupMembers() {
        return initialGroupMembers;
    }

    @Nonnull
    public RaftTermPersistentState getTermPersistentState() {
        return termPersistentState;
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
