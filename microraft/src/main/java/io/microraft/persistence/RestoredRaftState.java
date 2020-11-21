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

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotEntry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Contains restored state of a {@link RaftNode}. All the fields in this class are persisted via {@link RaftStore}.
 */
public final class RestoredRaftState {

    private final RaftEndpoint localEndpoint;
    private final boolean localEndpointVoting;
    private final RaftGroupMembersView initialGroupMembers;
    private final int term;
    private final RaftEndpoint votedMember;
    private final SnapshotEntry snapshotEntry;
    private final List<LogEntry> entries;

    public RestoredRaftState(@Nonnull RaftEndpoint localEndpoint, boolean localEndpointVoting,
                             @Nonnull RaftGroupMembersView initialGroupMembers, int term, @Nullable RaftEndpoint votedMember,
                             @Nullable SnapshotEntry snapshotEntry, @Nonnull List<LogEntry> entries) {
        this.localEndpoint = localEndpoint;
        this.localEndpointVoting = localEndpointVoting;
        this.initialGroupMembers = initialGroupMembers;
        this.term = term;
        this.votedMember = votedMember;
        this.snapshotEntry = snapshotEntry;
        this.entries = entries;
    }

    @Nonnull public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    public boolean isLocalEndpointVoting() {
        return localEndpointVoting;
    }

    @Nonnull public RaftGroupMembersView getInitialGroupMembers() {
        return initialGroupMembers;
    }

    public int getTerm() {
        return term;
    }

    @Nullable public RaftEndpoint getVotedMember() {
        return votedMember;
    }

    @Nullable public SnapshotEntry getSnapshotEntry() {
        return snapshotEntry;
    }

    @Nonnull public List<LogEntry> getLogEntries() {
        return entries;
    }

}
