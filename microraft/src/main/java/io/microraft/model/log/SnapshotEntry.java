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

package io.microraft.model.log;

import io.microraft.RaftEndpoint;
import io.microraft.statemachine.StateMachine;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Represents a snapshot in the Raft log.
 * <p>
 * A snapshot entry is also placed on a Raft log index, just like a regular log
 * entry, but instead of user-provided operations present in log entries,
 * a snapshot entry contains objects that are returned from
 * {@link StateMachine#takeSnapshot(long, Consumer)}. Additionally, a snapshot
 * entry contains the committed Raft group member list along with its commit
 * index at the time of the snapshot creation.
 */
public interface SnapshotEntry
        extends BaseLogEntry {

    static boolean isNonInitial(@Nullable SnapshotEntry snapshotEntry) {
        return snapshotEntry != null && snapshotEntry.getIndex() > 0;
    }

    int getSnapshotChunkCount();

    long getGroupMembersLogIndex();

    @Nonnull
    Collection<RaftEndpoint> getGroupMembers();

    /**
     * The builder interface for {@link SnapshotEntry}.
     */
    interface SnapshotEntryBuilder {

        @Nonnull
        SnapshotEntryBuilder setIndex(long index);

        @Nonnull
        SnapshotEntryBuilder setTerm(int term);

        @Nonnull
        SnapshotEntryBuilder setSnapshotChunks(@Nonnull List<SnapshotChunk> snapshotChunks);

        @Nonnull
        SnapshotEntryBuilder setGroupMembersLogIndex(long groupMembersLogIndex);

        @Nonnull
        SnapshotEntryBuilder setGroupMembers(@Nonnull Collection<RaftEndpoint> groupMembers);

        @Nonnull
        SnapshotEntry build();

    }

}
