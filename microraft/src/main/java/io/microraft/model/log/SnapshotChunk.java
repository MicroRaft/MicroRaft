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

import io.microraft.statemachine.StateMachine;

import javax.annotation.Nonnull;
import java.util.function.Consumer;

/**
 * Represents a snapshot chunk.
 * <p>
 * A snapshot entry in the Raft log contains at least one snapshot chunk.
 * <p>
 * Snapshot chunks are ordered by snapshot chunk indices. They contain
 * objects provided to the consumer argument of
 * {@link StateMachine#takeSnapshot(long, Consumer)}. Additionally, a snapshot
 * chunk contains the committed Raft group member list along with its commit
 * index at the time of the snapshot creation.
 */
public interface SnapshotChunk
        extends BaseLogEntry {

    int getSnapshotChunkIndex();

    int getSnapshotChunkCount();

    @Nonnull RaftGroupMembersView getGroupMembersView();


    interface SnapshotChunkBuilder {

        @Nonnull
        SnapshotChunkBuilder setIndex(long index);

        @Nonnull
        SnapshotChunkBuilder setTerm(int term);

        @Nonnull
        SnapshotChunkBuilder setOperation(@Nonnull Object operation);

        @Nonnull
        SnapshotChunkBuilder setSnapshotChunkIndex(int snapshotChunkIndex);

        @Nonnull
        SnapshotChunkBuilder setSnapshotChunkCount(int snapshotChunkCount);

        @Nonnull SnapshotChunkBuilder setGroupMembersView(@Nonnull RaftGroupMembersView groupMembersView);

        @Nonnull
        SnapshotChunk build();

    }

}
