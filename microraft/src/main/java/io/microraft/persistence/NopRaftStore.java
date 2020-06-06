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
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.SnapshotChunk;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Used when a Raft node works transiently (its state is not persisted).
 */
public class NopRaftStore
        implements RaftStore {

    @Override
    public void persistInitialMembers(@Nonnull RaftEndpoint localEndpoint, @Nonnull Collection<RaftEndpoint> initialMembers) {
    }

    @Override
    public void persistTerm(int term, @Nullable RaftEndpoint votedFor) {
    }

    @Override
    public void persistLogEntry(@Nonnull LogEntry logEntry) {
    }

    @Override
    public void persistSnapshotChunk(@Nonnull SnapshotChunk snapshotChunk) {
    }

    @Override
    public void truncateLogEntriesFrom(long logIndexInclusive) {
    }

    @Override
    public void truncateSnapshotChunksUntil(long logIndexInclusive) {
    }

    @Override
    public void flush() {
    }

}
