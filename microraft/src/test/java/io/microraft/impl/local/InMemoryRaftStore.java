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

package io.microraft.impl.local;

import static java.util.Comparator.comparingInt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import io.microraft.impl.log.RaftLog;
import io.microraft.model.impl.log.DefaultSnapshotEntryOrBuilder;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;

/**
 * A very simple in-memory {@link RaftStore} implementation used for testing.
 */
public final class InMemoryRaftStore implements RaftStore {

    private RaftEndpointPersistentState localEndpointPersistentState;
    private RaftGroupMembersView initialGroupMembers;
    private RaftTermPersistentState termPersistentState;
    private RaftLog raftLog;
    private List<SnapshotChunk> snapshotChunks = new ArrayList<>();

    public InMemoryRaftStore(int logCapacity) {
        this.raftLog = RaftLog.create(logCapacity);
    }

    @Override
    public void persistAndFlushLocalEndpoint(@Nonnull RaftEndpointPersistentState localEndpointPersistentState) {
        this.localEndpointPersistentState = localEndpointPersistentState;
    }

    @Override
    public synchronized void persistAndFlushInitialGroupMembers(@Nonnull RaftGroupMembersView initialGroupMembers) {
        this.initialGroupMembers = initialGroupMembers;
    }

    @Override
    public synchronized void persistAndFlushTerm(@Nonnull RaftTermPersistentState termPersistentState) {
        this.termPersistentState = termPersistentState;
    }

    @Override
    public synchronized void persistLogEntry(@Nonnull LogEntry logEntry) {
        raftLog.appendEntry(logEntry);
    }

    @Override
    public synchronized void persistSnapshotChunk(@Nonnull SnapshotChunk snapshotChunk) {
        snapshotChunks.add(snapshotChunk);

        if (snapshotChunk.getSnapshotChunkCount() == snapshotChunks.size()) {
            snapshotChunks.sort(comparingInt(SnapshotChunk::getSnapshotChunkIndex));
            SnapshotEntry snapshotEntry = new DefaultSnapshotEntryOrBuilder().setTerm(snapshotChunk.getTerm())
                    .setIndex(snapshotChunk.getIndex()).setSnapshotChunks(snapshotChunks)
                    .setGroupMembersView(snapshotChunk.getGroupMembersView()).build();
            raftLog.setSnapshot(snapshotEntry);
            snapshotChunks = new ArrayList<>();
        }
    }

    @Override
    public synchronized void truncateLogEntriesFrom(long logIndexInclusive) {
        raftLog.truncateEntriesFrom(logIndexInclusive);
    }

    @Override
    public synchronized void deleteSnapshotChunks(long logIndex, int snapshotChunkCount) {
        snapshotChunks.clear();
    }

    @Override
    public synchronized void flush() {
    }

    public synchronized RestoredRaftState toRestoredRaftState() {
        List<LogEntry> entries;
        if (raftLog.snapshotIndex() < raftLog.lastLogOrSnapshotIndex()) {
            entries = raftLog.getLogEntriesBetween(raftLog.snapshotIndex() + 1, raftLog.lastLogOrSnapshotIndex());
        } else {
            entries = Collections.emptyList();
        }

        return new RestoredRaftState(localEndpointPersistentState, initialGroupMembers, termPersistentState,
                raftLog.snapshotEntry(), entries);
    }

}
