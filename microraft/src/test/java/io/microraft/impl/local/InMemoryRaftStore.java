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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

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

    private static class SnapshotPersistenceState {

        final int term;
        final long snapshotIndex;
        final int chunkCount;
        final RaftGroupMembersView membersView;
        final NavigableMap<Integer, SnapshotChunk> chunks = new TreeMap<>();

        SnapshotPersistenceState(int term, long snapshotIndex, int chunkCount, RaftGroupMembersView membersView) {
            this.term = term;
            this.snapshotIndex = snapshotIndex;
            this.chunkCount = chunkCount;
            this.membersView = membersView;
        }

        boolean isCompleted() {
            return chunks.size() == chunkCount;
        }

        SnapshotEntry toSnapshotEntry() {
            if (isCompleted()) {
                return new DefaultSnapshotEntryOrBuilder().setTerm(term).setIndex(snapshotIndex)
                        .setSnapshotChunks(new ArrayList<>(chunks.values())).setGroupMembersView(membersView).build();
            }

            return null;
        }

    }

    private RaftEndpointPersistentState localEndpointPersistentState;
    private RaftGroupMembersView initialGroupMembers;
    private RaftTermPersistentState termPersistentState;
    private List<LogEntry> entries = new ArrayList<>();
    private SnapshotPersistenceState snapshotPersistenceState;
    private SnapshotEntry flushedSnapshotEntry;

    public InMemoryRaftStore() {
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
    public synchronized void persistLogEntries(@Nonnull List<LogEntry> logEntries) {
        entries.addAll(logEntries);
    }

    @Override
    public synchronized void persistSnapshotChunk(@Nonnull SnapshotChunk snapshotChunk) {
        if (snapshotPersistenceState == null || snapshotPersistenceState.snapshotIndex != snapshotChunk.getIndex()) {
            snapshotPersistenceState = new SnapshotPersistenceState(snapshotChunk.getTerm(), snapshotChunk.getIndex(),
                    snapshotChunk.getSnapshotChunkCount(), snapshotChunk.getGroupMembersView());
        }

        snapshotPersistenceState.chunks.put(snapshotChunk.getSnapshotChunkIndex(), snapshotChunk);
    }

    @Override
    public synchronized void truncateLogEntriesFrom(long logIndexInclusive) {
        List<LogEntry> newEntries = new ArrayList<>();
        for (LogEntry entry : entries) {
            if (entry.getIndex() < logIndexInclusive) {
                newEntries.add(entry);
            }
        }
        entries = newEntries;
    }

    @Override
    public synchronized void truncateLogEntriesUntil(long logIndexInclusive) throws IOException {
        List<LogEntry> newEntries = new ArrayList<>();
        for (LogEntry entry : entries) {
            if (entry.getIndex() > logIndexInclusive) {
                newEntries.add(entry);
            }
        }
        entries = newEntries;
    }

    @Override
    public synchronized void deleteSnapshotChunks(long logIndex, int snapshotChunkCount) {
        if (snapshotPersistenceState != null && snapshotPersistenceState.snapshotIndex == logIndex) {
            snapshotPersistenceState = null;
        }
    }

    @Override
    public synchronized void flush() {
        if (snapshotPersistenceState != null) {
            SnapshotEntry entry = snapshotPersistenceState.toSnapshotEntry();
            if (entry != null) {
                flushedSnapshotEntry = entry;
            }
        }
    }

    public synchronized RestoredRaftState toRestoredRaftState() {
        List<LogEntry> restoredEntries = new ArrayList<>();
        for (LogEntry entry : entries) {
            if (flushedSnapshotEntry == null || entry.getIndex() > flushedSnapshotEntry.getIndex()) {
                restoredEntries.add(entry);
            }
        }

        return new RestoredRaftState(localEndpointPersistentState, initialGroupMembers, termPersistentState,
                flushedSnapshotEntry, restoredEntries);
    }

}
