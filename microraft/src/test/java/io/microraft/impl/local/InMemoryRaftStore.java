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

import io.microraft.RaftEndpoint;
import io.microraft.impl.log.RaftLog;
import io.microraft.model.impl.log.DefaultSnapshotEntryOrBuilder;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.persistence.RaftStore;
import io.microraft.persistence.RestoredRaftState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Comparator.comparingInt;

/**
 * A very simple in-memory {@link RaftStore} implementation used for testing.
 */
public final class InMemoryRaftStore implements RaftStore {

    private RaftEndpoint localEndpoint;
    private boolean localEndpointVoting;
    private RaftGroupMembersView initialGroupMembers;
    private int term;
    private RaftEndpoint votedFor;
    private RaftLog raftLog;
    private List<SnapshotChunk> snapshotChunks = new ArrayList<>();

    public InMemoryRaftStore(int logCapacity) {
        this.raftLog = RaftLog.create(logCapacity);
    }

    @Override
    public void persistAndFlushLocalEndpoint(RaftEndpoint localEndpoint, boolean localEndpointVoting) {
        this.localEndpoint = localEndpoint;
        this.localEndpointVoting = localEndpointVoting;
    }

    @Override
    public synchronized void persistAndFlushInitialGroupMembers(@Nonnull RaftGroupMembersView initialGroupMembers) {
        this.initialGroupMembers = initialGroupMembers;
    }

    @Override
    public synchronized void persistAndFlushTerm(int term, @Nullable RaftEndpoint votedFor) {
        this.term = term;
        this.votedFor = votedFor;
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

        return new RestoredRaftState(localEndpoint, localEndpointVoting, initialGroupMembers, term, votedFor,
                raftLog.snapshotEntry(), entries);
    }

}
