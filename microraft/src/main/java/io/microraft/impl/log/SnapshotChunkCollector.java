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

package io.microraft.impl.log;

import io.microraft.RaftEndpoint;
import io.microraft.impl.handler.InstallSnapshotRequestHandler;
import io.microraft.impl.handler.InstallSnapshotResponseHandler;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.log.SnapshotEntry.SnapshotEntryBuilder;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.model.message.InstallSnapshotResponse;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;

/**
 * Collects received snapshot chunks during a snapshot installation process.
 * <p>
 * Snapshot chunks can be added in any order.
 *
 * @author metanet
 * @see InstallSnapshotRequest
 * @see InstallSnapshotResponse
 * @see InstallSnapshotRequestHandler
 * @see InstallSnapshotResponseHandler
 */
public class SnapshotChunkCollector {

    private final long snapshotIndex;
    private final int snapshotTerm;
    private final int chunkCount;
    private final List<SnapshotChunk> chunks = new ArrayList<>();
    private final Set<Integer> missingChunkIndices = new LinkedHashSet<>();
    private long groupMembersLogIndex;
    private Collection<RaftEndpoint> groupMembers;

    public SnapshotChunkCollector(long snapshotIndex, int snapshotTerm, int chunkCount, long groupMembersLogIndex,
                                  Collection<RaftEndpoint> groupMembers) {
        this.snapshotIndex = snapshotIndex;
        this.snapshotTerm = snapshotTerm;
        this.chunkCount = chunkCount;
        this.groupMembersLogIndex = groupMembersLogIndex;
        this.groupMembers = groupMembers;
        IntStream.range(0, chunkCount).forEach(missingChunkIndices::add);
    }

    public List<SnapshotChunk> add(@Nonnull List<SnapshotChunk> receivedChunks) {
        requireNonNull(receivedChunks);

        if (missingChunkIndices.isEmpty()) {
            return Collections.emptyList();
        }

        List<SnapshotChunk> added = new ArrayList<>();

        for (SnapshotChunk chunk : receivedChunks) {
            if (snapshotIndex != chunk.getIndex()) {
                throw new IllegalArgumentException("Invalid chunk: " + chunk + " for log index: " + snapshotIndex);
            } else if (missingChunkIndices.remove(chunk.getSnapshotChunkIndex())) {
                chunks.add(chunk);
                added.add(chunk);
            }
        }

        if (missingChunkIndices.isEmpty()) {
            chunks.sort(comparingInt(SnapshotChunk::getSnapshotChunkIndex));
        }

        return added;
    }

    public List<Integer> getNextChunks(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("Invalid limit: " + limit);
        } else if (isSnapshotCompleted()) {
            return Collections.emptyList();
        }

        // We circle through missing chunk indices so that we can return
        // different chunk ids to the leader on each InstallSnapshotRequest
        List<Integer> chunkIndices = new ArrayList<>();
        Iterator<Integer> it = missingChunkIndices.iterator();
        while (it.hasNext()) {
            chunkIndices.add(it.next());
            it.remove();
            if (chunkIndices.size() == limit) {
                break;
            }
        }

        missingChunkIndices.addAll(chunkIndices);

        return chunkIndices;
    }

    public boolean isSnapshotCompleted() {
        return missingChunkIndices.isEmpty();
    }

    public long getSnapshotIndex() {
        return snapshotIndex;
    }

    public int getSnapshotTerm() {
        return snapshotTerm;
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public long getGroupMembersLogIndex() {
        return groupMembersLogIndex;
    }

    public Collection<RaftEndpoint> getGroupMembers() {
        return groupMembers;
    }

    public List<SnapshotChunk> getChunks() {
        return chunks;
    }

    public SnapshotEntry buildSnapshotEntry(SnapshotEntryBuilder builder) {
        if (missingChunkIndices.size() > 0) {
            throw new IllegalStateException(
                    "Cannot build snapshot entry because there are missing snapshot chunks: " + missingChunkIndices
                            + " for snapshot index: " + snapshotIndex + " and snapshot chunk count: " + chunkCount);
        }

        return builder.setTerm(snapshotTerm).setIndex(snapshotIndex).setGroupMembersLogIndex(groupMembersLogIndex)
                      .setGroupMembers(groupMembers).setSnapshotChunks(chunks).build();
    }

}
