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


package io.microraft.impl.log;

import io.microraft.RaftEndpoint;
import io.microraft.impl.handler.InstallSnapshotRequestHandler;
import io.microraft.impl.handler.InstallSnapshotResponseHandler;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.log.SnapshotEntry.SnapshotEntryBuilder;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.model.message.InstallSnapshotResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
public final class SnapshotChunkCollector {

    private final long snapshotIndex;
    private final int snapshotTerm;
    private final int chunkCount;
    private final Collection<RaftEndpoint> snapshottedMembers = new HashSet<>();
    private final List<SnapshotChunk> chunks = new ArrayList<>();
    private final Set<Integer> missingChunkIndices = new LinkedHashSet<>();
    private long groupMembersLogIndex;
    private Collection<RaftEndpoint> groupMembers;
    private Map<RaftEndpoint, Integer> requestedMembers = new HashMap<>();
    private Set<RaftEndpoint> unresponsiveMembers = new HashSet<>();

    public SnapshotChunkCollector(InstallSnapshotRequest request) {
        this(request.getSnapshotIndex(), request.getSnapshotTerm(), request.getTotalSnapshotChunkCount(),
             request.getSnapshottedMembers(), request.getGroupMembersLogIndex(), request.getGroupMembers());
    }

    private SnapshotChunkCollector(long snapshotIndex, int snapshotTerm, int chunkCount,
                                   Collection<RaftEndpoint> snapshottedMembers, long groupMembersLogIndex,
                                   Collection<RaftEndpoint> groupMembers) {
        this.snapshotIndex = snapshotIndex;
        this.snapshotTerm = snapshotTerm;
        this.chunkCount = chunkCount;
        this.snapshottedMembers.addAll(snapshottedMembers);
        this.groupMembersLogIndex = groupMembersLogIndex;
        this.groupMembers = groupMembers;
        IntStream.range(0, chunkCount).forEach(missingChunkIndices::add);
    }

    public void updateSnapshottedMembers(Collection<RaftEndpoint> snapshottedMembers) {
        if (snapshottedMembers == null || snapshottedMembers.isEmpty()) {
            return;
        }

        this.snapshottedMembers.clear();
        this.snapshottedMembers.addAll(snapshottedMembers);
        this.requestedMembers.keySet().retainAll(snapshottedMembers);
        this.unresponsiveMembers.retainAll(snapshottedMembers);
    }

    public boolean handleReceivedSnapshotChunk(RaftEndpoint endpoint, long snapshotIndex, SnapshotChunk snapshotChunk) {
        requireNonNull(endpoint);
        if (this.snapshotIndex != snapshotIndex) {
            throw new IllegalArgumentException(
                    "Invalid snapshot chunk at snapshot index: " + snapshotIndex + " current snapshot index: "
                            + this.snapshotIndex);
        }

        // TODO [basri] exponential backoff maybe?

        // Un-mark the unresponsive endpoint even if the given chunk is already here
        unresponsiveMembers.remove(endpoint);

        if (snapshotChunk == null || !missingChunkIndices.remove(snapshotChunk.getSnapshotChunkIndex())) {
            return false;
        }

        chunks.add(snapshotChunk);
        requestedMembers.remove(endpoint, snapshotChunk.getSnapshotChunkIndex());

        if (missingChunkIndices.isEmpty()) {
            chunks.sort(comparingInt(SnapshotChunk::getSnapshotChunkIndex));
        }

        return true;
    }

    public Map<RaftEndpoint, Integer> requestSnapshotChunks(boolean trackRequests) {
        if (isSnapshotCompleted()) {
            return Collections.emptyMap();
        }

        Map<RaftEndpoint, Integer> requestedSnapshotChunkIndices = new HashMap<>();
        for (RaftEndpoint endpoint : snapshottedMembers) {
            if (requestedMembers.containsKey(endpoint) || unresponsiveMembers.contains(endpoint)) {
                continue;
            }

            if (trackRequests) {
                for (int chunkIndex : missingChunkIndices) {
                    if (!requestedMembers.containsValue(chunkIndex)) {
                        requestedMembers.put(endpoint, chunkIndex);
                        requestedSnapshotChunkIndices.put(endpoint, chunkIndex);
                        break;
                    }
                }
            } else {
                Iterator<Integer> it = missingChunkIndices.iterator();
                int chunkIndex = it.next();
                requestedSnapshotChunkIndices.put(endpoint, chunkIndex);
                it.remove();
                missingChunkIndices.add(chunkIndex);
            }
        }

        return requestedSnapshotChunkIndices;
    }

    public boolean cancelSnapshotChunkRequest(RaftEndpoint endpoint, int snapshotChunkIndex) {
        // Don't mark the endpoint as unresponsive if I already sent another request.
        if (requestedMembers.remove(endpoint, snapshotChunkIndex)) {
            unresponsiveMembers.add(endpoint);
            return true;
        }

        return false;
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

    // for testing
    public SnapshotChunkCollector copy() {
        SnapshotChunkCollector copy = new SnapshotChunkCollector(snapshotIndex, snapshotTerm, chunkCount, snapshottedMembers,
                                                                 groupMembersLogIndex, groupMembers);
        copy.chunks.addAll(chunks);
        copy.missingChunkIndices.addAll(missingChunkIndices);
        copy.unresponsiveMembers.addAll(unresponsiveMembers);
        copy.requestedMembers.putAll(requestedMembers);

        return copy;
    }

    // for testing
    public Collection<RaftEndpoint> getSnapshottedMembers() {
        return snapshottedMembers;
    }

}
