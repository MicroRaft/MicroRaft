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

package io.microraft.impl.model.log;

import io.microraft.RaftEndpoint;
import io.microraft.model.log.SnapshotChunk;

import javax.annotation.Nonnull;
import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public class DefaultSnapshotChunk
        implements SnapshotChunk {

    private int term;
    private long index;
    private Object operation;
    private int chunkIndex;
    private int chunkCount;
    private long groupMembersLogIndex;
    private Collection<RaftEndpoint> groupMembers;

    @Override
    public long getIndex() {
        return index;
    }

    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public Object getOperation() {
        return operation;
    }

    @Override
    public int getSnapshotChunkIndex() {
        return chunkIndex;
    }

    @Override
    public int getSnapshotChunkCount() {
        return chunkCount;
    }

    @Override
    public long getGroupMembersLogIndex() {
        return groupMembersLogIndex;
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getGroupMembers() {
        return groupMembers;
    }

    @Override
    public String toString() {
        return "DefaultSnapshotChunk{" + "term=" + term + ", index=" + index + ", operation=" + operation + ", chunkIndex="
                + chunkIndex + ", chunkCount=" + chunkCount + ", groupMembersLogIndex=" + groupMembersLogIndex + ", groupMembers="
                + groupMembers + '}';
    }

    public static class DefaultSnapshotChunkBuilder
            implements SnapshotChunkBuilder {

        private DefaultSnapshotChunk chunk = new DefaultSnapshotChunk();

        @Nonnull
        @Override
        public SnapshotChunkBuilder setIndex(long index) {
            chunk.index = index;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotChunkBuilder setTerm(int term) {
            chunk.term = term;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotChunkBuilder setOperation(@Nonnull Object operation) {
            chunk.operation = operation;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotChunkBuilder setSnapshotChunkIndex(int snapshotChunkIndex) {
            chunk.chunkIndex = snapshotChunkIndex;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotChunkBuilder setSnapshotChunkCount(int snapshotChunkCount) {
            chunk.chunkCount = snapshotChunkCount;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotChunkBuilder setGroupMembersLogIndex(long groupMembersLogIndex) {
            chunk.groupMembersLogIndex = groupMembersLogIndex;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotChunkBuilder setGroupMembers(@Nonnull Collection<RaftEndpoint> groupMembers) {
            requireNonNull(groupMembers);
            chunk.groupMembers = groupMembers;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotChunk build() {
            if (this.chunk == null) {
                throw new IllegalStateException("SnapshotChunk already built!");
            }

            SnapshotChunk chunk = this.chunk;
            this.chunk = null;
            return chunk;
        }

    }

}
