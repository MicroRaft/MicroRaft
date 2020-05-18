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

package io.microraft.model.impl.log;

import io.microraft.RaftEndpoint;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public class DefaultSnapshotEntry
        extends AbstractLogEntry
        implements SnapshotEntry {

    private long groupMembersLogIndex;
    private Collection<RaftEndpoint> groupMembers;

    private DefaultSnapshotEntry() {
    }

    @Override
    public int getSnapshotChunkCount() {
        return ((List<SnapshotChunk>) getOperation()).size();
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
        return "DefaultSnapshotEntry{" + "term=" + term + ", index=" + index + ", operation=" + operation
                + ", groupMembersLogIndex=" + groupMembersLogIndex + ", groupMembers=" + groupMembers + '}';
    }

    public static final class DefaultSnapshotEntryBuilder
            implements SnapshotEntryBuilder {

        private DefaultSnapshotEntry entry = new DefaultSnapshotEntry();

        @Nonnull
        @Override
        public SnapshotEntryBuilder setIndex(long index) {
            entry.index = index;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotEntryBuilder setTerm(int term) {
            entry.term = term;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotEntryBuilder setSnapshotChunks(@Nonnull List<SnapshotChunk> snapshotChunks) {
            requireNonNull(snapshotChunks);
            entry.operation = snapshotChunks;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotEntryBuilder setGroupMembersLogIndex(long groupMembersLogIndex) {
            entry.groupMembersLogIndex = groupMembersLogIndex;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotEntryBuilder setGroupMembers(@Nonnull Collection<RaftEndpoint> groupMembers) {
            requireNonNull(groupMembers);
            entry.groupMembers = groupMembers;
            return this;
        }

        @Nonnull
        @Override
        public SnapshotEntry build() {
            if (this.entry == null) {
                throw new IllegalStateException("SnapshotEntry already built!");
            }

            SnapshotEntry entry = this.entry;
            this.entry = null;
            return entry;
        }

    }

}
