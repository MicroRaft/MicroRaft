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

import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.log.SnapshotEntry.SnapshotEntryBuilder;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The default impl of the {@link SnapshotEntry} and {@link SnapshotEntryBuilder} interfaces. When an instance of this class is
 * created, it is in the builder mode and its state is populated. Once all fields are set, the object switches to the DTO mode
 * where it no longer allows mutations.
 * <p>
 * Please note that {@link #build()} does not verify if all fields are set or not. It is up to the user to populate the DTO state
 * via the builder.
 */
public class DefaultSnapshotEntryOrBuilder
        extends DefaultAbstractLogEntry
        implements SnapshotEntry, SnapshotEntryBuilder {

    private RaftGroupMembersView groupMembersView;
    private DefaultSnapshotEntryOrBuilder builder = this;

    @Override public int getSnapshotChunkCount() {
        return ((List<SnapshotChunk>) getOperation()).size();
    }

    @Nonnull @Override public RaftGroupMembersView getGroupMembersView() {
        return groupMembersView;
    }

    @Nonnull @Override public SnapshotEntryBuilder setIndex(long index) {
        builder.index = index;
        return this;
    }

    @Nonnull @Override public SnapshotEntryBuilder setTerm(int term) {
        builder.term = term;
        return this;
    }

    @Nonnull @Override public SnapshotEntryBuilder setSnapshotChunks(@Nonnull List<SnapshotChunk> snapshotChunks) {
        builder.operation = snapshotChunks;
        return this;
    }

    @Nonnull @Override public SnapshotEntryBuilder setGroupMembersView(@Nonnull RaftGroupMembersView groupMembersView) {
        builder.groupMembersView = groupMembersView;
        return this;
    }

    @Nonnull @Override public SnapshotEntry build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override public String toString() {
        String header = builder != null ? "SnapshotEntryBuilder" : "SnapshotEntry";
        return header + "{" + "term=" + term + ", index=" + index + ", operation=" + operation + ", groupMembers="
               + groupMembersView + '}';
    }

}
