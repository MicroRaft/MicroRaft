/*
 * Copyright (c) 2020, AfloatDB.
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

package io.afloatdb.internal.raft.impl.model.log;

import static java.util.stream.Collectors.toList;

import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.raft.proto.SnapshotEntryProto;
import io.microraft.RaftEndpoint;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.log.SnapshotEntry.SnapshotEntryBuilder;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

public class SnapshotEntryOrBuilder implements SnapshotEntry, SnapshotEntryBuilder {

    private SnapshotEntryProto.Builder builder;
    private SnapshotEntryProto entry;
    private List<SnapshotChunk> snapshotChunks;
    private RaftGroupMembersView groupMembersView;

    public SnapshotEntryOrBuilder() {
        this.builder = SnapshotEntryProto.newBuilder();
    }

    public SnapshotEntryOrBuilder(SnapshotEntryProto entry) {
        this.entry = entry;
        this.snapshotChunks = entry.getSnapshotChunkList().stream().map(SnapshotChunkOrBuilder::new).collect(toList());
        this.groupMembersView = new RaftGroupMembersViewOrBuilder(entry.getGroupMembersView());
    }

    public SnapshotEntryProto getEntry() {
        return entry;
    }

    @Override
    public long getIndex() {
        return entry.getIndex();
    }

    @Override
    public int getTerm() {
        return entry.getTerm();
    }

    @Nonnull
    @Override
    public Object getOperation() {
        return snapshotChunks;
    }

    @Override
    public int getSnapshotChunkCount() {
        return entry.getSnapshotChunkCount();
    }

    @Nonnull
    @Override
    public RaftGroupMembersView getGroupMembersView() {
        return groupMembersView;
    }

    @Override
    public SnapshotEntryBuilder setIndex(long index) {
        builder.setIndex(index);
        return this;
    }

    @Override
    public SnapshotEntryBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder setSnapshotChunks(@Nonnull List<SnapshotChunk> snapshotChunks) {
        snapshotChunks.stream().map(chunk -> ((SnapshotChunkOrBuilder) chunk).getSnapshotChunk())
                .forEach(builder::addSnapshotChunk);
        this.snapshotChunks = snapshotChunks;
        return this;
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder setGroupMembersView(@Nonnull RaftGroupMembersView groupMembersView) {
        builder.setGroupMembersView(((RaftGroupMembersViewOrBuilder) groupMembersView).getGroupMembersView());
        this.groupMembersView = groupMembersView;
        return this;
    }

    @Nonnull
    @Override
    public SnapshotEntry build() {
        entry = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "SnapshotEntry{builder=" + builder + "}";
        }

        return ("SnapshotEntry{" + "index=" + getIndex() + ", term=" + getTerm() + ", operation=" + getOperation()
                + ", groupMembersView=" + getGroupMembersView() + '}');
    }
}
