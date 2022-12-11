package io.afloatdb.internal.raft.impl.model.log;

import io.afloatdb.raft.proto.KVSnapshotChunk;
import io.afloatdb.raft.proto.KVSnapshotChunkData;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotChunk.SnapshotChunkBuilder;
import io.microraft.model.log.RaftGroupMembersView;

import javax.annotation.Nonnull;

public class SnapshotChunkOrBuilder implements SnapshotChunk, SnapshotChunkBuilder {

    private KVSnapshotChunk.Builder builder;
    private KVSnapshotChunk snapshotChunk;
    private RaftGroupMembersView groupMembersView;

    public SnapshotChunkOrBuilder() {
        this.builder = KVSnapshotChunk.newBuilder();
    }

    public SnapshotChunkOrBuilder(KVSnapshotChunk snapshotChunk) {
        this.snapshotChunk = snapshotChunk;
        this.groupMembersView = new RaftGroupMembersViewOrBuilder(snapshotChunk.getGroupMembersView());
    }

    public KVSnapshotChunk getSnapshotChunk() {
        return snapshotChunk;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setIndex(long index) {
        builder.setIndex(index);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setOperation(@Nonnull Object operation) {
        builder.setOperation((KVSnapshotChunkData) operation);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setSnapshotChunkIndex(int snapshotChunkIndex) {
        builder.setSnapshotChunkIndex(snapshotChunkIndex);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setSnapshotChunkCount(int snapshotChunkCount) {
        builder.setSnapshotChunkCount(snapshotChunkCount);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setGroupMembersView(@Nonnull RaftGroupMembersView groupMembersView) {
        builder.setGroupMembersView(((RaftGroupMembersViewOrBuilder) groupMembersView).getGroupMembersView());
        this.groupMembersView = groupMembersView;
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunk build() {
        snapshotChunk = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "RaftGroupMembersView{builder=" + builder + "}";
        }

        return "RaftGroupMembersView{" + "index=" + getIndex() + ", term=" + getTerm() + ", operation=" + getOperation()
                + ", snapshotChunkIndex=" + getSnapshotChunkIndex() + ", snapshotChunkCount=" + getSnapshotChunkCount()
                + ", groupMembersView=" + getGroupMembersView() + '}';
    }

    @Override
    public int getSnapshotChunkIndex() {
        return snapshotChunk.getSnapshotChunkIndex();
    }

    @Override
    public int getSnapshotChunkCount() {
        return snapshotChunk.getSnapshotChunkCount();
    }

    @Override
    public long getIndex() {
        return snapshotChunk.getIndex();
    }

    @Override
    public int getTerm() {
        return snapshotChunk.getTerm();
    }

    @Nonnull
    @Override
    public Object getOperation() {
        return snapshotChunk.getOperation();
    }

    @Nonnull
    @Override
    public RaftGroupMembersView getGroupMembersView() {
        return groupMembersView;
    }

}
