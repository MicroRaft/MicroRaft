package io.microraft.afloatdb.internal.raft.impl.model.message;

import io.microraft.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.microraft.afloatdb.raft.proto.InstallSnapshotResponseProto;
import io.microraft.afloatdb.raft.proto.RaftRequest;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.InstallSnapshotResponse;
import io.microraft.model.message.InstallSnapshotResponse.InstallSnapshotResponseBuilder;
import javax.annotation.Nonnull;

public class InstallSnapshotResponseOrBuilder
        implements
            InstallSnapshotResponse,
            InstallSnapshotResponseBuilder,
            RaftRequestAware {

    private InstallSnapshotResponseProto.Builder builder;
    private InstallSnapshotResponseProto response;
    private RaftEndpoint sender;

    public InstallSnapshotResponseOrBuilder() {
        this.builder = InstallSnapshotResponseProto.newBuilder();
    }

    public InstallSnapshotResponseOrBuilder(InstallSnapshotResponseProto response) {
        this.response = response;
        this.sender = AfloatDBEndpoint.wrap(response.getSender());
    }

    public InstallSnapshotResponseProto getResponse() {
        return response;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setSnapshotIndex(long snapshotIndex) {
        builder.setSnapshotIndex(snapshotIndex);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setRequestedSnapshotChunkIndex(int requestedSnapshotChunkIndex) {
        builder.setRequestedSnapshotChunkIndex(requestedSnapshotChunkIndex);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setQuerySequenceNumber(long querySequenceNumber) {
        builder.setQuerySequenceNumber(querySequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setFlowControlSequenceNumber(long flowControlSequenceNumber) {
        builder.setFlowControlSequenceNumber(flowControlSequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(RaftRequest.Builder builder) {
        builder.setInstallSnapshotResponse(response);
    }

    @Override
    public long getSnapshotIndex() {
        return response.getSnapshotIndex();
    }

    @Override
    public int getRequestedSnapshotChunkIndex() {
        return response.getRequestedSnapshotChunkIndex();
    }

    @Override
    public long getQuerySequenceNumber() {
        return response.getQuerySequenceNumber();
    }

    @Override
    public long getFlowControlSequenceNumber() {
        return response.getFlowControlSequenceNumber();
    }

    @Override
    public Object getGroupId() {
        return response.getGroupId();
    }

    @Nonnull
    @Override
    public RaftEndpoint getSender() {
        return sender;
    }

    @Override
    public int getTerm() {
        return response.getTerm();
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "InstallSnapshotResponse{builder=" + builder + "}";
        }

        return ("InstallSnapshotResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term="
                + getTerm() + ", snapshotIndex=" + getSnapshotIndex() + ", requestedSnapshotChunkIndex="
                + getRequestedSnapshotChunkIndex() + ", querySequenceNumber=" + getQuerySequenceNumber()
                + ", flowControlSequenceNumber=" + getFlowControlSequenceNumber() + '}');
    }
}
