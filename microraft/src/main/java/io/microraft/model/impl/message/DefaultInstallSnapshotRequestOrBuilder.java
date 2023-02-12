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

package io.microraft.model.impl.message;

import static java.util.Objects.requireNonNull;

import java.util.Collection;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.microraft.RaftEndpoint;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.model.message.InstallSnapshotRequest.InstallSnapshotRequestBuilder;

/**
 * The default impl of the {@link InstallSnapshotRequest} and
 * {@link InstallSnapshotRequestBuilder} interfaces. When an instance of this
 * class is created, it is in the builder mode and its state is populated. Once
 * all fields are set, the object switches to the DTO mode where it no longer
 * allows mutations.
 * <p>
 * Please note that {@link #build()} does not verify if all fields are set or
 * not. It is up to the user to populate the DTO state via the builder.
 */
public class DefaultInstallSnapshotRequestOrBuilder implements InstallSnapshotRequest, InstallSnapshotRequestBuilder {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private boolean leader;
    private int snapshotTerm;
    private long snapshotIndex;
    private int totalSnapshotChunkCount;
    private SnapshotChunk snapshotChunk;
    private Collection<RaftEndpoint> snapshottedMembers;
    private RaftGroupMembersView groupMembersView;
    private long querySequenceNumber;
    private long flowControlSequenceNumber;
    private DefaultInstallSnapshotRequestOrBuilder builder = this;

    @Override
    public Object getGroupId() {
        return groupId;
    }

    @Nonnull
    @Override
    public RaftEndpoint getSender() {
        return sender;
    }

    @Nonnegative
    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public boolean isSenderLeader() {
        return leader;
    }

    @Nonnegative
    @Override
    public int getSnapshotTerm() {
        return snapshotTerm;
    }

    @Nonnegative
    @Override
    public long getSnapshotIndex() {
        return snapshotIndex;
    }

    @Nonnegative
    @Override
    public int getTotalSnapshotChunkCount() {
        return totalSnapshotChunkCount;
    }

    @Nullable
    @Override
    public SnapshotChunk getSnapshotChunk() {
        return snapshotChunk;
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getSnapshottedMembers() {
        return snapshottedMembers;
    }

    @Nonnull
    @Override
    public RaftGroupMembersView getGroupMembersView() {
        return groupMembersView;
    }

    @Nonnegative
    @Override
    public long getQuerySequenceNumber() {
        return querySequenceNumber;
    }

    @Nonnegative
    @Override
    public long getFlowControlSequenceNumber() {
        return flowControlSequenceNumber;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setGroupId(@Nonnull Object groupId) {
        builder.groupId = groupId;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setTerm(@Nonnegative int term) {
        builder.term = term;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSenderLeader(boolean leader) {
        builder.leader = leader;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshotTerm(@Nonnegative int snapshotTerm) {
        builder.snapshotTerm = snapshotTerm;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshotIndex(@Nonnegative long snapshotIndex) {
        builder.snapshotIndex = snapshotIndex;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setTotalSnapshotChunkCount(@Nonnegative int totalSnapshotChunkCount) {
        builder.totalSnapshotChunkCount = totalSnapshotChunkCount;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshotChunk(@Nullable SnapshotChunk snapshotChunk) {
        builder.snapshotChunk = snapshotChunk;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshottedMembers(@Nonnull Collection<RaftEndpoint> snapshottedMembers) {
        builder.snapshottedMembers = snapshottedMembers;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setGroupMembersView(@Nonnull RaftGroupMembersView groupMembersView) {
        builder.groupMembersView = groupMembersView;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setQuerySequenceNumber(@Nonnegative long querySequenceNumber) {
        builder.querySequenceNumber = querySequenceNumber;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setFlowControlSequenceNumber(@Nonnegative long flowControlSequenceNumber) {
        builder.flowControlSequenceNumber = flowControlSequenceNumber;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequest build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        String header = builder != null ? "InstallSnapshotRequestBuilder" : "InstallSnapshotRequest";
        return header + "{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", leader=" + leader
                + ", snapshotTerm=" + snapshotTerm + ", snapshotIndex=" + snapshotIndex + ", chunkCount="
                + totalSnapshotChunkCount + ", snapshotChunk=" + snapshotChunk + ", snapshottedMembers="
                + snapshottedMembers + ", groupMembers=" + groupMembersView + ", querySequenceNumber="
                + querySequenceNumber + ", flowControlSequenceNumber=" + flowControlSequenceNumber + '}';
    }

}
