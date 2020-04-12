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

import io.microraft.RaftEndpoint;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.message.InstallSnapshotRequest;
import io.microraft.model.message.InstallSnapshotRequest.InstallSnapshotRequestBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public final class DefaultInstallSnapshotRequestOrBuilder
        implements InstallSnapshotRequest, InstallSnapshotRequestBuilder {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private boolean leader;
    private int snapshotTerm;
    private long snapshotIndex;
    private int totalSnapshotChunkCount;
    private SnapshotChunk snapshotChunk;
    private Collection<RaftEndpoint> snapshottedMembers;
    private long groupMembersLogIndex;
    private Collection<RaftEndpoint> groupMembers;
    private long querySeqNo;
    private long flowControlSeqNo;
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

    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public boolean isSenderLeader() {
        return leader;
    }

    @Override
    public int getSnapshotTerm() {
        return snapshotTerm;
    }

    @Override
    public long getSnapshotIndex() {
        return snapshotIndex;
    }

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
    public long getQuerySeqNo() {
        return querySeqNo;
    }

    @Override
    public long getFlowControlSeqNo() {
        return flowControlSeqNo;
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
    public InstallSnapshotRequestBuilder setTerm(int term) {
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
    public InstallSnapshotRequestBuilder setSnapshotTerm(int snapshotTerm) {
        builder.snapshotTerm = snapshotTerm;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setSnapshotIndex(long snapshotIndex) {
        builder.snapshotIndex = snapshotIndex;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setTotalSnapshotChunkCount(int totalSnapshotChunkCount) {
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
    public InstallSnapshotRequestBuilder setGroupMembersLogIndex(long groupMembersLogIndex) {
        builder.groupMembersLogIndex = groupMembersLogIndex;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setGroupMembers(@Nonnull Collection<RaftEndpoint> groupMembers) {
        builder.groupMembers = groupMembers;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setQuerySeqNo(long querySeqNo) {
        builder.querySeqNo = querySeqNo;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder setFlowControlSeqNo(long flowControlSeqNo) {
        builder.flowControlSeqNo = flowControlSeqNo;
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
                + totalSnapshotChunkCount + ", snapshotChunk=" + snapshotChunk + ", snapshottedMembers=" + snapshottedMembers
                + ", groupMembersLogIndex=" + groupMembersLogIndex + ", groupMembers=" + groupMembers + ", querySeqNo="
                + querySeqNo + ", flowControlSeqNo=" + flowControlSeqNo + '}';
    }

}
