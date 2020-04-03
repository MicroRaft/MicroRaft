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

package io.microraft.impl.model.message;

import io.microraft.RaftEndpoint;
import io.microraft.model.message.InstallSnapshotResponse;
import io.microraft.model.message.InstallSnapshotResponse.InstallSnapshotResponseBuilder;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public final class DefaultInstallSnapshotResponseOrBuilder
        implements InstallSnapshotResponse, InstallSnapshotResponseBuilder {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private long snapshotIndex;
    private int requestedSnapshotChunkIndex;
    private long querySeqNo;
    private long flowControlSeqNo;
    private DefaultInstallSnapshotResponseOrBuilder builder = this;

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
    public long getSnapshotIndex() {
        return snapshotIndex;
    }

    @Override
    public int getRequestedSnapshotChunkIndex() {
        return requestedSnapshotChunkIndex;
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
    public InstallSnapshotResponseBuilder setGroupId(@Nonnull Object groupId) {
        builder.groupId = groupId;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setTerm(int term) {
        builder.term = term;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setSnapshotIndex(long snapshotIndex) {
        builder.snapshotIndex = snapshotIndex;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setRequestedSnapshotChunkIndex(int requestedSnapshotChunkIndex) {
        builder.requestedSnapshotChunkIndex = requestedSnapshotChunkIndex;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setQuerySeqNo(long querySeqNo) {
        builder.querySeqNo = querySeqNo;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setFlowControlSeqNo(long flowControlSeqNo) {
        builder.flowControlSeqNo = flowControlSeqNo;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponse build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        String header = builder != null ? "InstallSnapshotResponseBuilder" : "InstallSnapshotResponse";
        return header + "{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", snapshotIndex=" + snapshotIndex
                + ", requestedSnapshotChunkIndex=" + requestedSnapshotChunkIndex + ", querySeqNo=" + querySeqNo
                + ", flowControlSeqNo=" + flowControlSeqNo + '}';
    }

}
