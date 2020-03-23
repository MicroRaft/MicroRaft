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
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.message.InstallSnapshotRequest;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public class DefaultInstallSnapshotRequest
        implements InstallSnapshotRequest {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private boolean leader;
    private int snapshotTerm;
    private long snapshotIndex;
    private int totalSnapshotChunkCount;
    private List<SnapshotChunk> snapshotChunks;
    private long groupMembersLogIndex;
    private Collection<RaftEndpoint> groupMembers;
    private long querySeqNo;
    private long flowControlSeqNo;

    private DefaultInstallSnapshotRequest() {
    }

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

    @Nonnull
    @Override
    public List<SnapshotChunk> getSnapshotChunks() {
        return snapshotChunks;
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

    @Override
    public String toString() {
        return "InstallSnapshotRequest{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", leader=" + leader
                + ", snapshotTerm=" + snapshotTerm + ", snapshotIndex=" + snapshotIndex + ", chunkCount="
                + totalSnapshotChunkCount + ", snapshotChunks=" + snapshotChunks + ", groupMembersLogIndex="
                + groupMembersLogIndex + ", groupMembers=" + groupMembers + ", querySeqNo=" + querySeqNo + ", flowControlSeqNo="
                + flowControlSeqNo + '}';
    }

    public static class DefaultInstallSnapshotRequestBuilder
            implements InstallSnapshotRequestBuilder {

        private DefaultInstallSnapshotRequest request = new DefaultInstallSnapshotRequest();

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setGroupId(@Nonnull Object groupId) {
            requireNonNull(groupId);
            request.groupId = groupId;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
            requireNonNull(sender);
            request.sender = sender;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setTerm(int term) {
            request.term = term;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setSenderLeader(boolean leader) {
            request.leader = leader;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setSnapshotTerm(int snapshotTerm) {
            request.snapshotTerm = snapshotTerm;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setSnapshotIndex(long snapshotIndex) {
            request.snapshotIndex = snapshotIndex;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setTotalSnapshotChunkCount(int totalSnapshotChunkCount) {
            request.totalSnapshotChunkCount = totalSnapshotChunkCount;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setSnapshotChunks(@Nonnull List<SnapshotChunk> snapshotChunks) {
            requireNonNull(snapshotChunks);
            request.snapshotChunks = snapshotChunks;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setGroupMembersLogIndex(long groupMembersLogIndex) {
            request.groupMembersLogIndex = groupMembersLogIndex;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setGroupMembers(@Nonnull Collection<RaftEndpoint> groupMembers) {
            requireNonNull(groupMembers);
            request.groupMembers = groupMembers;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setQuerySeqNo(long querySeqNo) {
            request.querySeqNo = querySeqNo;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequestBuilder setFlowControlSeqNo(long flowControlSeqNo) {
            request.flowControlSeqNo = flowControlSeqNo;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotRequest build() {
            if (this.request == null) {
                throw new IllegalStateException("InstallSnapshotRequest already built!");
            }

            InstallSnapshotRequest request = this.request;
            this.request = null;
            return request;
        }

        @Override
        public String toString() {
            return "InstallSnapshotRequestBuilder{" + "request=" + request + '}';
        }

    }

}
