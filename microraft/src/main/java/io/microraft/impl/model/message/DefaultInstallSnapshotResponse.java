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

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public final class DefaultInstallSnapshotResponse
        implements InstallSnapshotResponse {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private long snapshotIndex;
    private int requestedSnapshotChunkIndex;
    private long querySeqNo;
    private long flowControlSeqNo;

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

    @Override
    public String toString() {
        return "InstallSnapshotResponse{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", snapshotIndex="
                + snapshotIndex + ", requestedSnapshotChunkIndex=" + requestedSnapshotChunkIndex + ", querySeqNo=" + querySeqNo
                + ", flowControlSeqNo=" + flowControlSeqNo + '}';
    }

    public static final class DefaultInstallSnapshotResponseBuilder
            implements InstallSnapshotResponseBuilder {

        private DefaultInstallSnapshotResponse response = new DefaultInstallSnapshotResponse();

        @Nonnull
        @Override
        public InstallSnapshotResponseBuilder setGroupId(@Nonnull Object groupId) {
            requireNonNull(groupId);
            response.groupId = groupId;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
            requireNonNull(sender);
            response.sender = sender;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotResponseBuilder setTerm(int term) {
            response.term = term;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotResponseBuilder setSnapshotIndex(long snapshotIndex) {
            response.snapshotIndex = snapshotIndex;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotResponseBuilder setRequestedSnapshotChunkIndex(int requestedSnapshotChunkIndex) {
            response.requestedSnapshotChunkIndex = requestedSnapshotChunkIndex;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotResponseBuilder setQuerySeqNo(long querySeqNo) {
            response.querySeqNo = querySeqNo;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotResponseBuilder setFlowControlSeqNo(long flowControlSeqNo) {
            response.flowControlSeqNo = flowControlSeqNo;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotResponse build() {
            if (this.response == null) {
                throw new IllegalStateException("InstallSnapshotResponse already built!");
            }

            InstallSnapshotResponse response = this.response;
            this.response = null;
            return response;
        }

    }

}
