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
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public class DefaultInstallSnapshotResponse
        implements InstallSnapshotResponse {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private long snapshotIndex;
    private List<Integer> requestedSnapshotChunkIndices;
    private long queryRound;

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
    @Nonnull
    public List<Integer> getRequestedSnapshotChunkIndices() {
        return requestedSnapshotChunkIndices;
    }

    @Override
    public long getQueryRound() {
        return queryRound;
    }

    @Override
    public String toString() {
        return "InstallSnapshotResponse{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", snapshotIndex="
                + snapshotIndex + ", requestedSnapshotChunkIndices=" + requestedSnapshotChunkIndices + ", queryRound="
                + queryRound + '}';
    }

    public static class DefaultInstallSnapshotResponseBuilder
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
        public InstallSnapshotResponseBuilder setRequestedSnapshotChunkIndices(
                @Nonnull List<Integer> requestedSnapshotChunkIndices) {
            requireNonNull(requestedSnapshotChunkIndices);
            response.requestedSnapshotChunkIndices = requestedSnapshotChunkIndices;
            return this;
        }

        @Nonnull
        @Override
        public InstallSnapshotResponseBuilder setQueryRound(long queryRound) {
            response.queryRound = queryRound;
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
