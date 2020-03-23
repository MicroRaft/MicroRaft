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
import io.microraft.model.message.AppendEntriesSuccessResponse;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public class DefaultAppendEntriesSuccessResponse
        implements AppendEntriesSuccessResponse {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private long lastLogIndex;
    private long querySeqNo;
    private long flowControlSeqNo;

    private DefaultAppendEntriesSuccessResponse() {
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
    public long getLastLogIndex() {
        return lastLogIndex;
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
        return "AppendEntriesSuccessResponse{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term
                + ", lastLogIndex=" + lastLogIndex + ", querySeqNo=" + querySeqNo + ", flowControlSeqNo=" + flowControlSeqNo
                + '}';
    }

    public static class DefaultAppendEntriesSuccessResponseBuilder
            implements AppendEntriesSuccessResponseBuilder {

        private DefaultAppendEntriesSuccessResponse response = new DefaultAppendEntriesSuccessResponse();

        @Nonnull
        @Override
        public AppendEntriesSuccessResponseBuilder setGroupId(@Nonnull Object groupId) {
            requireNonNull(groupId);
            response.groupId = groupId;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesSuccessResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
            requireNonNull(sender);
            response.sender = sender;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesSuccessResponseBuilder setTerm(int term) {
            response.term = term;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesSuccessResponseBuilder setLastLogIndex(long lastLogIndex) {
            response.lastLogIndex = lastLogIndex;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesSuccessResponseBuilder setQuerySeqNo(long querySeqNo) {
            response.querySeqNo = querySeqNo;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesSuccessResponseBuilder setFlowControlSeqNo(long flowControlSeqNo) {
            response.flowControlSeqNo = flowControlSeqNo;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesSuccessResponse build() {
            if (this.response == null) {
                throw new IllegalStateException("AppendEntriesSuccessResponse already built!");
            }

            AppendEntriesSuccessResponse response = this.response;
            this.response = null;
            return response;
        }

        @Override
        public String toString() {
            return "AppendEntriesSuccessResponseBuilder{" + "response=" + response + '}';
        }

    }

}
