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
import io.microraft.model.log.LogEntry;
import io.microraft.model.message.AppendEntriesRequest;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public final class DefaultAppendEntriesRequest
        implements AppendEntriesRequest {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private int prevLogTerm;
    private long prevLogIndex;
    private long leaderCommitIndex;
    private List<LogEntry> logEntries;
    private long querySeqNo;
    private long flowControlSeqNo;

    private DefaultAppendEntriesRequest() {
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
    public int getPreviousLogTerm() {
        return prevLogTerm;
    }

    @Override
    public long getPreviousLogIndex() {
        return prevLogIndex;
    }

    @Override
    public long getCommitIndex() {
        return leaderCommitIndex;
    }

    @Nonnull
    @Override
    public List<LogEntry> getLogEntries() {
        return logEntries;
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
        return "AppendEntriesRequest{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", prevLogTerm="
                + prevLogTerm + ", prevLogIndex=" + prevLogIndex + ", leaderCommitIndex=" + leaderCommitIndex + ", logEntries="
                + logEntries + ", querySeqNo=" + querySeqNo + ", flowControlSeqNo=" + flowControlSeqNo + '}';
    }

    public static final class DefaultAppendEntriesRequestBuilder
            implements AppendEntriesRequestBuilder {

        private DefaultAppendEntriesRequest request = new DefaultAppendEntriesRequest();

        @Nonnull
        @Override
        public AppendEntriesRequestBuilder setGroupId(@Nonnull Object groupId) {
            requireNonNull(groupId);
            request.groupId = groupId;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
            requireNonNull(sender);
            request.sender = sender;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesRequestBuilder setTerm(int term) {
            request.term = term;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesRequestBuilder setPreviousLogTerm(int previousLogTerm) {
            request.prevLogTerm = previousLogTerm;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesRequestBuilder setPreviousLogIndex(long previousLogIndex) {
            request.prevLogIndex = previousLogIndex;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesRequestBuilder setCommitIndex(long commitIndex) {
            request.leaderCommitIndex = commitIndex;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesRequestBuilder setLogEntries(@Nonnull List<LogEntry> logEntries) {
            requireNonNull(logEntries);
            request.logEntries = logEntries;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesRequestBuilder setQuerySeqNo(long querySeqNo) {
            request.querySeqNo = querySeqNo;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesRequestBuilder setFlowControlSeqNo(long flowControlSeqNo) {
            request.flowControlSeqNo = flowControlSeqNo;
            return this;
        }

        @Nonnull
        @Override
        public AppendEntriesRequest build() {
            if (this.request == null) {
                throw new IllegalStateException("AppendEntriesRequest already built!");
            }

            AppendEntriesRequest request = this.request;
            this.request = null;
            return request;
        }

        @Override
        public String toString() {
            return "AppendEntriesRequestBuilder{" + "request=" + request + '}';
        }

    }

}
