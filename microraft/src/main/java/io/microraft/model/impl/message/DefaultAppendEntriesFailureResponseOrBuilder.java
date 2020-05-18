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
import io.microraft.model.message.AppendEntriesFailureResponse;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public class DefaultAppendEntriesFailureResponseOrBuilder
        implements AppendEntriesFailureResponse, AppendEntriesFailureResponse.AppendEntriesFailureResponseBuilder {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private long expectedNextIndex;
    private long querySeqNo;
    private long flowControlSeqNo;
    private DefaultAppendEntriesFailureResponseOrBuilder builder = this;

    public DefaultAppendEntriesFailureResponseOrBuilder() {
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
    public long getExpectedNextIndex() {
        return expectedNextIndex;
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
    public AppendEntriesFailureResponseBuilder setGroupId(@Nonnull Object groupId) {
        builder.groupId = groupId;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setTerm(int term) {
        builder.term = term;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setExpectedNextIndex(long expectedNextIndex) {
        builder.expectedNextIndex = expectedNextIndex;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setQuerySeqNo(long querySeqNo) {
        builder.querySeqNo = querySeqNo;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setFlowControlSeqNo(long flowControlSeqNo) {
        builder.flowControlSeqNo = flowControlSeqNo;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponse build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        String header = builder != null ? "AppendEntriesFailureResponseBuilder" : "AppendEntriesFailureResponse";
        return header + "{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", expectedNextIndex="
                + expectedNextIndex + ", querySeqNo=" + querySeqNo + ", flowControlSeqNo=" + flowControlSeqNo + '}';
    }

}
