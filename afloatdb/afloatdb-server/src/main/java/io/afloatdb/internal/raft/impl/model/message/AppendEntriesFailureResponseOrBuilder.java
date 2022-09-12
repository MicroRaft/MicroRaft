/*
 * Copyright (c) 2020, AfloatDB.
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

package io.afloatdb.internal.raft.impl.model.message;

import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.raft.proto.AppendEntriesFailureResponseProto;
import io.afloatdb.raft.proto.RaftMessageRequest;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesFailureResponse.AppendEntriesFailureResponseBuilder;

import javax.annotation.Nonnull;

public class AppendEntriesFailureResponseOrBuilder
        implements AppendEntriesFailureResponse, AppendEntriesFailureResponseBuilder, RaftMessageRequestAware {

    private AppendEntriesFailureResponseProto.Builder builder;
    private AppendEntriesFailureResponseProto response;
    private RaftEndpoint sender;

    public AppendEntriesFailureResponseOrBuilder() {
        this.builder = AppendEntriesFailureResponseProto.newBuilder();
    }

    public AppendEntriesFailureResponseOrBuilder(AppendEntriesFailureResponseProto response) {
        this.response = response;
        this.sender = AfloatDBEndpoint.wrap(response.getSender());
    }

    public AppendEntriesFailureResponseProto getResponse() {
        return response;
    }

    @Override
    public void populate(RaftMessageRequest.Builder builder) {
        builder.setAppendEntriesFailureResponse(response);
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setExpectedNextIndex(long expectedNextIndex) {
        builder.setExpectedNextIndex(expectedNextIndex);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setQuerySequenceNumber(long querySequenceNumber) {
        builder.setQuerySequenceNumber(querySequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setFlowControlSequenceNumber(long flowControlSequenceNumber) {
        builder.setFlowControlSequenceNumber(flowControlSequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public long getExpectedNextIndex() {
        return response.getExpectedNextIndex();
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
            return "AppendEntriesFailureResponse{builder=" + builder + "}";
        }

        return "AppendEntriesFailureResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", "
                + "term=" + getTerm() + ", expectedNextIndex=" + getExpectedNextIndex() + ", querySequenceNumber="
                + getQuerySequenceNumber() + ", flowControlSequenceNumber=" + getFlowControlSequenceNumber() + '}';
    }

}
