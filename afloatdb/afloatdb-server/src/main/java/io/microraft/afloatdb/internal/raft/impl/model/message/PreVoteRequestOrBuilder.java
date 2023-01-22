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

package io.microraft.afloatdb.internal.raft.impl.model.message;

import io.microraft.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.microraft.afloatdb.raft.proto.PreVoteRequestProto;
import io.microraft.afloatdb.raft.proto.RaftRequest;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.model.message.PreVoteRequest.PreVoteRequestBuilder;
import javax.annotation.Nonnull;

public class PreVoteRequestOrBuilder implements PreVoteRequest, PreVoteRequestBuilder, RaftRequestAware {

    private PreVoteRequestProto.Builder builder;
    private PreVoteRequestProto request;
    private RaftEndpoint sender;

    public PreVoteRequestOrBuilder() {
        this.builder = PreVoteRequestProto.newBuilder();
    }

    public PreVoteRequestOrBuilder(PreVoteRequestProto request) {
        this.request = request;
        this.sender = AfloatDBEndpoint.wrap(request.getSender());
    }

    public PreVoteRequestProto getRequest() {
        return request;
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder setLastLogTerm(int lastLogTerm) {
        builder.setLastLogTerm(lastLogTerm);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder setLastLogIndex(long lastLogIndex) {
        builder.setLastLogIndex(lastLogIndex);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteRequest build() {
        request = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(RaftRequest.Builder builder) {
        builder.setPreVoteRequest(request);
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "PreVoteRequest{builder=" + builder + "}";
        }

        return ("PreVoteRequest{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term=" + getTerm()
                + ", lastLogTerm=" + getLastLogTerm() + ", lastLogIndex=" + getLastLogIndex() + '}');
    }

    @Override
    public int getLastLogTerm() {
        return request.getLastLogTerm();
    }

    @Override
    public long getLastLogIndex() {
        return request.getLastLogIndex();
    }

    @Override
    public Object getGroupId() {
        return request.getGroupId();
    }

    @Nonnull
    @Override
    public RaftEndpoint getSender() {
        return sender;
    }

    @Override
    public int getTerm() {
        return request.getTerm();
    }
}
