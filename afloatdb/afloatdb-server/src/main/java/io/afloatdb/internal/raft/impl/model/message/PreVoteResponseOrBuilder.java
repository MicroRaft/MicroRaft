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
import io.afloatdb.raft.proto.PreVoteResponseProto;
import io.afloatdb.raft.proto.RaftMessageRequest;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.PreVoteResponse;
import io.microraft.model.message.PreVoteResponse.PreVoteResponseBuilder;

import javax.annotation.Nonnull;

public class PreVoteResponseOrBuilder implements PreVoteResponse, PreVoteResponseBuilder, RaftMessageRequestAware {

    private PreVoteResponseProto.Builder builder;
    private PreVoteResponseProto response;
    private RaftEndpoint sender;

    public PreVoteResponseOrBuilder() {
        this.builder = PreVoteResponseProto.newBuilder();
    }

    public PreVoteResponseOrBuilder(PreVoteResponseProto response) {
        this.response = response;
        this.sender = AfloatDBEndpoint.wrap(response.getSender());
    }

    public PreVoteResponseProto getResponse() {
        return response;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setGranted(boolean granted) {
        builder.setGranted(granted);
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(RaftMessageRequest.Builder builder) {
        builder.setPreVoteResponse(response);
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "PreVoteResponse{builder=" + builder + "}";
        }

        return "PreVoteResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term=" + getTerm()
                + ", granted=" + isGranted() + '}';
    }

    @Override
    public boolean isGranted() {
        return response.getGranted();
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

}
