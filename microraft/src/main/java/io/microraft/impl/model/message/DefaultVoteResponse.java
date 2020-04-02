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
import io.microraft.model.message.VoteResponse;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public final class DefaultVoteResponse
        implements VoteResponse {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private boolean granted;

    private DefaultVoteResponse() {
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
    public boolean isGranted() {
        return granted;
    }

    @Override
    public String toString() {
        return "VoteResponse{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", granted=" + granted + '}';
    }

    public static final class DefaultVoteResponseBuilder
            implements VoteResponseBuilder {

        private DefaultVoteResponse response = new DefaultVoteResponse();

        @Nonnull
        @Override
        public VoteResponseBuilder setGroupId(@Nonnull Object groupId) {
            response.groupId = groupId;
            return this;
        }

        @Nonnull
        @Override
        public VoteResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
            requireNonNull(sender);
            response.sender = sender;
            return this;
        }

        @Nonnull
        @Override
        public VoteResponseBuilder setTerm(int term) {
            response.term = term;
            return this;
        }

        @Nonnull
        @Override
        public VoteResponseBuilder setGranted(boolean granted) {
            response.granted = granted;
            return this;
        }

        @Nonnull
        @Override
        public VoteResponse build() {
            if (this.response == null) {
                throw new IllegalStateException("VoteResponse already built!");
            }

            VoteResponse response = this.response;
            this.response = null;
            return response;
        }

        @Override
        public String toString() {
            return "VoteResponseBuilder{" + "response=" + response + '}';
        }

    }

}
