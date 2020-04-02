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
import io.microraft.model.message.PreVoteResponse;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public final class DefaultPreVoteResponse
        implements PreVoteResponse {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private boolean granted;

    private DefaultPreVoteResponse() {
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
        return "PreVoteResponse{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", granted=" + granted + '}';
    }

    public static final class DefaultPreVoteResponseBuilder
            implements PreVoteResponseBuilder {

        private DefaultPreVoteResponse response = new DefaultPreVoteResponse();

        @Nonnull
        @Override
        public PreVoteResponseBuilder setGroupId(@Nonnull Object groupId) {
            requireNonNull(groupId);
            response.groupId = groupId;
            return this;
        }

        @Nonnull
        @Override
        public PreVoteResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
            requireNonNull(sender);
            response.sender = sender;
            return this;
        }

        @Nonnull
        @Override
        public PreVoteResponseBuilder setTerm(int term) {
            response.term = term;
            return this;
        }

        @Nonnull
        @Override
        public PreVoteResponseBuilder setGranted(boolean granted) {
            response.granted = granted;
            return this;
        }

        @Nonnull
        @Override
        public PreVoteResponse build() {
            if (this.response == null) {
                throw new IllegalStateException("PreVoteResponse already built!");
            }

            PreVoteResponse response = this.response;
            this.response = null;
            return response;
        }

        @Override
        public String toString() {
            return "PreVoteResponseBuilder{" + "response=" + response + '}';
        }

    }

}
