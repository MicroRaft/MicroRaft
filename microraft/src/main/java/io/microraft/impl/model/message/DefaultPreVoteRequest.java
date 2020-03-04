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
import io.microraft.model.message.PreVoteRequest;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public class DefaultPreVoteRequest
        implements PreVoteRequest {

    private Object groupId;
    private RaftEndpoint sender;
    private int nextTerm;
    private int lastLogTerm;
    private long lastLogIndex;

    private DefaultPreVoteRequest() {
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
        return nextTerm;
    }

    @Override
    public int getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public long getLastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public String toString() {
        return "PreVoteRequest{" + "groupId=" + groupId + ", sender=" + sender + ", nextTerm=" + nextTerm + ", lastLogTerm="
                + lastLogTerm + ", lastLogIndex=" + lastLogIndex + '}';
    }

    public static class DefaultPreVoteRequestBuilder
            implements PreVoteRequestBuilder {

        private DefaultPreVoteRequest request = new DefaultPreVoteRequest();

        @Nonnull
        @Override
        public PreVoteRequestBuilder setGroupId(@Nonnull Object groupId) {
            requireNonNull(groupId);
            request.groupId = groupId;
            return this;
        }

        @Nonnull
        @Override
        public PreVoteRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
            requireNonNull(sender);
            request.sender = sender;
            return this;
        }

        @Nonnull
        @Override
        public PreVoteRequestBuilder setTerm(int term) {
            request.nextTerm = term;
            return this;
        }

        @Nonnull
        @Override
        public PreVoteRequestBuilder setLastLogTerm(int lastLogTerm) {
            request.lastLogTerm = lastLogTerm;
            return this;
        }

        @Nonnull
        @Override
        public PreVoteRequestBuilder setLastLogIndex(long lastLogIndex) {
            request.lastLogIndex = lastLogIndex;
            return this;
        }

        @Nonnull
        @Override
        public PreVoteRequest build() {
            if (this.request == null) {
                throw new IllegalStateException("PreVoteRequest already built!");
            }

            PreVoteRequest request = this.request;
            this.request = null;
            return request;
        }

        @Override
        public String toString() {
            return "PreVoteRequestBuilder{" + "request=" + request + '}';
        }

    }

}
