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
import io.microraft.model.message.TriggerLeaderElectionRequest;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public final class DefaultTriggerLeaderElectionRequest
        implements TriggerLeaderElectionRequest {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private int lastLogTerm;
    private long lastLogIndex;

    private DefaultTriggerLeaderElectionRequest() {
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
    public int getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public long getLastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public String toString() {
        return "TriggerLeaderElectionRequest{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", lastLogTerm="
                + lastLogTerm + ", lastLogIndex=" + lastLogIndex + '}';
    }

    public static final class DefaultTriggerLeaderElectionRequestBuilder
            implements TriggerLeaderElectionRequestBuilder {

        private DefaultTriggerLeaderElectionRequest request = new DefaultTriggerLeaderElectionRequest();

        @Nonnull
        @Override
        public TriggerLeaderElectionRequestBuilder setGroupId(@Nonnull Object groupId) {
            requireNonNull(groupId);
            request.groupId = groupId;
            return this;
        }

        @Nonnull
        @Override
        public TriggerLeaderElectionRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
            requireNonNull(sender);
            request.sender = sender;
            return this;
        }

        @Nonnull
        @Override
        public TriggerLeaderElectionRequestBuilder setTerm(int term) {
            request.term = term;
            return this;
        }

        @Nonnull
        @Override
        public TriggerLeaderElectionRequestBuilder setLastLogTerm(int lastLogTerm) {
            request.lastLogTerm = lastLogTerm;
            return this;
        }

        @Nonnull
        @Override
        public TriggerLeaderElectionRequestBuilder setLastLogIndex(long lastLogIndex) {
            request.lastLogIndex = lastLogIndex;
            return this;
        }

        @Nonnull
        @Override
        public TriggerLeaderElectionRequest build() {
            if (this.request == null) {
                throw new IllegalStateException("TriggerLeaderElectionRequest already built!");
            }

            TriggerLeaderElectionRequest request = this.request;
            this.request = null;
            return request;
        }

        @Override
        public String toString() {
            return "TriggerLeaderElectionRequestBuilder{" + "request=" + request + '}';
        }

    }

}
