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
import io.microraft.model.message.PreVoteRequest;
import io.microraft.model.message.PreVoteRequest.PreVoteRequestBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nonnegative;

import static java.util.Objects.requireNonNull;

/**
 * The default impl of the {@link PreVoteRequest} and
 * {@link PreVoteRequestBuilder} interfaces. When an instance of this class is
 * created, it is in the builder mode and its state is populated. Once all
 * fields are set, the object switches to the DTO mode where it no longer allows
 * mutations.
 * <p>
 * Please note that {@link #build()} does not verify if all fields are set or
 * not. It is up to the user to populate the DTO state via the builder.
 */
public class DefaultPreVoteRequestOrBuilder implements PreVoteRequest, PreVoteRequestBuilder {

    private Object groupId;
    private RaftEndpoint sender;
    private int nextTerm;
    private int lastLogTerm;
    private long lastLogIndex;
    private DefaultPreVoteRequestOrBuilder builder = this;

    @Override
    public Object getGroupId() {
        return groupId;
    }

    @Nonnull
    @Override
    public RaftEndpoint getSender() {
        return sender;
    }

    @Nonnegative
    @Override
    public int getTerm() {
        return nextTerm;
    }

    @Nonnegative
    @Override
    public int getLastLogTerm() {
        return lastLogTerm;
    }

    @Nonnegative
    @Override
    public long getLastLogIndex() {
        return lastLogIndex;
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder setGroupId(@Nonnull Object groupId) {
        builder.groupId = groupId;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder setTerm(@Nonnegative int term) {
        builder.nextTerm = term;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder setLastLogTerm(@Nonnegative int lastLogTerm) {
        builder.lastLogTerm = lastLogTerm;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder setLastLogIndex(@Nonnegative long lastLogIndex) {
        builder.lastLogIndex = lastLogIndex;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteRequest build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        String header = builder != null ? "PreVoteRequestBuilder" : "PreVoteRequest";
        return header + "{" + "groupId=" + groupId + ", sender=" + sender + ", nextTerm=" + nextTerm + ", lastLogTerm="
                + lastLogTerm + ", lastLogIndex=" + lastLogIndex + '}';
    }

}
