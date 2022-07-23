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
import io.microraft.model.message.PreVoteResponse;
import io.microraft.model.message.PreVoteResponse.PreVoteResponseBuilder;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * The default impl of the {@link PreVoteResponse} and {@link PreVoteResponseBuilder} interfaces. When an instance of
 * this class is created, it is in the builder mode and its state is populated. Once all fields are set, the object
 * switches to the DTO mode where it no longer allows mutations.
 * <p>
 * Please note that {@link #build()} does not verify if all fields are set or not. It is up to the user to populate the
 * DTO state via the builder.
 */
public class DefaultPreVoteResponseOrBuilder implements PreVoteResponse, PreVoteResponseBuilder {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private boolean granted;
    private DefaultPreVoteResponseOrBuilder builder = this;

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

    @Nonnull
    @Override
    public PreVoteResponseBuilder setGroupId(@Nonnull Object groupId) {
        builder.groupId = groupId;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setTerm(int term) {
        builder.term = term;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder setGranted(boolean granted) {
        builder.granted = granted;
        return this;
    }

    @Nonnull
    @Override
    public PreVoteResponse build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        String header = builder != null ? "PreVoteResponseBuilder" : "PreVoteResponse";
        return header + "{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", granted=" + granted
                + '}';
    }

}
