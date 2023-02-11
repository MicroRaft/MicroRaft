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
import io.microraft.model.message.AppendEntriesFailureResponse.AppendEntriesFailureResponseBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nonnegative;

import static java.util.Objects.requireNonNull;

/**
 * The default impl of the {@link AppendEntriesFailureResponse} and
 * {@link AppendEntriesFailureResponseBuilder} interfaces. When an instance of
 * this class is created, it is in the builder mode and its state is populated.
 * Once all fields are set, the object switches to the DTO mode where it no
 * longer allows mutations.
 * <p>
 * Please note that {@link #build()} does not verify if all fields are set or
 * not. It is up to the user to populate the DTO state via the builder.
 */
public class DefaultAppendEntriesFailureResponseOrBuilder
        implements
            AppendEntriesFailureResponse,
            AppendEntriesFailureResponseBuilder {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private long expectedNextIndex;
    private long querySequenceNumber;
    private long flowControlSequenceNumber;
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

    @Nonnegative
    @Override
    public int getTerm() {
        return term;
    }

    @Nonnegative
    @Override
    public long getExpectedNextIndex() {
        return expectedNextIndex;
    }

    @Nonnegative
    @Override
    public long getQuerySequenceNumber() {
        return querySequenceNumber;
    }

    @Nonnegative
    @Override
    public long getFlowControlSequenceNumber() {
        return flowControlSequenceNumber;
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
    public AppendEntriesFailureResponseBuilder setTerm(@Nonnegative int term) {
        builder.term = term;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setExpectedNextIndex(@Nonnegative long expectedNextIndex) {
        builder.expectedNextIndex = expectedNextIndex;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setQuerySequenceNumber(@Nonnegative long querySequenceNumber) {
        builder.querySequenceNumber = querySequenceNumber;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder setFlowControlSequenceNumber(
            @Nonnegative long flowControlSequenceNumber) {
        builder.flowControlSequenceNumber = flowControlSequenceNumber;
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
                + expectedNextIndex + ", querySequenceNumber=" + querySequenceNumber + ", flowControlSequenceNumber="
                + flowControlSequenceNumber + '}';
    }

}
