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

import static java.util.Objects.requireNonNull;

import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import io.microraft.RaftEndpoint;
import io.microraft.model.log.LogEntry;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesRequest.AppendEntriesRequestBuilder;

/**
 * The default impl of the {@link AppendEntriesRequest} and
 * {@link AppendEntriesRequestBuilder} interfaces. When an instance of this
 * class is created, it is in the builder mode and its state is populated. Once
 * all fields are set, the object switches to the DTO mode where it no longer
 * allows mutations.
 * <p>
 * Please note that {@link #build()} does not verify if all fields are set or
 * not. It is up to the user to populate the DTO state via the builder.
 */
public class DefaultAppendEntriesRequestOrBuilder implements AppendEntriesRequest, AppendEntriesRequestBuilder {

    private Object groupId;
    private RaftEndpoint sender;
    private int term;
    private int previousLogTerm;
    private long previousLogIndex;
    private long commitIndex;
    private List<LogEntry> logEntries;
    private long querySequenceNumber;
    private long flowControlSequenceNumber;
    private DefaultAppendEntriesRequestOrBuilder builder = this;

    public DefaultAppendEntriesRequestOrBuilder() {
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
    public int getPreviousLogTerm() {
        return previousLogTerm;
    }

    @Nonnegative
    @Override
    public long getPreviousLogIndex() {
        return previousLogIndex;
    }

    @Nonnegative
    @Override
    public long getCommitIndex() {
        return commitIndex;
    }

    @Nonnull
    @Override
    public List<LogEntry> getLogEntries() {
        return logEntries;
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
    public AppendEntriesRequestBuilder setGroupId(@Nonnull Object groupId) {
        builder.groupId = groupId;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setTerm(@Nonnegative int term) {
        builder.term = term;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setPreviousLogTerm(@Nonnegative int previousLogTerm) {
        builder.previousLogTerm = previousLogTerm;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setPreviousLogIndex(@Nonnegative long previousLogIndex) {
        builder.previousLogIndex = previousLogIndex;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setCommitIndex(@Nonnegative long commitIndex) {
        builder.commitIndex = commitIndex;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setLogEntries(@Nonnull List<LogEntry> logEntries) {
        builder.logEntries = logEntries;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setQuerySequenceNumber(@Nonnegative long querySequenceNumber) {
        builder.querySequenceNumber = querySequenceNumber;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setFlowControlSequenceNumber(@Nonnegative long flowControlSequenceNumber) {
        builder.flowControlSequenceNumber = flowControlSequenceNumber;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequest build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        String header = builder != null ? "AppendEntriesRequestBuilder" : "AppendEntriesRequest";
        return header + "{" + "groupId=" + groupId + ", sender=" + sender + ", term=" + term + ", prevLogTerm="
                + previousLogTerm + ", prevLogIndex=" + previousLogIndex + ", leaderCommitIndex=" + commitIndex
                + ", logEntries=" + logEntries + ", querySequenceNumber=" + querySequenceNumber
                + ", flowControlSequenceNumber=" + flowControlSequenceNumber + '}';
    }

}
