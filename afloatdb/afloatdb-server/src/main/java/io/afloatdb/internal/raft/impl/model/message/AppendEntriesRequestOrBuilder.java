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
import io.afloatdb.internal.raft.impl.model.log.LogEntryOrBuilder;
import io.afloatdb.raft.proto.AppendEntriesRequestProto;
import io.afloatdb.raft.proto.LogEntryProto;
import io.afloatdb.raft.proto.RaftRequest;
import io.microraft.RaftEndpoint;
import io.microraft.model.log.LogEntry;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesRequest.AppendEntriesRequestBuilder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public class AppendEntriesRequestOrBuilder
        implements
            AppendEntriesRequest,
            AppendEntriesRequestBuilder,
            RaftRequestAware {

    private AppendEntriesRequestProto.Builder builder;
    private AppendEntriesRequestProto request;
    private RaftEndpoint sender;
    private List<LogEntry> logEntries;

    public AppendEntriesRequestOrBuilder() {
        builder = AppendEntriesRequestProto.newBuilder();
    }

    public AppendEntriesRequestOrBuilder(AppendEntriesRequestProto request) {
        this.request = request;
        this.sender = AfloatDBEndpoint.wrap(request.getSender());
        this.logEntries = new ArrayList<>(request.getEntryCount());
        for (LogEntryProto e : request.getEntryList()) {
            this.logEntries.add(new LogEntryOrBuilder(e));
        }
    }

    public AppendEntriesRequestProto getRequest() {
        return request;
    }

    @Override
    public void populate(RaftRequest.Builder builder) {
        builder.setAppendEntriesRequest(request);
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setGroupId(@Nonnull Object groupId) {
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.unwrap(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setPreviousLogTerm(int previousLogTerm) {
        builder.setPrevLogTerm(previousLogTerm);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setPreviousLogIndex(long previousLogIndex) {
        builder.setPrevLogIndex(previousLogIndex);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setCommitIndex(long commitIndex) {
        builder.setCommitIndex(commitIndex);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setLogEntries(@Nonnull List<LogEntry> logEntries) {
        for (LogEntry entry : logEntries) {
            builder.addEntry(((LogEntryOrBuilder) entry).getEntry());
        }

        this.logEntries = logEntries;

        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setQuerySequenceNumber(long querySequenceNumber) {
        builder.setQuerySequenceNumber(querySequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder setFlowControlSequenceNumber(long flowControlSequenceNumber) {
        builder.setFlowControlSequenceNumber(flowControlSequenceNumber);
        return this;
    }

    @Nonnull
    @Override
    public AppendEntriesRequest build() {
        request = builder.build();
        builder = null;
        return this;
    }

    @Override
    public int getPreviousLogTerm() {
        return request.getPrevLogTerm();
    }

    @Override
    public long getPreviousLogIndex() {
        return request.getPrevLogIndex();
    }

    @Override
    public long getCommitIndex() {
        return request.getCommitIndex();
    }

    @Nonnull
    @Override
    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    @Override
    public long getQuerySequenceNumber() {
        return request.getQuerySequenceNumber();
    }

    @Override
    public long getFlowControlSequenceNumber() {
        return request.getFlowControlSequenceNumber();
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

    @Override
    public String toString() {
        if (builder != null) {
            return "AppendEntriesRequest{builder=" + builder + "}";
        }

        return ("AppendEntriesRequest{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term="
                + getTerm() + ", commitIndex=" + getCommitIndex() + ", querySequenceNumber=" + getQuerySequenceNumber()
                + ", flowControlSequenceNumber=" + getFlowControlSequenceNumber() + ", " + "prevLogIndex="
                + getPreviousLogIndex() + ", prevLogTerm=" + getPreviousLogTerm() + ", entries=" + getLogEntries()
                + '}');
    }
}
