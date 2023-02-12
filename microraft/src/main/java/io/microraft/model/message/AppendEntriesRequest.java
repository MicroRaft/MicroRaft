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

package io.microraft.model.message;

import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import io.microraft.RaftEndpoint;
import io.microraft.impl.handler.AppendEntriesRequestHandler;
import io.microraft.model.log.LogEntry;

/**
 * Raft message for the AppendEntries RPC.
 * <p>
 * See <i>5.3 Log replication</i> section of <i>In Search of an Understandable
 * Consensus Algorithm</i> paper by <i>Diego Ongaro</i> and <i>John
 * Ousterhout</i>.
 * <p>
 * Invoked by leader to replicate log entries (§5.3); also used as heartbeat
 * (§5.2).
 *
 * @see AppendEntriesRequestHandler
 */
public interface AppendEntriesRequest extends RaftMessage {

    @Nonnegative
    int getPreviousLogTerm();

    @Nonnegative
    long getPreviousLogIndex();

    @Nonnegative
    long getCommitIndex();

    @Nonnull
    List<LogEntry> getLogEntries();

    @Nonnegative
    long getQuerySequenceNumber();

    @Nonnegative
    long getFlowControlSequenceNumber();

    /**
     * The builder interface for {@link AppendEntriesRequest}.
     */
    interface AppendEntriesRequestBuilder extends RaftMessageBuilder<AppendEntriesRequest> {

        @Nonnull
        AppendEntriesRequestBuilder setGroupId(@Nonnull Object groupId);

        @Nonnull
        AppendEntriesRequestBuilder setSender(@Nonnull RaftEndpoint sender);

        @Nonnull
        AppendEntriesRequestBuilder setTerm(@Nonnegative int term);

        @Nonnull
        AppendEntriesRequestBuilder setPreviousLogTerm(@Nonnegative int previousLogTerm);

        @Nonnull
        AppendEntriesRequestBuilder setPreviousLogIndex(@Nonnegative long previousLogIndex);

        @Nonnull
        AppendEntriesRequestBuilder setCommitIndex(@Nonnegative long commitIndex);

        @Nonnull
        AppendEntriesRequestBuilder setLogEntries(@Nonnull List<LogEntry> logEntries);

        @Nonnull
        AppendEntriesRequestBuilder setQuerySequenceNumber(@Nonnegative long querySequenceNumber);

        @Nonnull
        AppendEntriesRequestBuilder setFlowControlSequenceNumber(@Nonnegative long flowControlSequenceNumber);

    }

}
