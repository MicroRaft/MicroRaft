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

import io.microraft.RaftEndpoint;
import io.microraft.impl.handler.AppendEntriesRequestHandler;

import javax.annotation.Nonnull;

/**
 * Response for a successful {@link AppendEntriesRequest}.
 * <p>
 * See <i>5.3 Log replication</i> section of <i>In Search of an Understandable
 * Consensus Algorithm</i> paper by <i>Diego Ongaro</i> and <i>John
 * Ousterhout</i>.
 *
 * @see AppendEntriesRequest
 * @see AppendEntriesRequestHandler
 */
public interface AppendEntriesSuccessResponse extends RaftMessage {

    long getLastLogIndex();

    long getQuerySequenceNumber();

    long getFlowControlSequenceNumber();

    /**
     * The builder interface for {@link AppendEntriesSuccessResponse}.
     */
    interface AppendEntriesSuccessResponseBuilder extends RaftMessageBuilder<AppendEntriesSuccessResponse> {

        @Nonnull
        AppendEntriesSuccessResponseBuilder setGroupId(@Nonnull Object groupId);

        @Nonnull
        AppendEntriesSuccessResponseBuilder setSender(@Nonnull RaftEndpoint sender);

        @Nonnull
        AppendEntriesSuccessResponseBuilder setTerm(int term);

        @Nonnull
        AppendEntriesSuccessResponseBuilder setLastLogIndex(long lastLogIndex);

        @Nonnull
        AppendEntriesSuccessResponseBuilder setQuerySequenceNumber(long querySequenceNumber);

        @Nonnull
        AppendEntriesSuccessResponseBuilder setFlowControlSequenceNumber(long flowControlSequenceNumber);

    }

}
