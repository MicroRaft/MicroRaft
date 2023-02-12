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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import io.microraft.RaftEndpoint;

/**
 * Raft message for the VoteRequest RPC.
 * <p>
 * See <i>5.2 Leader election</i> section of <i>In Search of an Understandable
 * Consensus Algorithm</i> paper by <i>Diego Ongaro</i> and <i>John
 * Ousterhout</i>.
 * <p>
 * Invoked by candidates to gather votes (§5.2).
 */
public interface VoteRequest extends RaftMessage {

    @Nonnegative
    int getLastLogTerm();

    @Nonnegative
    long getLastLogIndex();

    boolean isSticky();

    /**
     * The builder interface for {@link VoteRequest}.
     */
    interface VoteRequestBuilder extends RaftMessageBuilder<VoteRequest> {

        @Nonnull
        VoteRequestBuilder setGroupId(@Nonnull Object groupId);

        @Nonnull
        VoteRequestBuilder setSender(@Nonnull RaftEndpoint sender);

        @Nonnull
        VoteRequestBuilder setTerm(@Nonnegative int term);

        @Nonnull
        VoteRequestBuilder setLastLogTerm(@Nonnegative int lastLogTerm);

        @Nonnull
        VoteRequestBuilder setLastLogIndex(@Nonnegative long lastLogIndex);

        @Nonnull
        VoteRequestBuilder setSticky(boolean sticky);

    }

}
