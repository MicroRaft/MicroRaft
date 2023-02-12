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
 * Raft message for the PreVoteRequest RPC.
 * <p>
 * See <i>Four modifications for the Raft consensus algorithm</i> by Henrik
 * Ingo.
 *
 * @see VoteRequest
 */
public interface PreVoteRequest extends RaftMessage {

    @Nonnegative
    int getLastLogTerm();

    @Nonnegative
    long getLastLogIndex();

    /**
     * The builder interface for {@link PreVoteRequest}.
     */
    interface PreVoteRequestBuilder extends RaftMessageBuilder<PreVoteRequest> {

        @Nonnull
        PreVoteRequestBuilder setGroupId(@Nonnull Object groupId);

        @Nonnull
        PreVoteRequestBuilder setSender(@Nonnull RaftEndpoint sender);

        @Nonnull
        PreVoteRequestBuilder setTerm(@Nonnegative int term);

        @Nonnull
        PreVoteRequestBuilder setLastLogTerm(@Nonnegative int lastLogTerm);

        @Nonnull
        PreVoteRequestBuilder setLastLogIndex(@Nonnegative long lastLogIndex);

    }

}
