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

import javax.annotation.Nonnull;
import javax.annotation.Nonnegative;

/**
 * Response for {@link PreVoteRequest}.
 * <p>
 * See <i>Four modifications for the Raft consensus algorithm</i> by Henrik
 * Ingo.
 *
 * @see PreVoteRequest
 * @see VoteResponse
 */
public interface PreVoteResponse extends RaftMessage {

    boolean isGranted();

    /**
     * The builder interface for {@link PreVoteResponse}.
     */
    interface PreVoteResponseBuilder extends RaftMessageBuilder<PreVoteResponse> {

        @Nonnull
        PreVoteResponseBuilder setGroupId(@Nonnull Object groupId);

        @Nonnull
        PreVoteResponseBuilder setSender(@Nonnull RaftEndpoint sender);

        @Nonnull
        PreVoteResponseBuilder setTerm(@Nonnegative int term);

        @Nonnull
        PreVoteResponseBuilder setGranted(boolean granted);

    }

}
