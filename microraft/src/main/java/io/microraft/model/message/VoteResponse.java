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

/**
 * Response for {@link VoteRequest}.
 * <p>
 * See <i>5.2 Leader election</i> section of <i>In Search of an Understandable Consensus Algorithm</i> paper by <i>Diego
 * Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see VoteRequest
 */
public interface VoteResponse extends RaftMessage {

    boolean isGranted();

    /**
     * The builder interface for {@link VoteResponse}.
     */
    interface VoteResponseBuilder extends RaftMessageBuilder<VoteResponse> {

        @Nonnull
        VoteResponseBuilder setGroupId(@Nonnull Object groupId);

        @Nonnull
        VoteResponseBuilder setSender(@Nonnull RaftEndpoint sender);

        @Nonnull
        VoteResponseBuilder setTerm(int term);

        @Nonnull
        VoteResponseBuilder setGranted(boolean granted);

    }

}
