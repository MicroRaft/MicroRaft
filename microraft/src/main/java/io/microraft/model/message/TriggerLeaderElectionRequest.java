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
 * Raft message for the leadership transfer logic.
 * <p>
 * See <i>4.2.3 Disruptive servers</i> section of of the Raft dissertation.
 */
public interface TriggerLeaderElectionRequest
        extends RaftMessage {

    int getLastLogTerm();

    long getLastLogIndex();

    /**
     * The builder interface for {@link TriggerLeaderElectionRequest}.
     */
    interface TriggerLeaderElectionRequestBuilder
            extends RaftMessageBuilder<TriggerLeaderElectionRequest> {

        @Nonnull TriggerLeaderElectionRequestBuilder setGroupId(@Nonnull Object groupId);

        @Nonnull TriggerLeaderElectionRequestBuilder setSender(@Nonnull RaftEndpoint sender);

        @Nonnull TriggerLeaderElectionRequestBuilder setTerm(int term);

        @Nonnull TriggerLeaderElectionRequestBuilder setLastLogTerm(int lastLogTerm);

        @Nonnull TriggerLeaderElectionRequestBuilder setLastLogIndex(long lastLogIndex);

    }

}
