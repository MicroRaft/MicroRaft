/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright 2020, MicroRaft.
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
import io.microraft.model.RaftModel;

import javax.annotation.Nonnull;

/**
 * Implemented by request and response classes of the Raft consensus
 * algorithm RPCs. Raft messages are the objects that go back and forth between
 * Raft nodes.
 * <p>
 * Raft message implementations must be treated as immutable and once
 * a Raft message object is created its contents must not be mutated.
 *
 * @author mdogan
 * @author metanet
 */
public interface RaftMessage
        extends RaftModel {

    /**
     * Returns the group id of the Raft node which created this message
     *
     * @return the group id of the Raft node which created this message
     */
    Object getGroupId();

    /**
     * Returns the endpoint of the Raft node which created this message
     *
     * @return the endpoint of the Raft node which created this message
     */
    @Nonnull
    RaftEndpoint getSender();

    /**
     * Returns the term at which the Raft node created this message
     *
     * @return the term at which the Raft node created this message
     */
    int getTerm();

    /**
     * The base builder interface for Raft message classes
     *
     * @param <T>
     *         the concrete type of the Raft message
     */
    interface RaftMessageBuilder<T extends RaftMessage> {

        @Nonnull
        T build();

    }

}
