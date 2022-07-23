/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright (c) 2020, MicroRaft.
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

package io.microraft;

import javax.annotation.Nonnull;

/**
 * Represents an endpoint that participates to at least one Raft group and executes the Raft consensus algorithm with a
 * {@link RaftNode} instance.
 * <p>
 * For the Raft algorithm implementation, it is sufficient to differentiate members of a Raft group with a unique id,
 * and that is why we only have a single method in this interface. It is users' responsibility to assign unique ids to
 * different Raft endpoints.
 */
public interface RaftEndpoint {

    /**
     * Returns the unique identifier of the Raft endpoint.
     *
     * @return the unique identifier of the Raft endpoint
     */
    @Nonnull
    Object getId();

}
