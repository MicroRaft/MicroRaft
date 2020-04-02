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

package io.microraft.report;

import io.microraft.RaftEndpoint;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Keeps member list of a Raft group with an index identifying on which log
 * index the given member list is appended to the Raft log.
 * <p>
 * The initial member list of a Raft group has index of 0.
 *
 * @author metanet
 */
public interface RaftGroupMembers {

    /**
     * Returns the Raft log index that contains this Raft group member list.
     *
     * @return the Raft log index that contains this Raft group member list
     */
    long getLogIndex();

    /**
     * Returns the member list of the Raft group.
     *
     * @return the member list of the Raft group
     */
    @Nonnull
    Collection<RaftEndpoint> getMembers();

}
