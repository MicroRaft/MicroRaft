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
import io.microraft.RaftRole;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Represents member list of a Raft group with an index identifying on which log index the given member list is appended
 * to the Raft log.
 * <p>
 * The initial member list of a Raft group has log index of 0.
 */
public interface RaftGroupMembers {

    /**
     * The maximum number of {@link RaftRole#LEARNER} members allowed in the Raft group member list.
     */
    int MAX_LEARNER_COUNT = 2;

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

    /**
     * Returns voting members in the Raft group member list.
     *
     * @return voting members in the Raft group member list
     */
    @Nonnull
    Collection<RaftEndpoint> getVotingMembers();

    /**
     * Returns the majority quorum size of the Raft group member list.
     *
     * @return the majority quorum size of the Raft group member list
     */
    int getMajorityQuorumSize();

}
