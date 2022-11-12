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

/**
 * Types of membership changes that occur on Raft groups.
 */
public enum MembershipChangeMode {

    /**
     * Denotes that a new Raft endpoint will be added to the Raft group as a
     * {@link RaftRole#LEARNER}. The quorum size of the Raft group will not change
     * after the new Raft endpoint is added, since the number of voting members
     * stays the same. Once the new Raft endpoint catches up with the leader, it can
     * be promoted to the voting member role via
     * {@link #ADD_OR_PROMOTE_TO_FOLLOWER}.
     */
    ADD_LEARNER,

    /**
     * Denotes that either a new Raft endpoint will be added to the Raft group as a
     * {@link RaftRole#FOLLOWER}, or an existing (i.e., {@link RaftRole#LEARNER}
     * Raft endpoint in the Raft group will be promoted to the
     * {@link RaftRole#FOLLOWER} role. The quorum size of the Raft group is
     * re-calculated based on the new number of voting members.
     */
    ADD_OR_PROMOTE_TO_FOLLOWER,

    /**
     * Denotes that a Raft endpoint will be removed from the Raft group.
     */
    REMOVE_MEMBER

}
