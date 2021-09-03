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
 * The roles of Raft nodes as defined in the Raft consensus algorithm.
 * <p>
 * At any given time each Raft node is in one of roles defined here.
 */
public enum RaftRole {

    /**
     * A leader handles client requests, commit operations and orchestrates a Raft
     * group.
     */
    LEADER,

    /**
     * When a follower starts a new leader election, it first turns into a
     * candidate. A candidate becomes leader if it wins the majority votes.
     */
    CANDIDATE,

    /**
     * Followers issue no requests on their own. They respond to append-entries
     * requests sent by leaders. Followers are also voting members. They respond to
     * vote requests sent by candidates. They can also run queries if
     * {@link QueryPolicy#EVENTUAL_CONSISTENCY} or
     * {@link QueryPolicy#BOUNDED_STALENESS} is used.
     */
    FOLLOWER,

    /**
     * Learners issue no requests on their own. They only respond to append-entries
     * requests sent by leaders. Learners are non-voting members. They do not turn
     * into candidates and do not receive vote requests from other candidates during
     * leader election rounds. They can also run queries if
     * {@link QueryPolicy#EVENTUAL_CONSISTENCY} or
     * {@link QueryPolicy#BOUNDED_STALENESS} is used.
     */
    LEARNER

}
