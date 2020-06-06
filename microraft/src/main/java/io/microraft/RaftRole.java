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

package io.microraft;

/**
 * The roles of Raft nodes as defined in the Raft consensus algorithm.
 * <p>
 * At any given time each Raft node is in one of three roles: {@link #LEADER},
 * {@link #FOLLOWER}, or {@link #CANDIDATE}.
 */
public enum RaftRole {

    /**
     * Followers are passive. They issue no requests on their own
     * and they just respond to requests sent by leaders and candidates.
     * They can also handle queries if the {@link QueryPolicy#ANY_LOCAL} policy
     * is used.
     */
    FOLLOWER,

    /**
     * When a follower starts a new leader election, it first turns into
     * a candidate. A candidate becomes leader if it wins the majority votes.
     */
    CANDIDATE,

    /**
     * A leader handles client requests, commit operations and orchestrates
     * a Raft group.
     */
    LEADER
}
