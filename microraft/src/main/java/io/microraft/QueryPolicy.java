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
 * Policies to decide how a query operation will be executed on the state machine.
 */
public enum QueryPolicy {

    /**
     * Runs the query on the local state machine of any Raft group member.
     * <p>
     * Reading stale value is possible when a follower lags behind the leader.
     * <p>
     * {@link #LEADER_LOCAL} should be preferred if it's important to read up-to-date data mostly.
     */
    ANY_LOCAL,

    /**
     * Runs the query on the local state machine of the Raft group leader.
     * <p>
     * If the leader is split from the other Raft nodes and a new leader is elected already, stale values can be read for {@code
     * LEADER_LOCAL} queries until the stale-leader Raft node turns into a follower.
     * <p>
     * This policy is much more likely to hit more recent state when compared to the {@link #ANY_LOCAL} policy.
     */
    LEADER_LOCAL,

    /**
     * Runs the query in a linearizable manner, by using the algorithm defined in the <i>Section: 6.4 Processing read-only queries
     * more efficiently</i> of the Raft dissertation. In short, linearizable queries are handled via a round of AppendEntries RPC
     * between the leader and followers without appending an entry to the internal Raft log.
     * <p>
     * Since, a given query is executed by the leader Raft node for both {@link #LEADER_LOCAL} and this policy, both policies have
     * the same processing cost. However, this policy guarantees linearizability with an extra cost of 1 RTT latency overhead
     * compared to the {@link #LEADER_LOCAL} policy.
     */
    LINEARIZABLE
}
