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
 * Policies to decide how a query operation will be executed on the state
 * machine. Each policy offers a different consistency guarantee.
 *
 * @see RaftNode#query(Object, QueryPolicy, java.util.Optional,
 *      java.util.Optional)
 */
public enum QueryPolicy {

    /**
     * Runs the query on the local state machine of any Raft node.
     * <p>
     * Reading stale value is likely if queries are issued on follower or learner
     * Raft nodes while the leader Raft node is committing new operations.
     * <p>
     * Callers can achieve monotonic reads by keeping track of highest commit index
     * they observed via return values of RaftNode's methods and passing it to
     * queries. In this case, RaftNode executes a given query only if its local
     * commit index is greater than equal to the given commit index.
     */
    EVENTUAL_CONSISTENCY,

    /**
     * Runs the query on the local state machine of the leader Raft node.
     * <p>
     * The leader Raft node executes a given query operation with this policy only
     * if it has received AppendEntries RPC responses from the majority of the Raft
     * group in the last leader heartbeat duration.
     * <p>
     * This policy is much more likely to hit more recent state when compared to the
     * {@link #EVENTUAL_CONSISTENCY} policy. However, it cannot guarantee
     * linearizability since other Raft nodes' clocks may move forward in time and
     * they can elect a new leader among themselves while the old leader still
     * considers itself as the leader.
     */
    LEADER_LEASE,

    /**
     * Runs the query in a linearizable manner on the leader Raft node by using the
     * algorithm defined in the <i>Section: 6.4 Processing read-only queries more
     * efficiently</i> of the Raft dissertation. In short, linearizable queries are
     * handled via a round of AppendEntries RPC between the leader and others
     * without appending an entry to the internal Raft log.
     * <p>
     * Since, a given query is executed by the leader Raft node for both
     * {@link #LEADER_LEASE} and this policy, both policies have the same processing
     * cost. However, this policy guarantees linearizability with an extra cost of 1
     * RTT latency overhead compared to the {@link #LEADER_LEASE} policy.
     */
    LINEARIZABLE
}
