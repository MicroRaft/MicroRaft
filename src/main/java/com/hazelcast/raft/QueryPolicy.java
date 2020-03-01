package com.hazelcast.raft;

/**
 * Policies to decide how a query operation will be executed on the state machine.
 *
 * @author mdogan
 * @author metanet
 */
public enum QueryPolicy {

    /**
     * Runs the query on the local state machine of the Raft group leader.
     * <p>
     * If the leader is split from the  rest and a new leader is elected
     * already, stale values can be read for {@code LEADER_LOCAL} queries until
     * the Raft node turns into a follower.
     * <p>
     * This policy is likely to hit more recent state when compared to
     * {@link #ANY_LOCAL}.
     */
    LEADER_LOCAL,

    /**
     * Runs the query on the local state machine of any Raft group member.
     * <p>
     * Reading stale value is possible when a follower lags behind the leader.
     * <p>
     * {@link #LEADER_LOCAL} should be preferred if it's important to read
     * up-to-date data mostly.
     */
    ANY_LOCAL,

    /**
     * Runs the query in a linearizable manner, either by appending and
     * committing a log entry to the Raft log, or using the algorithm defined
     * in <i>6.4 Processing read-only queries more efficiently</i>
     * section of the Raft dissertation.
     */
    LINEARIZABLE
}
