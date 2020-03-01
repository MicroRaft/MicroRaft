package com.hazelcast.raft;

/**
 * Roles of Raft nodes.
 * <p>
 * At any given time each Raft node is in one of three roles:
 * {@link #LEADER}, {@link #FOLLOWER}, or {@link #CANDIDATE}.
 * <p>
 * In steady state, there is one leader and all of the other Raft nodes are
 * followers, but during splits there can be multiple Raft nodes thinking that
 * they are the leader but only one of them will be legitimate and the stale
 * other leaders will not be able to perform any operation on followers.
 * In addition, there can be multiple candidates during leader elections.
 *
 * @author mdogan
 * @author metanet
 */
public enum RaftRole {

    /**
     * Followers are passive. They issue no requests on their own
     * and they just respond to requests sent by leaders and candidates.
     */
    FOLLOWER,

    /**
     * When a follower starts a new leader election, it first turns into
     * a candidate. A candidate becomes leader when it wins the majority votes.
     */
    CANDIDATE,

    /**
     * The leader handles all client requests and replicates them to followers.
     */
    LEADER
}
