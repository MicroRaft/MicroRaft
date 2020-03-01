package com.hazelcast.raft;

/**
 * Types of membership changes that occur on Raft groups.
 *
 * @author mdogan
 * @author metanet
 */
public enum MembershipChangeMode {

    /**
     * Denotes that a new Raft endpoint will be added to the Raft group.
     */
    ADD,

    /**
     * Denotes that a Raft endpoint will be removed from the Raft group.
     */
    REMOVE
}
