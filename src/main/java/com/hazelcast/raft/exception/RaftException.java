package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftEndpoint;

/**
 * Base exception class for Raft-related exceptions.
 *
 * @author mdogan
 * @author metanet
 */
public class RaftException
        extends RuntimeException {

    private static final long serialVersionUID = 3165333502175586105L;

    private final RaftEndpoint leader;

    public RaftException(RaftEndpoint leader) {
        this.leader = leader;
    }

    public RaftException(String message, RaftEndpoint leader) {
        super(message);
        this.leader = leader;
    }

    public RaftException(String message, RaftEndpoint leader, Throwable cause) {
        super(message, cause);
        this.leader = leader;
    }

    /**
     * Returns the leader member of related Raft group, if known/available
     * by the time this exception is thrown.
     */
    public RaftEndpoint getLeader() {
        return leader;
    }
}
