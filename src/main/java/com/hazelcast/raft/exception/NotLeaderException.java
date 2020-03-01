package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftEndpoint;

/**
 * Thrown when an operation, query, or a membership change is triggered
 * on a non-leader Raft group member. In this case, the operation can be
 * retried on another member of the Raft group.
 *
 * @author mdogan
 * @author metanet
 */
public class NotLeaderException
        extends RaftException
        implements RetryableState {

    private static final long serialVersionUID = 1817579502149525710L;

    public NotLeaderException(RaftEndpoint local, RaftEndpoint leader) {
        super(local + " is not LEADER. Known leader is: " + (leader != null ? leader : "N/A"), leader);
    }
}
