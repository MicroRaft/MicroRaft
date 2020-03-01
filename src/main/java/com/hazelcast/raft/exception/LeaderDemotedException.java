package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftNode;

/**
 * Thrown when an inflight (i.e., appended but not-yet-committed) operation is
 * truncated from the Raft log by the new leader. This means that the operation
 * is not certainly committed, so it can be replicated again via
 * {@link RaftNode#replicate(Object)}.
 *
 * @author mdogan
 * @author metanet
 * @see RetryableState
 */
public class LeaderDemotedException
        extends RaftException
        implements RetryableState {

    private static final long serialVersionUID = 4284556927980596355L;

    public LeaderDemotedException(RaftEndpoint local, RaftEndpoint leader) {
        super(local + " is not LEADER anymore. Known leader is: " + (leader != null ? leader : "N/A"), leader);
    }
}
