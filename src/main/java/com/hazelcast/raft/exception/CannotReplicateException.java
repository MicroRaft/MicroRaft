package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftEndpoint;

/**
 * Thrown when an operation cannot be temporarily replicated.
 * It can occur in one of the following cases:
 * <ul>
 * <li>There are too many inflight (i.e., appended but not-yet-committed)
 * operations in the Raft group leader,</li>
 * <li>A new membership change is attempted before an entry is committed
 * in the current term.</li>
 * </ul>
 *
 * @author mdogan
 * @author metanet
 * @see RaftConfig#getUncommittedLogEntryCountToRejectNewAppends()
 * @see RetryableState
 */
public class CannotReplicateException
        extends RaftException
        implements RetryableState {

    private static final long serialVersionUID = 4407025930140337716L;

    public CannotReplicateException(RaftEndpoint leader) {
        super("Cannot replicate new operations for now", leader);
    }
}
