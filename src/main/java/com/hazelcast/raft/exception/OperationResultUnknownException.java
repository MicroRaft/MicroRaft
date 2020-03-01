package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftEndpoint;

/**
 * Thrown when an installed snapshot causes an appended operation to be
 * truncated from the Raft log before its commit status is discovered.
 * This can happen when a Raft group leader appends an operation locally and
 * loses the leadership, then a new leader is elected and installs a snapshot
 * to the previous leader. In this case, the previous leader cannot detect if
 * the operation is actually committed or overwritten by another operation
 * committed on the same log index by the new leader.
 *
 * @author mdogan
 * @author metanet
 * @see IndeterminateState
 */
public class OperationResultUnknownException
        extends RaftException
        implements IndeterminateState {

    private static final long serialVersionUID = -736303015926722821L;

    public OperationResultUnknownException(RaftEndpoint leader) {
        super(leader);
    }
}
