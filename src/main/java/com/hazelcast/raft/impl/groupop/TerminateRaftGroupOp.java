package com.hazelcast.raft.impl.groupop;

/**
 * Terminates a Raft group. Termination means that once this operation is
 * committed to the Raft group, no new operation will be replicated or no new
 * query will be executed.
 *
 * @author mdogan
 * @author metanet
 */
public class TerminateRaftGroupOp
        extends RaftGroupOp {

    private static final long serialVersionUID = -3342502717645211289L;

    public TerminateRaftGroupOp() {
    }

    @Override
    public String toString() {
        return "TerminateRaftGroupOp{}";
    }
}
