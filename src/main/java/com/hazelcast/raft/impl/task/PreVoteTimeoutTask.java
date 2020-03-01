package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.impl.RaftNodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hazelcast.raft.RaftRole.FOLLOWER;

/**
 * PreVoteTimeoutTask is scheduled by {@link PreVoteTask} to trigger pre-voting
 * again if this node is still a follower and a leader is not available after
 * leader election timeout.
 *
 * @author mdogan
 * @author metanet
 */
public class PreVoteTimeoutTask
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreVoteTimeoutTask.class);

    private int term;

    public PreVoteTimeoutTask(RaftNodeImpl raftNode, int term) {
        super(raftNode);
        this.term = term;
    }

    @Override
    protected void doRun() {
        if (state.role() != FOLLOWER) {
            return;
        }

        LOGGER.debug("{} Pre-vote for term: {} has timed out!", localEndpointName(), node.state().term());
        new PreVoteTask(node, term).run();
    }
}
