package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.impl.RaftNodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hazelcast.raft.RaftRole.CANDIDATE;

/**
 * LeaderElectionTimeoutTask is scheduled by {@link LeaderElectionTask}
 * to trigger leader election again if one is not elected after
 * leader election timeout.
 *
 * @author mdogan
 * @author metanet
 */
public class LeaderElectionTimeoutTask
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionTimeoutTask.class);

    public LeaderElectionTimeoutTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected void doRun() {
        if (state.role() != CANDIDATE) {
            return;
        }

        LOGGER.warn("{} Leader election for term: {} has timed out!", localEndpointName(), node.state().term());
        new LeaderElectionTask(node, false).run();
    }
}
