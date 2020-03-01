package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.handler.PreVoteResponseHandler;
import com.hazelcast.raft.impl.msg.VoteRequest;
import com.hazelcast.raft.impl.state.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LeaderElectionTask is scheduled when current leader is null, unreachable
 * or unknown by {@link PreVoteResponseHandler} after a follower receives
 * votes from at least majority. Local member becomes a candidate using
 * {@link RaftState#toCandidate(boolean)} and sends {@link VoteRequest}s to other
 * members.
 * <p>
 * Also a {@link LeaderElectionTimeoutTask} is scheduled with a
 * {@link RaftNodeImpl#getRandomizedLeaderElectionTimeoutMs()} delay to trigger
 * leader election if one is not elected yet.
 *
 * @author mdogan
 * @author metanet
 */
public class LeaderElectionTask
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionTask.class);

    private boolean disruptive;

    public LeaderElectionTask(RaftNodeImpl raftNode, boolean disruptive) {
        super(raftNode);
        this.disruptive = disruptive;
    }

    @Override
    protected void doRun() {
        if (state.leader() != null) {
            LOGGER.warn("{} No new election round, we already have a LEADER: {}", localEndpointName(), state.leader());
            return;
        }

        node.toCandidate(disruptive);
    }

}
