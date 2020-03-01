package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.msg.PreVoteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * PreVoteTask is scheduled when current leader is null, unreachable
 * or unknown. It sends {@link PreVoteRequest}s to other members to receive
 * make sure majority is reachable and ready to elect a new leader.
 * <p>
 * Also a {@link PreVoteTimeoutTask} is scheduled with a
 * {@link RaftNodeImpl#getRandomizedLeaderElectionTimeoutMs()} delay to trigger
 * pre-voting if a leader is not available yet.
 *
 * @author mdogan
 * @author metanet
 */
public class PreVoteTask
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreVoteTask.class);

    private int term;

    public PreVoteTask(RaftNodeImpl raftNode, int term) {
        super(raftNode);
        this.term = term;
    }

    @Override
    protected void doRun() {
        if (state.leader() != null) {
            LOGGER.debug("{} No new pre-vote phase, we already have a LEADER: {}", localEndpointName(), state.leader());
            return;
        } else if (state.term() != term) {
            LOGGER.debug("{} No new pre-vote phase for term: {} because of new term: {}", localEndpointName(), term,
                    state.term());
            return;
        }

        Collection<RaftEndpoint> remoteMembers = state.remoteMembers();
        if (remoteMembers.isEmpty()) {
            LOGGER.debug("{} Remote members is empty. No need for pre-voting.", localEndpointName());
            return;
        }

        node.preCandidate();
    }

}
