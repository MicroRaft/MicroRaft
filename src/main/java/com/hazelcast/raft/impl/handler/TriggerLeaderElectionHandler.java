package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.msg.TriggerLeaderElectionRequest;
import com.hazelcast.raft.impl.task.LeaderElectionTask;
import com.hazelcast.raft.impl.task.RaftNodeStatusAwareTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a {@link TriggerLeaderElectionRequest} that and initiates a new
 * leader election round if this node accepts the sender as the leader and
 * the local Raft log is up-to-date with the leader's Raft log.
 *
 * @author mdogan
 * @author metanet
 * @see TriggerLeaderElectionRequest
 */
public class TriggerLeaderElectionHandler
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerLeaderElectionHandler.class);

    private final TriggerLeaderElectionRequest req;

    public TriggerLeaderElectionHandler(RaftNodeImpl raftNode, TriggerLeaderElectionRequest req) {
        super(raftNode);
        this.req = req;
    }

    @Override
    protected void doRun() {
        LOGGER.debug("{} Received {}", localEndpointName(), req);

        // Verify the term and the leader.
        // If the requesting leader is legit,
        // I will eventually accept it as the leader with a periodic append request.
        // Once I pass this if block, I know that I am follower and my log is same
        // with the leader's log.
        if (!(req.term() == state.term() && req.leader().equals(state.leader()))) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} Ignoring {} since term: {} and leader: {}", localEndpointName(), req, state.term(),
                        state.leader());
            }

            return;
        }

        // Verify the last log entry
        LogEntry entry = state.log().lastLogOrSnapshotEntry();
        if (!(entry.index() == req.lastLogIndex() && entry.term() == req.lastLogTerm())) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "{} Could not accept leadership transfer because local Raft log is not same with the current leader. "
                                + "Last log entry: {}, request: {}", localEndpointName(), entry, req);
            }

            return;
        }

        // I will send a disruptive VoteRequest to bypass leader stickiness
        LOGGER.info("{} Starting a new leader election since the current leader: {} in term: {} asked for a leadership transfer!",
                localEndpointName(), req.leader(), req.term());
        state.leader(null);
        new LeaderElectionTask(node, true).run();
    }

}
