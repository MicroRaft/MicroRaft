package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.msg.PreVoteRequest;
import com.hazelcast.raft.impl.msg.PreVoteResponse;
import com.hazelcast.raft.impl.state.CandidateState;
import com.hazelcast.raft.impl.task.LeaderElectionTask;
import com.hazelcast.raft.impl.task.PreVoteTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hazelcast.raft.RaftRole.FOLLOWER;

/**
 * Handles a {@link PreVoteResponse} sent for a {@link PreVoteRequest}.
 * <p>
 * Initiates a new leader election by executing {@link LeaderElectionTask}
 * if the Raft group majority grants "pre-votes" for this pre-voting term.
 *
 * @author mdogan
 * @author metanet
 * @see PreVoteResponse
 * @see PreVoteTask
 * @see LeaderElectionTask
 */
public class PreVoteResponseHandler
        extends BaseResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreVoteResponseHandler.class);

    private final PreVoteResponse response;

    public PreVoteResponseHandler(RaftNodeImpl raftNode, PreVoteResponse response) {
        super(raftNode);
        this.response = response;
    }

    @Override
    protected void handleResponse() {
        if (state.role() != FOLLOWER) {
            LOGGER.info("{} Ignored {}. We are not FOLLOWER anymore.", localEndpointName(), response);
            return;
        }

        if (response.term() < state.term()) {
            LOGGER.warn("{} Stale {} is received, current term: {}", localEndpointName(), response, state.term());
            return;
        }

        CandidateState preCandidateState = state.preCandidateState();
        if (preCandidateState == null) {
            LOGGER.debug("{} Ignoring {}. We are not interested in pre-votes anymore.", localEndpointName(), response);
            return;
        }

        if (response.granted() && preCandidateState.grantVote(response.voter())) {
            LOGGER.info("{} Pre-vote granted from {} for term: {}, number of votes: {}, majority: {}", localEndpointName(),
                    response.voter(), response.term(), preCandidateState.voteCount(), preCandidateState.majority());
        }

        if (preCandidateState.isMajorityGranted()) {
            LOGGER.info("{} We have the majority during pre-vote phase. Let's start real election!", localEndpointName());
            new LeaderElectionTask(node, false).run();
        }
    }

    @Override
    protected RaftEndpoint sender() {
        return response.voter();
    }
}
