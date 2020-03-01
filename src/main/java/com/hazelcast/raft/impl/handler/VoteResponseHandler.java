package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftIntegration;
import com.hazelcast.raft.RaftRole;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.msg.VoteRequest;
import com.hazelcast.raft.impl.msg.VoteResponse;
import com.hazelcast.raft.impl.state.CandidateState;
import com.hazelcast.raft.impl.state.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hazelcast.raft.RaftRole.CANDIDATE;

/**
 * Handles a {@link VoteResponse} sent for a {@link VoteRequest}.
 * <p>
 * Changes the local Raft member's role to {@link RaftRole#LEADER} via
 * {@link RaftState#toLeader()} if the majority vote is granted for this term.
 * <p>
 * Appends the no-op entry which is returned via
 * {@link RaftIntegration#getOperationToAppendAfterLeaderElection()}.
 * <p>
 * See <i>5.2 Leader election</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @author mdogan
 * @author metanet
 * @see VoteRequest
 * @see VoteResponse
 */
public class VoteResponseHandler
        extends BaseResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(VoteResponseHandler.class);

    private final VoteResponse response;

    public VoteResponseHandler(RaftNodeImpl raftNode, VoteResponse response) {
        super(raftNode);
        this.response = response;
    }

    @Override
    protected void handleResponse() {
        if (state.role() != CANDIDATE) {
            LOGGER.info("{} Ignored {}. We are not CANDIDATE anymore.", localEndpointName(), response);
            return;
        }

        if (response.term() > state.term()) {
            LOGGER.info("{} Demoting to FOLLOWER from current term: {} to new term: {} after {}", localEndpointName(),
                    state.term(), response.term(), response);
            node.toFollower(response.term());
            return;
        }

        if (response.term() < state.term()) {
            LOGGER.warn("{} Stale {} is received, current term: {}", localEndpointName(), response, state.term());
            return;
        }

        CandidateState candidateState = state.candidateState();
        if (response.granted() && candidateState.grantVote(response.voter())) {
            LOGGER.info("{} Vote granted from {} for term: {}, number of votes: {}, majority: {}", localEndpointName(),
                    response.voter(), state.term(), candidateState.voteCount(), candidateState.majority());
        }

        if (candidateState.isMajorityGranted()) {
            LOGGER.info("{} We are the LEADER!", localEndpointName());
            node.toLeader();
        }
    }

    @Override
    protected RaftEndpoint sender() {
        return response.voter();
    }
}
