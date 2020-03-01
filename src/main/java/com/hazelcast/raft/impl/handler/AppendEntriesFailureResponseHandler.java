package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.msg.AppendEntriesFailureResponse;
import com.hazelcast.raft.impl.msg.AppendEntriesRequest;
import com.hazelcast.raft.impl.msg.InstallSnapshotRequest;
import com.hazelcast.raft.impl.state.FollowerState;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.raft.impl.state.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hazelcast.raft.RaftRole.LEADER;

/**
 * Handles an {@link AppendEntriesFailureResponse} which can be sent
 * as a response to a previous append-entries request or an install-snapshot
 * request.
 * <p>
 * Decrements {@code nextIndex} of the follower by 1 if the response is valid.
 * <p>
 * See <i>5.3 Log replication</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @author mdogan
 * @author metanet
 * @see AppendEntriesRequest
 * @see InstallSnapshotRequest
 */
public class AppendEntriesFailureResponseHandler
        extends BaseResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppendEntriesFailureResponseHandler.class);

    private final AppendEntriesFailureResponse response;

    public AppendEntriesFailureResponseHandler(RaftNodeImpl raftNode, AppendEntriesFailureResponse response) {
        super(raftNode);
        this.response = response;
    }

    @Override
    protected void handleResponse() {
        RaftState state = node.state();

        if (state.role() != LEADER) {
            LOGGER.warn("{} {} is ignored since we are not LEADER.", localEndpointName(), response);
            return;
        }

        if (response.term() > state.term()) {
            // If the request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            LOGGER.info("{} Demoting to FOLLOWER after {} from current term: {}", localEndpointName(), response, state.term());
            node.toFollower(response.term());
            return;
        }

        LOGGER.debug("{} Received {}", localEndpointName(), response);

        if (updateNextIndex(state)) {
            node.sendAppendEntriesRequest(response.follower());
        }
    }

    private boolean updateNextIndex(RaftState state) {
        LeaderState leaderState = state.leaderState();
        FollowerState followerState = leaderState.getFollowerState(response.follower());

        long nextIndex = followerState.nextIndex();
        long matchIndex = followerState.matchIndex();

        if (response.expectedNextIndex() == nextIndex) {
            // Received a response for the last append request. Resetting the flag...
            followerState.resetAppendEntriesRequestBackoff();

            // this is the response of the request I have sent for this nextIndex
            nextIndex--;
            if (nextIndex <= matchIndex) {
                LOGGER.error("{} Cannot decrement next index: {} below match index: {} for follower: {}", localEndpointName(),
                        nextIndex, matchIndex, response.follower());
                return false;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        localEndpointName() + " Updating next index: " + nextIndex + " for follower: " + response.follower());
            }

            followerState.nextIndex(nextIndex);
            return true;
        }

        return false;
    }

    @Override
    protected RaftEndpoint sender() {
        return response.follower();
    }
}
