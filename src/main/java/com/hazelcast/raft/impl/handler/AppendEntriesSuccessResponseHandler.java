package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.msg.AppendEntriesFailureResponse;
import com.hazelcast.raft.impl.msg.AppendEntriesRequest;
import com.hazelcast.raft.impl.msg.AppendEntriesSuccessResponse;
import com.hazelcast.raft.impl.state.FollowerState;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.raft.impl.state.QueryState;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.InternallyCompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

import static com.hazelcast.raft.RaftNodeStatus.ACTIVE;
import static com.hazelcast.raft.RaftRole.LEADER;
import static java.util.Arrays.sort;

/**
 * Handles an {@link AppendEntriesSuccessResponse} which can be sent
 * as a response to a previous append-entries request or an install-snapshot
 * request.
 * <p>
 * Advances {@link RaftState#commitIndex()} according to match indices
 * of followers.
 * <p>
 * See <i>5.3 Log replication</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @author mdogan
 * @author metanet
 * @see AppendEntriesRequest
 * @see AppendEntriesSuccessResponse
 * @see AppendEntriesFailureResponse
 */
public class AppendEntriesSuccessResponseHandler
        extends BaseResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppendEntriesSuccessResponseHandler.class);

    private final AppendEntriesSuccessResponse response;

    public AppendEntriesSuccessResponseHandler(RaftNodeImpl raftNode, AppendEntriesSuccessResponse response) {
        super(raftNode);
        this.response = response;
    }

    @Override
    protected void handleResponse() {
        if (state.role() != LEADER) {
            LOGGER.warn("{} Ignored {}. We are not LEADER anymore.", localEndpointName(), response);
            return;
        }

        assert response.term() <= state.term() :
                localEndpointName() + " Invalid " + response + " for current term: " + state.term();

        LOGGER.debug("{} Received {}", localEndpointName(), response);

        if (!updateFollowerIndices(state)) {
            tryRunQueries(state);
            return;
        }

        if (!tryAdvanceCommitIndex(state)) {
            trySendAppendRequest(state);
        }
    }

    private boolean updateFollowerIndices(RaftState state) {
        // If successful: update nextIndex and matchIndex for follower (§5.3)

        RaftEndpoint follower = response.follower();
        LeaderState leaderState = state.leaderState();
        FollowerState followerState = leaderState.getFollowerState(follower);
        QueryState queryState = leaderState.queryState();

        if (queryState.tryAck(response.queryRound(), follower)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(localEndpointName() + "Ack from " + follower + " for query round: " + response.queryRound());
            }
        }

        long matchIndex = followerState.matchIndex();
        long followerLastLogIndex = response.lastLogIndex();

        if (followerLastLogIndex > matchIndex) {
            // Received a response for the last append request. Resetting the flag...
            followerState.resetAppendEntriesRequestBackoff();

            long newNextIndex = followerLastLogIndex + 1;
            followerState.matchIndex(followerLastLogIndex);
            followerState.nextIndex(newNextIndex);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        localEndpointName() + " Updated match index: " + followerLastLogIndex + " and next index: " + newNextIndex
                                + " for follower: " + follower);
            }

            return true;
        } else if (followerLastLogIndex < matchIndex) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(localEndpointName() + " Will not update match index for follower: " + follower
                        + ". follower last log index: " + followerLastLogIndex + ", match index: " + matchIndex);
            }
        }

        return false;
    }

    private long findQuorumMatchIndex(RaftState state) {
        LeaderState leaderState = state.leaderState();
        long[] indices = leaderState.matchIndices();

        // if the leader is leaving, it should not count its vote for quorum...
        if (node.state().isKnownMember(localEndpoint())) {
            indices[indices.length - 1] = state.log().lastLogOrSnapshotIndex();
        } else {
            // Remove the last empty slot reserved for leader index
            indices = Arrays.copyOf(indices, indices.length - 1);
        }

        sort(indices);

        long quorumMatchIndex = indices[(indices.length - 1) / 2];
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    localEndpointName() + " Quorum match index: " + quorumMatchIndex + ", indices: " + Arrays.toString(indices));
        }

        return quorumMatchIndex;
    }

    private boolean tryAdvanceCommitIndex(RaftState state) {
        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N (§5.3, §5.4)
        long quorumMatchIndex = findQuorumMatchIndex(state);
        long commitIndex = state.commitIndex();
        RaftLog raftLog = state.log();
        for (; quorumMatchIndex > commitIndex; quorumMatchIndex--) {
            // Only log entries from the leader’s current term are committed by counting replicas; once an entry
            // from the current term has been committed in this way, then all prior entries are committed indirectly
            // because of the Log Matching Property.
            LogEntry entry = raftLog.getLogEntry(quorumMatchIndex);
            if (entry.term() == state.term()) {
                commitEntries(state, quorumMatchIndex);
                return true;
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        localEndpointName() + " Cannot commit " + entry + " since an entry from the current term: " + state.term()
                                + " is needed.");
            }
        }
        return false;
    }

    private void commitEntries(RaftState state, long commitIndex) {
        LOGGER.debug("{} Setting commit index: {}", localEndpointName(), commitIndex);

        state.commitIndex(commitIndex);
        node.broadcastAppendEntriesRequest();

        if (node.getStatus() == ACTIVE) {
            node.applyLogEntries();
            tryRunQueries(state);
        } else {
            tryRunQueries(state);
            node.applyLogEntries();
        }
    }

    private void tryRunQueries(RaftState state) {
        QueryState queryState = state.leaderState().queryState();
        if (queryState.queryCount() == 0) {
            return;
        }

        long commitIndex = state.commitIndex();
        if (!queryState.isMajorityAcked(commitIndex, state.majority())) {
            return;
        } else if (queryState.isAckNeeded(response.follower(), state.majority())) {
            node.sendAppendEntriesRequest(response.follower());
            return;
        }

        Collection<Entry<Object, InternallyCompletableFuture<Object>>> operations = queryState.operations();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(localEndpointName() + " Running " + operations.size() + " queries at commit index: " + commitIndex
                    + ", query round: " + queryState.queryRound());
        }

        operations.forEach(t -> node.runQuery(t.getKey(), t.getValue()));
        queryState.reset();
    }

    private void trySendAppendRequest(RaftState state) {
        long followerLastLogIndex = response.lastLogIndex();
        if (state.log().lastLogOrSnapshotIndex() > followerLastLogIndex || state.commitIndex() == followerLastLogIndex) {
            // If the follower is still missing some log entries or has not learnt the latest commit index yet,
            // then send another append request.
            node.sendAppendEntriesRequest(response.follower());
        }
    }

    @Override
    protected RaftEndpoint sender() {
        return response.follower();
    }
}
