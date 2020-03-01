package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.RaftMsg;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.log.SnapshotEntry;
import com.hazelcast.raft.impl.msg.AppendEntriesFailureResponse;
import com.hazelcast.raft.impl.msg.AppendEntriesSuccessResponse;
import com.hazelcast.raft.impl.msg.InstallSnapshotRequest;
import com.hazelcast.raft.impl.task.RaftNodeStatusAwareTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hazelcast.raft.RaftRole.FOLLOWER;

/**
 * Handles an {@link InstallSnapshotRequest} sent by the Raft group leader.
 * Responds with either an {@link AppendEntriesSuccessResponse}
 * or an {@link AppendEntriesFailureResponse}.
 * <p>
 * See <i>7 Log compaction</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @author mdogan
 * @author metanet
 * @see InstallSnapshotRequest
 * @see AppendEntriesSuccessResponse
 * @see AppendEntriesFailureResponse
 */
public class InstallSnapshotRequestHandler
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstallSnapshotRequestHandler.class);

    private final InstallSnapshotRequest request;

    public InstallSnapshotRequestHandler(RaftNodeImpl raftNode, InstallSnapshotRequest request) {
        super(raftNode);
        this.request = request;
    }

    @Override
    protected void doRun() {
        LOGGER.debug("{} Received {}", localEndpointName(), request);

        SnapshotEntry snapshot = request.snapshot();

        // Reply false if term < currentTerm (§5.1)
        if (request.term() < state.term()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.warn(localEndpointName() + " Stale snapshot: " + request + " received in current term: " + state.term());
            }

            RaftMsg response = new AppendEntriesFailureResponse(localEndpoint(), state.term(), snapshot.index() + 1);
            node.send(response, request.leader());
            return;
        }

        // Transform into follower if a newer term is seen or another node wins the election of the current term
        if (request.term() > state.term() || state.role() != FOLLOWER) {
            // If the request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

            LOGGER.info("{} Demoting to FOLLOWER from current role: {}, term: {} to new term: {} and leader: {}",
                    localEndpointName(), state.role(), state.term(), request.term(), request.leader());

            node.toFollower(request.term());
        }

        if (!request.leader().equals(state.leader())) {
            LOGGER.info("{} Setting leader: {}", localEndpointName(), request.leader());
            node.leader(request.leader());
        }

        if (node.installSnapshot(snapshot)) {
            RaftMsg response = new AppendEntriesSuccessResponse(localEndpoint(), request.term(), snapshot.index(),
                    request.queryRound());
            node.send(response, request.leader());
        }
    }
}
