package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.msg.VoteRequest;
import com.hazelcast.raft.impl.msg.VoteResponse;
import com.hazelcast.raft.impl.task.LeaderElectionTask;
import com.hazelcast.raft.impl.task.RaftNodeStatusAwareTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hazelcast.raft.RaftRole.FOLLOWER;

/**
 * Handles a {@link VoteRequest} sent by a candidate and responds with
 * a {@link VoteResponse}.
 * Leader election is initiated by {@link LeaderElectionTask}.
 * <p>
 * See <i>5.2 Leader election</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @author mdogan
 * @author metanet
 * @see VoteRequest
 * @see VoteResponse
 * @see LeaderElectionTask
 */
public class VoteRequestHandler
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(VoteRequestHandler.class);

    private final VoteRequest request;

    public VoteRequestHandler(RaftNodeImpl raftNode, VoteRequest request) {
        super(raftNode);
        this.request = request;
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    // Justification: It is easier to follow the RequestVoteRPC logic in a single method
    protected void doRun() {
        RaftEndpoint localEndpoint = localEndpoint();

        // (Raft thesis - Section 4.2.3) This check conflicts with the leadership transfer mechanism,
        // in which a server legitimately starts an election without waiting an election timeout.
        // Those VoteRequest objects are marked with a special flag ("disruptive") to bypass leader stickiness.
        if (!request.isDisruptive() && node.isAppendEntriesRequestReceivedRecently()) {
            LOGGER.info("{} Rejecting {} since received append entries recently.", localEndpointName(), request);
            node.send(new VoteResponse(localEndpoint, state.term(), false), request.candidate());
            return;
        }

        // Reply false if term < currentTerm (§5.1)
        if (state.term() > request.term()) {
            LOGGER.info("{} Rejecting {} since current term: {} is bigger.", localEndpointName(), request, state.term());
            node.send(new VoteResponse(localEndpoint, state.term(), false), request.candidate());
            return;
        }

        if (state.term() < request.term()) {
            // If the request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            if (state.role() != FOLLOWER) {
                LOGGER.info("{} Demoting to FOLLOWER after {} since current term: {} is smaller.", localEndpointName(), request,
                        state.term());
            } else {
                LOGGER.info("{} Moving to new term: {} from current term: {} after {}", localEndpointName(), request.term(),
                        state.term(), request);
            }

            node.toFollower(request.term());
        }

        if (state.leader() != null && !request.candidate().equals(state.leader())) {
            LOGGER.warn("{} Rejecting {} since we have a leader: {}", localEndpointName(), request, state.leader());
            node.send(new VoteResponse(localEndpoint, request.term(), false), request.candidate());
            return;
        }

        if (state.votedFor() != null) {
            boolean granted = (request.candidate().equals(state.votedFor()));
            if (granted) {
                LOGGER.info("{} Vote granted for duplicate {}", localEndpointName(), request);
            } else {
                LOGGER.info("{} Duplicate {}. currently voted-for: {}", localEndpointName(), request, state.votedFor());
            }
            node.send(new VoteResponse(localEndpoint, request.term(), granted), request.candidate());
            return;
        }

        LogEntry lastLogEntry = state.log().lastLogOrSnapshotEntry();
        if (lastLogEntry.term() > request.lastLogTerm()) {
            LOGGER.info("{} Rejecting {} since our last log term: {} is greater.", localEndpointName(), request,
                    lastLogEntry.term());
            node.send(new VoteResponse(localEndpoint, request.term(), false), request.candidate());
            return;
        }

        if (lastLogEntry.term() == request.lastLogTerm() && lastLogEntry.index() > request.lastLogIndex()) {
            LOGGER.info("{} Rejecting {} since our last log index: {} is greater.", localEndpointName(), request,
                    lastLogEntry.index());
            node.send(new VoteResponse(localEndpoint, request.term(), false), request.candidate());
            return;
        }

        LOGGER.info("{} Granted vote for {}", localEndpointName(), request);
        state.persistVote(request.term(), request.candidate());

        node.send(new VoteResponse(localEndpoint, request.term(), true), request.candidate());
    }
}
