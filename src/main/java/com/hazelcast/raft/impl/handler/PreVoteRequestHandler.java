package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.msg.PreVoteRequest;
import com.hazelcast.raft.impl.msg.PreVoteResponse;
import com.hazelcast.raft.impl.task.PreVoteTask;
import com.hazelcast.raft.impl.task.RaftNodeStatusAwareTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a {@link PreVoteRequest} and responds to the sender
 * with a {@link PreVoteResponse}. Pre-voting is initiated by
 * {@link PreVoteTask}.
 * <p>
 * Rejects to grant a "pre-vote" if this node has recently heart from
 * the current Raft group leader, or has a longer Raft log than
 * the sender, or has a bigger term. If none of these conditions hold,
 * then a "pre-vote" is granted. A "pre-vote" means that if the sender
 * starts a new election, the local Raft member could vote for it.
 * <p>
 * This task is a read-only task and does not mutate local state.
 *
 * @author mdogan
 * @author metanet
 * @see PreVoteRequest
 * @see PreVoteResponse
 * @see PreVoteTask
 */
public class PreVoteRequestHandler
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreVoteRequestHandler.class);

    private final PreVoteRequest request;

    public PreVoteRequestHandler(RaftNodeImpl raftNode, PreVoteRequest request) {
        super(raftNode);
        this.request = request;
    }

    @Override
    protected void doRun() {
        RaftEndpoint localEndpoint = localEndpoint();

        // Reply false if term < currentTerm (§5.1)
        if (state.term() > request.nextTerm()) {
            LOGGER.info("{} Rejecting {} since current term: {} is bigger.", localEndpointName(), request, state.term());
            node.send(new PreVoteResponse(localEndpoint, state.term(), false), request.candidate());
            return;
        }

        // Reply false if last AppendEntries call was received less than election timeout ago (leader stickiness)
        if (node.isAppendEntriesRequestReceivedRecently()) {
            LOGGER.info("{} Rejecting {} since received append entries recently.", localEndpointName(), request);
            node.send(new PreVoteResponse(localEndpoint, state.term(), false), request.candidate());
            return;
        }

        LogEntry lastLogEntry = state.log().lastLogOrSnapshotEntry();
        if (lastLogEntry.term() > request.lastLogTerm()) {
            LOGGER.info("{} Rejecting {} since our last log term: {} is greater.", localEndpointName(), request,
                    lastLogEntry.term());
            node.send(new PreVoteResponse(localEndpoint, request.nextTerm(), false), request.candidate());
            return;
        }

        if (lastLogEntry.term() == request.lastLogTerm() && lastLogEntry.index() > request.lastLogIndex()) {
            LOGGER.info("{} Rejecting {} since our last log index: {} is greater.", localEndpointName(), request,
                    lastLogEntry.index());
            node.send(new PreVoteResponse(localEndpoint, request.nextTerm(), false), request.candidate());
            return;
        }

        LOGGER.info("{} Granted pre-vote for {}", localEndpointName(), request);
        node.send(new PreVoteResponse(localEndpoint, request.nextTerm(), true), request.candidate());
    }
}
