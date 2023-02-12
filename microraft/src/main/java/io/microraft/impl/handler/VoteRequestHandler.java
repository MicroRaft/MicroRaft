/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright (c) 2020, MicroRaft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.microraft.impl.handler;

import static io.microraft.RaftRole.LEARNER;
import static java.util.Objects.requireNonNull;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.microraft.RaftEndpoint;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.task.LeaderElectionTask;
import io.microraft.model.log.BaseLogEntry;
import io.microraft.model.message.VoteRequest;
import io.microraft.model.message.VoteResponse;
import io.microraft.model.message.VoteResponse.VoteResponseBuilder;

/**
 * Handles a {@link VoteRequest} sent by a candidate and responds with a
 * {@link VoteResponse}.
 * <p>
 * Leader election is initiated by {@link LeaderElectionTask}.
 * <p>
 * See <i>5.2 Leader election</i> section of <i>In Search of an Understandable
 * Consensus Algorithm</i> paper by <i>Diego Ongaro</i> and <i>John
 * Ousterhout</i>.
 *
 * @see VoteRequest
 * @see VoteResponse
 * @see LeaderElectionTask
 */
public class VoteRequestHandler extends AbstractMessageHandler<VoteRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VoteRequestHandler.class);

    public VoteRequestHandler(RaftNodeImpl raftNode, VoteRequest request) {
        super(raftNode, request);
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    // Justification: It is easier to follow the RequestVoteRPC logic in a single
    // method
    protected void handle(@Nonnull VoteRequest request) {
        requireNonNull(request);

        VoteResponseBuilder responseBuilder = modelFactory.createVoteResponseBuilder().setGroupId(node.getGroupId())
                .setSender(localEndpoint());

        RaftEndpoint candidate = request.getSender();
        int candidateTerm = request.getTerm();

        // Reply false if term < currentTerm (§5.1)
        if (state.term() > candidateTerm) {
            LOGGER.info("{} Rejecting {} since current term: {} is bigger.", localEndpointStr(), request, state.term());
            node.send(candidate, responseBuilder.setTerm(state.term()).setGranted(false).build());
            if (state.leaderState() != null) {
                node.sendAppendEntriesRequest(candidate);
            }

            return;
        }

        // (Raft thesis - Section 4.2.3) This check conflicts with the leadership
        // transfer mechanism,
        // in which a server legitimately starts an election without waiting a leader
        // heartbeat timeout.
        // Those VoteRequest objects are marked as "non-sticky" to bypass leader
        // stickiness.
        // Also if the request comes from the current leader, then the leader stickiness
        // check is skipped.
        // Since the current leader may have restarted by recovering its persistent
        // state.
        if (request.isSticky() && (state.leaderState() != null || !node.isLeaderHeartbeatTimeoutElapsed())
                && !candidate.equals(state.leader())) {
            LOGGER.info("{} Rejecting {} since the leader is still alive...", localEndpointStr(), request);
            node.send(candidate, responseBuilder.setTerm(state.term()).setGranted(false).build());
            return;
        }

        if (state.term() < candidateTerm) {
            // If the request term is greater than the local term, update the local term and
            // convert to follower (§5.1)
            LOGGER.info("{} Moving to new term: {} from current term: {} after {}", localEndpointStr(), candidateTerm,
                    state.term(), request);

            node.toFollower(candidateTerm);
        }

        if (state.leader() != null && !candidate.equals(state.leader())) {
            LOGGER.warn("{} Rejecting {} since we have a leader: {}", localEndpointStr(), request,
                    state.leader().getId());
            node.send(candidate, responseBuilder.setTerm(candidateTerm).setGranted(false).build());

            return;
        }

        if (state.votedEndpoint() != null) {
            boolean granted = (candidate.equals(state.votedEndpoint()));
            if (granted) {
                LOGGER.debug("{} Vote granted for {} (duplicate)", localEndpointStr(), request);
            } else {
                LOGGER.debug("{} no vote for {}. currently voted-for: {}", localEndpointStr(), request,
                        state.votedEndpoint().getId());
            }
            node.send(candidate, responseBuilder.setTerm(candidateTerm).setGranted(granted).build());
            return;
        }

        BaseLogEntry lastLogEntry = state.log().lastLogOrSnapshotEntry();
        if (lastLogEntry.getTerm() > request.getLastLogTerm()) {
            LOGGER.info("{} Rejecting {} since our last log term: {} is greater.", localEndpointStr(), request,
                    lastLogEntry.getTerm());
            node.send(candidate, responseBuilder.setTerm(candidateTerm).setGranted(false).build());
            return;
        }

        if (lastLogEntry.getTerm() == request.getLastLogTerm() && lastLogEntry.getIndex() > request.getLastLogIndex()) {
            LOGGER.info("{} Rejecting {} since our last log index: {} is greater.", localEndpointStr(), request,
                    lastLogEntry.getIndex());
            node.send(candidate, responseBuilder.setTerm(candidateTerm).setGranted(false).build());
            return;
        }

        if (state.role() == LEARNER) {
            LOGGER.info("{} is {} but {} asked for vote.", localEndpointStr(), LEARNER, candidate.getId());
        }

        LOGGER.info("{} Granted vote for {}", localEndpointStr(), request);
        state.grantVote(candidateTerm, candidate);

        node.send(candidate, responseBuilder.setTerm(candidateTerm).setGranted(true).build());
    }

}
