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

import io.microraft.RaftEndpoint;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.task.PreVoteTask;
import io.microraft.model.log.BaseLogEntry;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.model.message.PreVoteResponse;
import io.microraft.model.message.PreVoteResponse.PreVoteResponseBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static io.microraft.RaftRole.LEARNER;
import static java.util.Objects.requireNonNull;

/**
 * Handles a {@link PreVoteRequest} and responds to the sender with a
 * {@link PreVoteResponse}. Pre-voting is initiated by {@link PreVoteTask}.
 * <p>
 * Rejects to grant a "pre-vote" if the local Raft node has recently heard from
 * the current Raft group leader, or has a longer Raft log than the sender, or
 * has a bigger term. If none of these conditions hold, then a "pre-vote" is
 * granted. A "pre-vote" means that if the sender starts a new election, the
 * local Raft member could vote for it.
 * <p>
 * This task is a read-only task and does not mutate the local Raft node state.
 *
 * @see PreVoteRequest
 * @see PreVoteResponse
 * @see PreVoteTask
 */
public class PreVoteRequestHandler extends AbstractMessageHandler<PreVoteRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreVoteRequestHandler.class);

    public PreVoteRequestHandler(RaftNodeImpl raftNode, PreVoteRequest request) {
        super(raftNode, request);
    }

    @Override
    protected void handle(@Nonnull PreVoteRequest request) {
        requireNonNull(request);

        RaftEndpoint localEndpoint = localEndpoint();
        RaftEndpoint candidate = request.getSender();
        int nextTerm = request.getTerm();

        PreVoteResponseBuilder responseBuilder = modelFactory.createPreVoteResponseBuilder()
                .setGroupId(node.getGroupId()).setSender(localEndpoint);

        // Reply false if term < currentTerm (§5.1)
        if (state.term() > nextTerm) {
            LOGGER.info("{} Rejecting {} since current term: {} is bigger.", localEndpointStr(), request, state.term());
            node.send(candidate, responseBuilder.setTerm(state.term()).setGranted(false).build());
            if (state.leaderState() != null) {
                node.sendAppendEntriesRequest(candidate);
            }

            return;
        }

        // Reply false if last AppendEntries call was received recently (leader
        // stickiness)
        if (state.leaderState() != null || !node.isLeaderHeartbeatTimeoutElapsed()) {
            LOGGER.info("{} Rejecting {} since the leader is still alive...", localEndpointStr(), request);
            node.send(candidate, responseBuilder.setTerm(state.term()).setGranted(false).build());
            return;
        }

        BaseLogEntry lastLogEntry = state.log().lastLogOrSnapshotEntry();
        if (lastLogEntry.getTerm() > request.getLastLogTerm()) {
            LOGGER.info("{} Rejecting {} since our last log term: {} is greater.", localEndpointStr(), request,
                    lastLogEntry.getTerm());
            node.send(candidate, responseBuilder.setTerm(nextTerm).setGranted(false).build());
            return;
        }

        if (lastLogEntry.getTerm() == request.getLastLogTerm() && lastLogEntry.getIndex() > request.getLastLogIndex()) {
            LOGGER.info("{} Rejecting {} since our last log index: {} is greater.", localEndpointStr(), request,
                    lastLogEntry.getIndex());
            node.send(candidate, responseBuilder.setTerm(nextTerm).setGranted(false).build());
            return;
        }

        if (state.role() == LEARNER) {
            LOGGER.info("{} is {} but {} asked for pre-vote.", localEndpointStr(), LEARNER, candidate.getId());
        }

        LOGGER.info("{} Granted pre-vote for {}", localEndpointStr(), request);
        node.send(candidate, responseBuilder.setTerm(nextTerm).setGranted(true).build());
    }

}
