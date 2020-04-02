/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright 2020, MicroRaft.
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

import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.state.CandidateState;
import io.microraft.impl.task.LeaderElectionTask;
import io.microraft.impl.task.PreVoteTask;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.model.message.PreVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static io.microraft.RaftRole.FOLLOWER;

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
        extends AbstractResponseHandler<PreVoteResponse> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreVoteResponseHandler.class);

    public PreVoteResponseHandler(RaftNodeImpl raftNode, PreVoteResponse response) {
        super(raftNode, response);
    }

    @Override
    protected void handleResponse(@Nonnull PreVoteResponse response) {
        LOGGER.debug("{} received {}.", localEndpointStr(), response);

        if (state.role() != FOLLOWER) {
            LOGGER.info("{} Ignored {}. We are not FOLLOWER anymore.", localEndpointStr(), response);
            return;
        }

        if (response.getTerm() < state.term()) {
            LOGGER.warn("{} Stale {} is received, current term: {}", localEndpointStr(), response, state.term());
            return;
        }

        CandidateState preCandidateState = state.preCandidateState();
        if (preCandidateState == null) {
            LOGGER.debug("{} Ignoring {}. We are not interested in pre-votes anymore.", localEndpointStr(), response);
            return;
        }

        if (response.isGranted() && preCandidateState.grantVote(response.getSender())) {
            LOGGER.info("{} Pre-vote granted from {} for term: {}, number of votes: {}, majority: {}", localEndpointStr(),
                        response.getSender().getId(), response.getTerm(), preCandidateState.voteCount(),
                        preCandidateState.majority());
        }

        if (preCandidateState.isMajorityGranted()) {
            LOGGER.info("{} We have the majority during pre-vote phase. Let's start real election!", localEndpointStr());
            new LeaderElectionTask(node, true).run();
        }
    }

}
