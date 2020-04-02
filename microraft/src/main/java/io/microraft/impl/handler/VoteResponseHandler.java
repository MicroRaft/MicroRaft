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

import io.microraft.RaftRole;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.state.CandidateState;
import io.microraft.impl.state.RaftState;
import io.microraft.integration.StateMachine;
import io.microraft.model.message.VoteRequest;
import io.microraft.model.message.VoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static io.microraft.RaftRole.CANDIDATE;

/**
 * Handles a {@link VoteResponse} sent for a {@link VoteRequest}.
 * <p>
 * Changes the local Raft node's role to {@link RaftRole#LEADER} via
 * {@link RaftState#toLeader()} if the majority vote has been granted for this
 * term.
 * <p>
 * In the beginning of the new term, the Raft group leader appends a new log
 * entry that contains an operation which is returned via
 * {@link StateMachine#getNewTermOperation()}.
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
        extends AbstractResponseHandler<VoteResponse> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VoteResponseHandler.class);

    public VoteResponseHandler(RaftNodeImpl raftNode, VoteResponse response) {
        super(raftNode, response);
    }

    @Override
    protected void handleResponse(@Nonnull VoteResponse response) {
        if (state.role() != CANDIDATE) {
            LOGGER.debug("{} Ignored {}. We are not CANDIDATE anymore.", localEndpointStr(), response);
            return;
        } else if (response.getTerm() > state.term()) {
            // If the response term is greater than the local term, update the local term and convert to follower (§5.1)
            LOGGER.info("{} Demoting to FOLLOWER from current term: {} to new term: {} after {}", localEndpointStr(),
                        state.term(), response.getTerm(), response);
            node.toFollower(response.getTerm());
            return;
        } else if (response.getTerm() < state.term()) {
            LOGGER.warn("{} Stale {} is received, current term: {}", localEndpointStr(), response, state.term());
            return;
        }

        CandidateState candidateState = state.candidateState();
        if (response.isGranted() && candidateState.grantVote(response.getSender())) {
            LOGGER.info("{} Vote granted from {} for term: {}, number of votes: {}, majority: {}", localEndpointStr(),
                        response.getSender().getId(), state.term(), candidateState.voteCount(), candidateState.majority());
        }

        if (candidateState.isMajorityGranted()) {
            LOGGER.info("{} We are the LEADER!", localEndpointStr());
            node.toLeader();
        }
    }

}
