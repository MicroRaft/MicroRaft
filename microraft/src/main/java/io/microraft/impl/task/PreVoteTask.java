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

package io.microraft.impl.task;

import io.microraft.impl.RaftNodeImpl;
import io.microraft.model.message.PreVoteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduled when the current leader is null, unreachable, or unknown.
 * <p>
 * It sends {@link PreVoteRequest}s to other Raft group members to make sure
 * the current leader is considered to be unhealthy by the majority and a new
 * leader election round can be started.
 * <p>
 * Also a {@link PreVoteTimeoutTask} is scheduled with the
 * {@link RaftNodeImpl#getLeaderElectionTimeoutMs()} delay to trigger another
 * round of pre-voting if a leader is not available yet.
 */
public final class PreVoteTask
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreVoteTask.class);

    private int term;

    public PreVoteTask(RaftNodeImpl raftNode, int term) {
        super(raftNode);
        this.term = term;
    }

    @Override
    protected void doRun() {
        if (state.leader() != null) {
            LOGGER.debug("{} No new pre-vote phase, we already have a LEADER: {}", localEndpointStr(), state.leader().getId());
            return;
        } else if (state.term() != term) {
            LOGGER.debug("{} No new pre-vote phase for term: {} because of new term: {}", localEndpointStr(), term, state.term());
            return;
        }

        if (state.remoteMembers().isEmpty()) {
            LOGGER.debug("{} Remote members is empty. No need for pre-voting.", localEndpointStr());
            return;
        }

        node.preCandidate();
    }

}
