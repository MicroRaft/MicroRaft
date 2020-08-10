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

package io.microraft.impl.task;

import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.handler.PreVoteResponseHandler;
import io.microraft.impl.state.RaftState;
import io.microraft.model.message.VoteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduled when the current leader is null, unreachable, or unknown by
 * {@link PreVoteResponseHandler} after a follower receives votes from the
 * majority. A Raft node becomes a candidate via
 * {@link RaftState#toCandidate()} and sends {@link VoteRequest}s to the other
 * Raft group members.
 * <p>
 * Also a {@link LeaderElectionTimeoutTask} is scheduled with the
 * {@link RaftNodeImpl#getLeaderElectionTimeoutMs()} delay to trigger another
 * round of leader election if a leader is not elected yet.
 */
public final class LeaderElectionTask
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionTask.class);

    private final boolean sticky;

    public LeaderElectionTask(RaftNodeImpl raftNode, boolean sticky) {
        super(raftNode);
        this.sticky = sticky;
    }

    @Override
    protected void doRun() {
        if (state.leader() != null) {
            LOGGER.warn("{} No new election round, we already have a LEADER: {}", localEndpointStr(), state.leader().getId());
            return;
        }

        node.toCandidate(sticky);
    }

}
