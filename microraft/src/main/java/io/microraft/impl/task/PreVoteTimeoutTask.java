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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.microraft.RaftRole.FOLLOWER;

/**
 * Scheduled by {@link PreVoteTask} to trigger pre-voting again if the local
 * Raft node is still a follower and a leader is not yet available after
 * the leader election timeout.
 *
 * @author mdogan
 * @author metanet
 */
public final class PreVoteTimeoutTask
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreVoteTimeoutTask.class);

    private int term;

    public PreVoteTimeoutTask(RaftNodeImpl raftNode, int term) {
        super(raftNode);
        this.term = term;
    }

    @Override
    protected void doRun() {
        if (state.role() != FOLLOWER) {
            return;
        }

        LOGGER.debug("{} Pre-vote for term: {} has timed out!", localEndpointStr(), node.state().term());
        new PreVoteTask(node, term).run();
    }

}
