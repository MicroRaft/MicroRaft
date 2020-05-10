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

import io.microraft.RaftEndpoint;
import io.microraft.impl.RaftNodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.microraft.RaftRole.FOLLOWER;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Checks whether currently there is a known leader endpoint and triggers
 * the pre-voting mechanism there is no known leader or the leader has
 * timed out.
 *
 * @author mdogan
 * @author metanet
 */
public class HeartbeatTask
        extends RaftNodeStatusAwareTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatTask.class);

    public HeartbeatTask(RaftNodeImpl node) {
        super(node);
    }

    @Override
    protected void doRun() {
        try {
            if (state.leaderState() != null) {
                if (!node.demoteToFollowerIfMajorityHeartbeatTimeoutElapsed()) {
                    node.broadcastAppendEntriesRequest();
                    // TODO [basri] append no-op if snapshotIndex > 0 && snapshotIndex == lastLogIndex
                }

                return;
            }

            RaftEndpoint leader = state.leader();
            if (leader == null) {
                if (state.role() == FOLLOWER && state.preCandidateState() == null) {
                    LOGGER.warn("{} We are FOLLOWER and there is no current leader. Will start new election round",
                                localEndpointStr());
                    node.runPreVote();
                }
            } else if (node.isLeaderHeartbeatTimeoutElapsed() && state.preCandidateState() == null) {
                LOGGER.warn("{} Current leader {}'s heartbeats are timed-out. Will start new election round.", localEndpointStr(),
                            leader.getId());
                resetLeaderAndTriggerPreVote();
            } else if (!state.committedGroupMembers().isKnownMember(leader) && state.preCandidateState() == null) {
                LOGGER.warn("{} Current leader {} is not member anymore. Will start new election round.", localEndpointStr(),
                            leader.getId());
                resetLeaderAndTriggerPreVote();
            }
        } finally {
            node.getExecutor().schedule(this, node.getConfig().getLeaderHeartbeatPeriodSecs(), SECONDS);
        }
    }

    void resetLeaderAndTriggerPreVote() {
        node.leader(null);
        node.runPreVote();
    }

}
