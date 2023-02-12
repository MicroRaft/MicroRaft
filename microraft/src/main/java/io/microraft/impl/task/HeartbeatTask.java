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

import static io.microraft.RaftRole.FOLLOWER;
import static io.microraft.RaftRole.LEARNER;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.microraft.RaftEndpoint;
import io.microraft.impl.RaftNodeImpl;

/**
 * Checks whether currently there is a known leader endpoint and triggers the
 * pre-voting mechanism there is no known leader or the leader has timed out.
 */
public class HeartbeatTask extends RaftNodeStatusAwareTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatTask.class);

    public HeartbeatTask(RaftNodeImpl node) {
        super(node);
    }

    @Override
    protected void doRun() {
        try {
            if (state.leaderState() != null) {
                if (!node.demoteToFollowerIfQuorumHeartbeatTimeoutElapsed()) {
                    node.broadcastAppendEntriesRequest();
                    // TODO(basri) append no-op if snapshotIndex > 0 && snapshotIndex ==
                    // lastLogIndex
                }

                return;
            }

            RaftEndpoint leader = state.leader();
            if (leader == null) {
                if (state.role() == FOLLOWER && state.preCandidateState() == null) {
                    LOGGER.warn("{} We are FOLLOWER and there is no current leader. Will start new election round.",
                            localEndpointStr());
                    resetLeaderAndTryTriggerPreVote(false);
                }
            } else if (node.isLeaderHeartbeatTimeoutElapsed() && state.preCandidateState() == null) {
                LOGGER.warn("{} Current leader {}'s heartbeats are timed-out.", localEndpointStr(), leader.getId());
                resetLeaderAndTryTriggerPreVote(true);
            } else if (!state.committedGroupMembers().isKnownMember(leader) && state.preCandidateState() == null) {
                LOGGER.warn("{} Current leader {} is not member anymore.", localEndpointStr(), leader.getId());
                resetLeaderAndTryTriggerPreVote(true);
            }
        } finally {
            node.getExecutor().schedule(this, node.getConfig().getLeaderHeartbeatPeriodSecs(), SECONDS);
        }
    }

    void resetLeaderAndTryTriggerPreVote(boolean resetLeader) {
        if (resetLeader) {
            node.leader(null);
        }

        if (state.role() == LEARNER) {
            LOGGER.debug("{} is not starting pre-vote since it is {}", localEndpointStr(), LEARNER);
            return;
        }

        if (state.leaderElectionQuorumSize() > 1) {
            node.runPreVote();
        } else if (state.effectiveGroupMembers().getVotingMembers().contains(localEndpoint())) {
            // we can encounter this case if the leader crashes before it
            // commit the replicated membership change while it is leaving.
            LOGGER.info("{} is the single voting member left in the Raft group.", localEndpointStr());
            node.toSingletonLeader();
        }
    }

}
