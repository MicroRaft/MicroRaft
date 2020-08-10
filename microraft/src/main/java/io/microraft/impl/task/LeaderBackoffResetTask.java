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

import io.microraft.RaftEndpoint;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.impl.state.FollowerState;
import io.microraft.impl.state.LeaderState;

import java.util.Map.Entry;

/**
 * If the append entries request backoff period is active for any follower,
 * this task will send a new append entries request on the backoff
 * completion.
 */
public class LeaderBackoffResetTask
        extends RaftNodeStatusAwareTask {

    public LeaderBackoffResetTask(RaftNodeImpl node) {
        super(node);
    }

    @Override
    protected void doRun() {
        LeaderState leaderState = state.leaderState();
        if (leaderState == null) {
            return;
        }

        leaderState.requestBackoffResetTaskScheduled(false);

        for (Entry<RaftEndpoint, FollowerState> e : leaderState.getFollowerStates().entrySet()) {
            FollowerState followerState = e.getValue();
            if (followerState.isRequestBackoffSet()) {
                if (followerState.completeBackoffRound()) {
                    node.sendAppendEntriesRequest(e.getKey());
                } else {
                    node.scheduleLeaderRequestBackoffResetTask(leaderState);
                }
            }
        }
    }

}
