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
import io.microraft.impl.log.RaftLog;
import io.microraft.impl.state.LeaderState;

/**
 * Flushes the leader Raft node's local Raft log and tries to advance
 * the commit index.
 * <p>
 * Silently returns if the Raft node is no longer the leader.
 */
public class LeaderFlushTask
        extends RaftNodeStatusAwareTask {

    public LeaderFlushTask(RaftNodeImpl node) {
        super(node);
    }

    @Override
    protected void doRun() {
        RaftLog log = state.log();
        log.flush();

        LeaderState leaderState = state.leaderState();
        if (leaderState == null) {
            return;
        }

        leaderState.flushTaskSubmitted(false);
        leaderState.flushedLogIndex(log.lastLogOrSnapshotIndex());

        node.tryAdvanceCommitIndex();
    }

}
