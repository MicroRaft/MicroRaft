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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.microraft.RaftRole.CANDIDATE;

/**
 * Scheduled by {@link LeaderElectionTask} to trigger leader election again if a leader is not elected yet.
 */
public final class LeaderElectionTimeoutTask
        extends RaftNodeStatusAwareTask
        implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionTimeoutTask.class);

    public LeaderElectionTimeoutTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override protected void doRun() {
        if (state.role() != CANDIDATE) {
            return;
        }

        LOGGER.warn("{} Leader election for term: {} has timed out!", localEndpointStr(), node.state().term());
        new LeaderElectionTask(node, true).run();
    }

}
