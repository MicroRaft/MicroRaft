/*
 * Copyright (c) 2026, MicroRaft.
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

package io.microraft.exception;

import io.microraft.QueryPolicy;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import java.util.Optional;

/**
 * Thrown when the leader cannot run a linearizable
 * {@link RaftNode#query(Object, QueryPolicy, Optional, Optional)} call because
 * its state machine has not applied the log up to the query's read index. This
 * happens only after applying committed entries failed, for example because
 * taking a snapshot threw. Please see the <i>Section: 6.4 Processing read-only
 * queries more efficiently</i> of the Raft dissertation for more details.
 */
public class LaggingStateMachineException extends RaftException {

    private static final long serialVersionUID = 4120473307165244251L;

    /**
     * Creates an instance of this exception.
     *
     * @param lastApplied
     *            the last log index applied to the state machine of the Raft
     *            node which throws this exception
     * @param readIndex
     *            the read index of the query
     * @param leader
     *            the leader endpoint if it is known, null otherwise
     */
    public LaggingStateMachineException(long lastApplied, long readIndex, RaftEndpoint leader) {
        super("Last applied index: " + lastApplied + " is smaller than the read index: " + readIndex, leader);
    }

    @Override
    public String toString() {
        return "LaggingStateMachineException{leader=" + getLeader() + "}";
    }

}
