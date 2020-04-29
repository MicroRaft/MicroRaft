/*
 * Copyright (c) 2020, MicroRaft.
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

/**
 * Thrown when a Raft node's current commit index is smaller than the commit
 * index specified in a {@link RaftNode#query(Object, QueryPolicy, long)} call.
 * This exception means that the Raft node instance cannot execute the given
 * query by preserving the monotonicity of the observed state. Please see
 * the <i>Section: 6.4 Processing read-only queries more efficiently</i> of
 * the Raft dissertation for more details.
 */
public class LaggingCommitIndexException
        extends RaftException {

    private static final long serialVersionUID = -2244714904905721002L;

    public LaggingCommitIndexException(long commitIndex, long expectedCommitIndex, RaftEndpoint leader) {
        super("Commit index: " + commitIndex + " is smaller than min commit index: " + expectedCommitIndex, leader);
    }

    @Override
    public String toString() {
        return "LaggingCommitIndexException{leader=" + getLeader() + "}";
    }

}
