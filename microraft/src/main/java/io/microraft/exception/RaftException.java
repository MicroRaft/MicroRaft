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

package io.microraft.exception;

import io.microraft.RaftEndpoint;

/**
 * Base exception class for Raft-related exceptions.
 */
public class RaftException
        extends RuntimeException {

    private static final long serialVersionUID = 3165333502175586105L;

    private final RaftEndpoint leader;

    public RaftException(Throwable cause) {
        super(cause);
        this.leader = null;
    }

    public RaftException(RaftEndpoint leader) {
        this.leader = leader;
    }

    public RaftException(String message, RaftEndpoint leader) {
        super(message);
        this.leader = leader;
    }

    public RaftException(String message, RaftEndpoint leader, Throwable cause) {
        super(message, cause);
        this.leader = leader;
    }

    /**
     * Returns the leader endpoint of the related Raft group, if available and
     * known by the Raft node by the time this exception is thrown.
     *
     * @return the leader endpoint of the related Raft group, if available and
     *         known by the Raft node by the time this exception is thrown
     */
    public RaftEndpoint getLeader() {
        return leader;
    }

    @Override
    public String toString() {
        return "RaftException{leader=" + getLeader() + "}";
    }

}
