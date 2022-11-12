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

import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;

/**
 * Thrown when an operation cannot be temporarily replicated. It can occur in
 * one of the following cases:
 * <ul>
 * <li>There are too many inflight (i.e., appended but not-yet-committed)
 * operations in the Raft group leader,</li>
 * <li>There are too many inflight (i.e., pending at the Raft leader to be
 * executed) queries,</li>
 * <li>A new membership change is attempted before an entry is committed in the
 * current term.</li>
 * </ul>
 *
 * @see RaftConfig#getMaxPendingLogEntryCount()
 */
public class CannotReplicateException extends RaftException {

    private static final long serialVersionUID = 4407025930140337716L;

    public CannotReplicateException(RaftEndpoint leader) {
        super("Cannot replicate new operations for now", leader);
    }

    @Override
    public String toString() {
        return "CannotReplicateException{leader=" + getLeader() + "}";
    }

}
