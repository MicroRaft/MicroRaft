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

package io.microraft.exception;

import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;

/**
 * A Raft leader may demote to the follower role after it appends an entry to
 * its local Raft log, but before discovering its commit status. In some of
 * these cases, which cause this exception to be thrown, it may happen that
 * the Raft node cannot determine if the operation is committed or not.
 * In this case, the {@code CompletableFuture} objects returned for these
 * operations are notified with this exception.
 * <p>
 * It is up to the clients to decide on retry upon receiving this exception.
 * If the operation is retried either on the same or different Raft node,
 * it could be committed twice, hence causes at-least-once execution.
 * On the contrary, if {@link RaftNode#replicate(Object)} is not called again,
 * then at-most-once execution happens.
 * <p>
 * Idempotent operations can be retried on indeterminate situations.
 *
 * @author mdogan
 * @author metanet
 */
public class IndeterminateStateException
        extends RaftException {

    private static final long serialVersionUID = -736303015926722821L;

    public IndeterminateStateException() {
        this(null);
    }

    public IndeterminateStateException(RaftEndpoint leader) {
        super(leader);
    }

    @Override
    public String toString() {
        return "IndeterminateStateException{leader=" + getLeader() + "}";
    }

}
