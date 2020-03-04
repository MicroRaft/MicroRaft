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

/**
 * Thrown when an operation, query, or a membership change is triggered
 * on a non-leader Raft node. In this case, the operation can be retried
 * on another Raft node of the Raft group.
 *
 * @author mdogan
 * @author metanet
 */
public class NotLeaderException
        extends RaftException {

    private static final long serialVersionUID = 1817579502149525710L;

    public NotLeaderException(RaftEndpoint local, RaftEndpoint leader) {
        super(local + " is not LEADER. Known leader is: " + (leader != null ? leader : "N/A"), leader);
    }

}
