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

package io.microraft.report;

import io.microraft.RaftEndpoint;

import javax.annotation.Nullable;

/**
 * Contains a snapshot of a Raft node's current state in a term.
 */
public interface RaftTerm {

    /**
     * Returns the term this Raft node is currently at.
     *
     * @return the term this Raft node is currently at
     */
    int getTerm();

    /**
     * Returns the known Raft leader endpoint in the current term, or null if unknown.
     *
     * @return the known Raft leader endpoint in the current term, or null if unknown
     */
    @Nullable
    RaftEndpoint getLeaderEndpoint();

    /**
     * Returns the Raft endpoint that this Raft node has voted for in the current term, or null if none.
     *
     * @return the Raft endpoint that this Raft node has voted for in the current term, or null if none
     */
    @Nullable
    RaftEndpoint getVotedEndpoint();

}
