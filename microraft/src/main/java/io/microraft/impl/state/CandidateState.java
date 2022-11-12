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

package io.microraft.impl.state;

import java.util.HashSet;
import java.util.Set;

/**
 * State maintained by each candidate during the pre-voting and voting phases.
 */
public final class CandidateState {

    private final int majority;
    private final Set<Object> voters = new HashSet<>();

    CandidateState(int majority) {
        this.majority = majority;
    }

    /**
     * Persists vote for the endpoint during election. This method is idempotent,
     * multiple votes from the same point are counted only once.
     *
     * @return false if endpoint is already voted, true otherwise
     */
    public boolean grantVote(Object address) {
        return voters.add(address);
    }

    /**
     * Returns true if the majority votes are granted, false otherwise.
     */
    public boolean isMajorityGranted() {
        return voteCount() >= majority();
    }

    /**
     * Returns the number of the majority votes.
     */
    public int majority() {
        return majority;
    }

    /**
     * Returns current granted number of the votes.
     */
    public int voteCount() {
        return voters.size();
    }

}
