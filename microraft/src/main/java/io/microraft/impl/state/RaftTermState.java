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

package io.microraft.impl.state;

import io.microraft.RaftEndpoint;
import io.microraft.report.RaftTerm;

import static java.util.Objects.requireNonNull;

/**
 * Contains a snapshot of a Raft node's current state in a term.
 *
 * @author metanet
 */
public final class RaftTermState
        implements RaftTerm {

    public static final RaftTermState INITIAL = new RaftTermState(0, null, null);
    /**
     * Highest term this node has seen.
     * <p>
     * Initialized to 0 on first boot, increases monotonically.
     * <p>
     * [PERSISTENT]
     */
    private final int term;
    /**
     * Latest known leader endpoint (or null if not known).
     */
    private final RaftEndpoint leaderEndpoint;
    /**
     * Endpoint that received vote in the current term, or null if none.
     * <p>
     * [PERSISTENT]
     */
    private final RaftEndpoint votedEndpoint;

    private RaftTermState(int term, RaftEndpoint leaderEndpoint, RaftEndpoint votedEndpoint) {
        this.term = term;
        this.leaderEndpoint = leaderEndpoint;
        this.votedEndpoint = votedEndpoint;
    }

    public static RaftTermState restore(int term, RaftEndpoint votedEndpoint) {
        return new RaftTermState(term, null, votedEndpoint);
    }

    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public RaftEndpoint getLeaderEndpoint() {
        return leaderEndpoint;
    }

    @Override
    public RaftEndpoint getVotedEndpoint() {
        return votedEndpoint;
    }

    public RaftTermState switchTo(int newTerm) {
        assert newTerm >= term : "New term: " + newTerm + ", current term: " + term;

        RaftEndpoint votedEndpoint = newTerm > term ? null : this.votedEndpoint;

        return new RaftTermState(newTerm, null, votedEndpoint);
    }

    public RaftTermState grantVote(int term, RaftEndpoint votedEndpoint) {
        requireNonNull(votedEndpoint);
        assert this.term == term : "current term: " + this.term + " voted term: " + term + " voted for: " + votedEndpoint;
        assert this.votedEndpoint == null : "current term: " + this.term + " already voted for: " + this.votedEndpoint
                + " new vote to: " + votedEndpoint;

        return new RaftTermState(this.term, this.leaderEndpoint, votedEndpoint);
    }

    public RaftTermState withLeader(RaftEndpoint leaderEndpoint) {
        assert this.leaderEndpoint == null || leaderEndpoint == null : "current term: " + this.term + " current " + "leader: "
                + this.leaderEndpoint + " new " + "leader: " + leaderEndpoint;

        return new RaftTermState(this.term, leaderEndpoint, this.votedEndpoint);
    }

    @Override
    public String toString() {
        return "RaftGroupTerm{" + "term=" + term + ", leaderEndpoint=" + leaderEndpoint + ", votedEndpoint=" + votedEndpoint
                + '}';
    }

}
