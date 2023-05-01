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

import static java.util.Objects.requireNonNull;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import io.microraft.RaftEndpoint;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.report.RaftTerm;
import io.microraft.report.RaftTermMetrics;

/**
 * Contains a snapshot of a Raft node's current state in a term.
 */
public final class RaftTermState implements RaftTerm {

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

    private final RaftTermMetricsState metrics;

    private RaftTermState(int term, RaftEndpoint leaderEndpoint, RaftEndpoint votedEndpoint,
            RaftTermMetricsState metrics) {
        this.term = term;
        this.leaderEndpoint = leaderEndpoint;
        this.votedEndpoint = votedEndpoint;
        this.metrics = metrics;
    }

    public static RaftTermState initial(@Nonnegative long termStartTimeTsMs) {
        return new RaftTermState(0, null, null, new RaftTermMetricsState(termStartTimeTsMs));
    }

    public static RaftTermState restore(@Nonnull RaftTermPersistentState persistentState) {
        return new RaftTermState(persistentState.getTerm(), null, persistentState.getVotedFor(),
                new RaftTermMetricsState(persistentState.getTermStartTsMs()).withVote(persistentState.getVoteTsMs()));
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

    @Override
    @Nonnull
    public RaftTermMetrics getMetrics() {
        return metrics;
    }

    public RaftTermState switchTo(int newTerm, long termSwitchTsMs) {
        assert newTerm >= term : "New term: " + newTerm + ", current term: " + term;
        RaftEndpoint votedEndpoint = newTerm > term ? null : this.votedEndpoint;
        return new RaftTermState(newTerm, null, votedEndpoint,
                newTerm > term ? new RaftTermMetricsState(termSwitchTsMs) : metrics);
    }

    public RaftTermState grantVote(int term, RaftEndpoint votedEndpoint, long voteTsMs) {
        requireNonNull(votedEndpoint);
        assert this.term == term
                : "current term: " + this.term + " voted term: " + term + " voted for: " + votedEndpoint;
        assert this.votedEndpoint == null : "current term: " + this.term + " already voted for: " + this.votedEndpoint
                + " new vote to: " + votedEndpoint;

        return new RaftTermState(this.term, this.leaderEndpoint, votedEndpoint, metrics.withVote(voteTsMs));
    }

    public RaftTermState withLeader(RaftEndpoint leaderEndpoint, long tsMs) {
        assert this.leaderEndpoint == null || leaderEndpoint == null : "current term: " + this.term + " current "
                + "leader: " + this.leaderEndpoint + " new " + "leader: " + leaderEndpoint;
        return new RaftTermState(this.term, leaderEndpoint, this.votedEndpoint,
                leaderEndpoint != null ? metrics.withLeaderSet(tsMs) : metrics.withLeaderReset(tsMs));
    }

    public RaftTermPersistentState.RaftTermPersistentStateBuilder populate(
            @Nonnull RaftTermPersistentState.RaftTermPersistentStateBuilder builder) {
        return builder.setTerm(getTerm()).setVotedFor(getVotedEndpoint()).setTermStartTsMs(metrics.getTermStartTsMs())
                .setVoteTsMs(metrics.getVoteTsMs());
    }

    @Override
    public String toString() {
        return "RaftGroupTerm{" + "term=" + term + ", leaderEndpoint=" + leaderEndpoint + ", votedEndpoint="
                + votedEndpoint + '}';
    }

}
