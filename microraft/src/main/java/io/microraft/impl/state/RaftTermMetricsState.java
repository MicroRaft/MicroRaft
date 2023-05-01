package io.microraft.impl.state;

import javax.annotation.Nullable;

import io.microraft.report.RaftTermMetrics;

public class RaftTermMetricsState implements RaftTermMetrics {

    private final long termStartTsMs;

    private final long leaderSetTsMs;

    private final long leaderResetTsMs;

    private final long voteTsMs;

    RaftTermMetricsState(long termStartTsMs) {
        this(termStartTsMs, 0, 0, 0);
    }

    private RaftTermMetricsState(long termStartTsMs, long leaderSetTsMs, long leaderResetTsMs, long voteTsMs) {
        this.termStartTsMs = termStartTsMs;
        this.leaderSetTsMs = leaderSetTsMs;
        this.leaderResetTsMs = leaderResetTsMs;
        this.voteTsMs = voteTsMs;
    }

    @Override
    public long getTermStartTsMs() {
        return termStartTsMs;
    }

    @Override
    public long getLeaderSetTsMs() {
        return leaderSetTsMs;
    }

    @Override
    public long getLeaderResetTsMs() {
        return leaderResetTsMs;
    }

    @Override
    public long getVoteTsMs() {
        return voteTsMs;
    }

    public RaftTermMetricsState withLeaderSet(long leaderSetTsMs) {
        return new RaftTermMetricsState(this.termStartTsMs, leaderSetTsMs, this.leaderResetTsMs, this.voteTsMs);
    }

    public RaftTermMetricsState withLeaderReset(long leaderSetTsMs) {
        return new RaftTermMetricsState(this.termStartTsMs, this.leaderSetTsMs, leaderResetTsMs, this.voteTsMs);
    }

    public RaftTermMetricsState withVote(long voteTsMs) {
        return new RaftTermMetricsState(this.termStartTsMs, this.leaderSetTsMs, this.leaderResetTsMs, voteTsMs);
    }

    @Override
    public String toString() {
        return "RaftTermMetricsState [termStartTsMs=" + termStartTsMs + ", leaderSetTsMs=" + leaderSetTsMs
                + ", leaderResetTsMs=" + leaderResetTsMs + ", voteTsMs=" + voteTsMs + "]";
    }

}
