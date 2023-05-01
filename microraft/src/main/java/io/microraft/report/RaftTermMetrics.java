package io.microraft.report;

public interface RaftTermMetrics {

    long getTermStartTsMs();

    long getLeaderSetTsMs();

    long getLeaderResetTsMs();

    long getVoteTsMs();

}
