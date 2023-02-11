package io.microraft.model.impl.persistence;

import io.microraft.RaftEndpoint;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState.RaftTermPersistentStateBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.Nonnegative;

import static java.util.Objects.requireNonNull;

public class DefaultRaftTermPersistentStateOrBuilder
        implements
            RaftTermPersistentState,
            RaftTermPersistentStateBuilder {

    private int term;
    private RaftEndpoint votedFor;
    private DefaultRaftTermPersistentStateOrBuilder builder = this;

    @Nonnegative
    @Override
    public int getTerm() {
        return term;
    }

    @Override
    @Nullable
    public RaftEndpoint getVotedFor() {
        return votedFor;
    }

    @Override
    @Nonnull
    public RaftTermPersistentStateBuilder setTerm(@Nonnegative int term) {
        builder.term = term;
        return this;
    }

    @Override
    @Nonnull
    public RaftTermPersistentStateBuilder setVotedFor(@Nullable RaftEndpoint votedFor) {
        builder.votedFor = votedFor;
        return this;
    }

    @Override
    @Nonnull
    public RaftTermPersistentState build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        String header = builder != null ? "DefaultRaftTermPersistentStateOrBuilder" : "DefaultRaftTermPersistentState";
        return header + "{" + "term=" + term + ", votedFor=" + votedFor + '}';
    }

}
