package io.microraft.model.impl.persistence;

import static java.util.Objects.requireNonNull;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.microraft.RaftEndpoint;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState.RaftTermPersistentStateBuilder;

/**
 * The default impl of the {@link RaftTermPersistentState} and
 * {@link RaftTermPersistentStateBuilder} interfaces. When an instance of this
 * class is created, it is in the builder mode and its state is populated. Once
 * all fields are set, the object switches to the DTO mode where it no longer
 * allows mutations.
 * <p>
 * Please note that {@link #build()} does not verify if all fields are set or
 * not. It is up to the user to populate the DTO state via the builder.
 */
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
