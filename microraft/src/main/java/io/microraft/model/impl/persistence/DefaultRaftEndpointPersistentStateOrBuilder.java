package io.microraft.model.impl.persistence;

import io.microraft.RaftEndpoint;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftEndpointPersistentState.RaftEndpointPersistentStateBuilder;;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * The default impl of the {@link RaftEndpointPersistentState} and
 * {@link RaftEndpointPersistentStateBuilder} interfaces. When an instance of
 * this class is created, it is in the builder mode and its state is populated.
 * Once all fields are set, the object switches to the DTO mode where it no
 * longer allows mutations.
 * <p>
 * Please note that {@link #build()} does not verify if all fields are set or
 * not. It is up to the user to populate the DTO state via the builder.
 */
public class DefaultRaftEndpointPersistentStateOrBuilder
        implements
            RaftEndpointPersistentState,
            RaftEndpointPersistentStateBuilder {

    private RaftEndpoint localEndpoint;
    private boolean voting;
    private DefaultRaftEndpointPersistentStateOrBuilder builder = this;

    @Override
    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    @Override
    public boolean isVoting() {
        return voting;
    }

    @Override
    @Nonnull
    public RaftEndpointPersistentStateBuilder setLocalEndpoint(@Nonnull RaftEndpoint localEndpoint) {
        builder.localEndpoint = localEndpoint;
        return this;
    }

    @Override
    @Nonnull
    public RaftEndpointPersistentStateBuilder setVoting(boolean voting) {
        builder.voting = voting;
        return this;
    }

    @Override
    public RaftEndpointPersistentState build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        String header = builder != null ? "RaftEndpointPersistentStateBuilder" : "RaftEndpointPersistentState";
        return header + "{" + "localEndpoint=" + localEndpoint + ", voting=" + voting + '}';
    }

}
