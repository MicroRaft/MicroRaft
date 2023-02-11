package io.microraft.model.impl.persistence;

import io.microraft.RaftEndpoint;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftEndpointPersistentState.RaftEndpointPersistentStateBuilder;;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

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
