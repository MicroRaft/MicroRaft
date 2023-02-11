package io.microraft.model.persistence;

import io.microraft.model.RaftModel;
import io.microraft.RaftEndpoint;

import javax.annotation.Nonnull;

public interface RaftEndpointPersistentState extends RaftModel {

    RaftEndpoint getLocalEndpoint();

    boolean isVoting();

    interface RaftEndpointPersistentStateBuilder {

        @Nonnull
        RaftEndpointPersistentStateBuilder setLocalEndpoint(@Nonnull RaftEndpoint localEndpoint);

        @Nonnull
        RaftEndpointPersistentStateBuilder setVoting(boolean voting);

        RaftEndpointPersistentState build();
    }

}
