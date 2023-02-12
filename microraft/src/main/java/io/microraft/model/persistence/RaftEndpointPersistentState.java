package io.microraft.model.persistence;

import javax.annotation.Nonnull;

import io.microraft.RaftEndpoint;
import io.microraft.model.RaftModel;

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
