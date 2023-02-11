package io.microraft.model.persistence;

import io.microraft.model.RaftModel;

import io.microraft.RaftEndpoint;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;
import javax.annotation.Nonnegative;

public interface RaftTermPersistentState extends RaftModel {

    int getTerm();

    @Nullable
    RaftEndpoint getVotedFor();

    interface RaftTermPersistentStateBuilder {

        @Nonnull
        RaftTermPersistentStateBuilder setTerm(@Nonnegative int term);

        @Nonnull
        RaftTermPersistentStateBuilder setVotedFor(@Nullable RaftEndpoint votedFor);

        @Nonnull
        RaftTermPersistentState build();
    }

}
