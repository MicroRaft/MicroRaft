package io.microraft.model.persistence;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.microraft.RaftEndpoint;
import io.microraft.model.RaftModel;

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
