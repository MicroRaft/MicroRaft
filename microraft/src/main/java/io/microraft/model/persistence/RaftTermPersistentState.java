/*
 * Copyright (c) 2023, MicroRaft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
