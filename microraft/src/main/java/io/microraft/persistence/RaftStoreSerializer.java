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

package io.microraft.persistence;

import javax.annotation.Nonnull;

import io.microraft.RaftEndpoint;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState;

/**
 * Similarly to the {@link io.microraft.model.RaftModelFactory}, users of the
 * RaftStore implementations must provide methods for converting a few of their
 * types into binary data for persistence. This logic is expected to be
 * relatively straightforward for the implementer, since similar logic will
 * exist within the {@link io.microraft.transport.Transport}. It should be noted
 * that serialization performed here may need to be deserialized for an
 * indefinite period and so evolution of any relevant types should be considered
 * by the implementer.
 */
public interface RaftStoreSerializer {

    Serializer<RaftGroupMembersView> raftGroupMembersViewSerializer();

    Serializer<RaftEndpoint> raftEndpointSerializer();

    Serializer<LogEntry> logEntrySerializer();

    Serializer<SnapshotChunk> snapshotChunkSerializer();

    Serializer<RaftEndpointPersistentState> raftEndpointPersistentStateSerializer();

    Serializer<RaftTermPersistentState> raftTermPersistentState();

    interface Serializer<T> {
        @Nonnull
        byte[] serialize(@Nonnull T element);

        @Nonnull
        T deserialize(@Nonnull byte[] element);
    }
}
