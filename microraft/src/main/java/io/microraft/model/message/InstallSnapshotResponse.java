/*
 * Copyright (c) 2020, MicroRaft.
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

package io.microraft.model.message;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import io.microraft.RaftEndpoint;

/**
 * Response for {@link InstallSnapshotRequest}.
 * <p>
 * See <i>7 Log compaction</i> section of <i>In Search of an Understandable
 * Consensus Algorithm</i> paper by <i>Diego Ongaro</i> and <i>John
 * Ousterhout</i>.
 * <p>
 * A follower can request the missing snapshot chunks in any order from the
 * leader.
 *
 * @see InstallSnapshotRequest
 */
public interface InstallSnapshotResponse extends RaftMessage {

    @Nonnegative
    long getSnapshotIndex();

    @Nonnegative
    int getRequestedSnapshotChunkIndex();

    @Nonnegative
    long getQuerySequenceNumber();

    @Nonnegative
    long getFlowControlSequenceNumber();

    /**
     * The builder interface for {@link InstallSnapshotResponse}.
     */
    interface InstallSnapshotResponseBuilder extends RaftMessageBuilder<InstallSnapshotResponse> {

        @Nonnull
        InstallSnapshotResponseBuilder setGroupId(@Nonnull Object groupId);

        @Nonnull
        InstallSnapshotResponseBuilder setSender(@Nonnull RaftEndpoint sender);

        @Nonnull
        InstallSnapshotResponseBuilder setTerm(@Nonnegative int term);

        @Nonnull
        InstallSnapshotResponseBuilder setSnapshotIndex(@Nonnegative long snapshotIndex);

        @Nonnull
        InstallSnapshotResponseBuilder setRequestedSnapshotChunkIndex(@Nonnegative int requestedSnapshotChunkIndex);

        @Nonnull
        InstallSnapshotResponseBuilder setQuerySequenceNumber(@Nonnegative long querySequenceNumber);

        @Nonnull
        InstallSnapshotResponseBuilder setFlowControlSequenceNumber(@Nonnegative long flowControlSequenceNumber);

    }

}
