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

import java.util.Collection;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.microraft.RaftEndpoint;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;

/**
 * Raft message for the InstallSnapshot RPC.
 * <p>
 * See <i>7 Log compaction</i> section of <i>In Search of an Understandable
 * Consensus Algorithm</i> paper by <i>Diego Ongaro</i> and <i>John
 * Ousterhout</i>.
 * <p>
 * Invoked by leader to send chunks of a snapshot to a follower. Chunks are sent
 * in the order defined by the follower and the follower is free to request the
 * chunks in any order.
 *
 * @see InstallSnapshotResponse
 */
public interface InstallSnapshotRequest extends RaftMessage {

    boolean isSenderLeader();

    @Nonnegative
    int getSnapshotTerm();

    @Nonnegative
    long getSnapshotIndex();

    @Nonnegative
    int getTotalSnapshotChunkCount();

    @Nullable
    SnapshotChunk getSnapshotChunk();

    @Nonnull
    Collection<RaftEndpoint> getSnapshottedMembers();

    @Nonnull
    RaftGroupMembersView getGroupMembersView();

    @Nonnegative
    long getQuerySequenceNumber();

    @Nonnegative
    long getFlowControlSequenceNumber();

    /**
     * The builder interface for {@link InstallSnapshotRequest}.
     */
    interface InstallSnapshotRequestBuilder extends RaftMessageBuilder<InstallSnapshotRequest> {

        @Nonnull
        InstallSnapshotRequestBuilder setGroupId(@Nonnull Object groupId);

        @Nonnull
        InstallSnapshotRequestBuilder setSender(@Nonnull RaftEndpoint sender);

        @Nonnull
        InstallSnapshotRequestBuilder setTerm(@Nonnegative int term);

        @Nonnull
        InstallSnapshotRequestBuilder setSenderLeader(boolean leader);

        @Nonnull
        InstallSnapshotRequestBuilder setSnapshotTerm(@Nonnegative int snapshotTerm);

        @Nonnull
        InstallSnapshotRequestBuilder setSnapshotIndex(@Nonnegative long snapshotIndex);

        @Nonnull
        InstallSnapshotRequestBuilder setTotalSnapshotChunkCount(@Nonnegative int totalSnapshotChunkCount);

        @Nonnull
        InstallSnapshotRequestBuilder setSnapshotChunk(@Nullable SnapshotChunk snapshotChunk);

        @Nonnull
        InstallSnapshotRequestBuilder setSnapshottedMembers(@Nonnull Collection<RaftEndpoint> snapshottedMembers);

        @Nonnull
        InstallSnapshotRequestBuilder setGroupMembersView(@Nonnull RaftGroupMembersView groupMembersView);

        @Nonnull
        InstallSnapshotRequestBuilder setQuerySequenceNumber(@Nonnegative long querySequenceNumber);

        @Nonnull
        InstallSnapshotRequestBuilder setFlowControlSequenceNumber(@Nonnegative long flowControlSequenceNumber);

    }

}
