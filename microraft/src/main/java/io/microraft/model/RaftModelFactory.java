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

package io.microraft.model;

import io.microraft.RaftNode;
import io.microraft.lifecycle.RaftNodeLifecycleAware;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp.UpdateRaftGroupMembersOpBuilder;
import io.microraft.model.impl.DefaultRaftModelFactory;
import io.microraft.model.log.LogEntry.LogEntryBuilder;
import io.microraft.model.log.SnapshotChunk.SnapshotChunkBuilder;
import io.microraft.model.log.SnapshotEntry.SnapshotEntryBuilder;
import io.microraft.model.message.AppendEntriesFailureResponse.AppendEntriesFailureResponseBuilder;
import io.microraft.model.message.AppendEntriesRequest.AppendEntriesRequestBuilder;
import io.microraft.model.message.AppendEntriesSuccessResponse.AppendEntriesSuccessResponseBuilder;
import io.microraft.model.message.InstallSnapshotRequest.InstallSnapshotRequestBuilder;
import io.microraft.model.message.InstallSnapshotResponse.InstallSnapshotResponseBuilder;
import io.microraft.model.message.PreVoteRequest.PreVoteRequestBuilder;
import io.microraft.model.message.PreVoteResponse.PreVoteResponseBuilder;
import io.microraft.model.message.TriggerLeaderElectionRequest.TriggerLeaderElectionRequestBuilder;
import io.microraft.model.message.VoteRequest.VoteRequestBuilder;
import io.microraft.model.message.VoteResponse.VoteResponseBuilder;
import io.microraft.persistence.RaftStore;
import io.microraft.transport.Transport;

import javax.annotation.Nonnull;

/**
 * Used for creating {@link RaftModel} objects with the builder pattern.
 * <p>
 * Users of MicroRaft can provide an implementation of this interface while
 * creating {@link RaftNode} instances. Otherwise,
 * {@link DefaultRaftModelFactory} is used. {@link RaftModel} objects created
 * by a Raft model factory implementation are passed to {@link Transport} for
 * networking, and {@link RaftStore} for persistence.
 * <p>
 * A {@link RaftModelFactory} implementation can implement
 * {@link RaftNodeLifecycleAware} to perform initialization and clean up work
 * during {@link RaftNode} startup and termination. {@link RaftNode} calls
 * {@link RaftNodeLifecycleAware#onRaftNodeStart()} before calling any other
 * method on {@link RaftModelFactory}, and finally calls
 * {@link RaftNodeLifecycleAware#onRaftNodeTerminate()} on termination.
 */
public interface RaftModelFactory {

    @Nonnull
    LogEntryBuilder createLogEntryBuilder();

    @Nonnull
    SnapshotEntryBuilder createSnapshotEntryBuilder();

    @Nonnull
    SnapshotChunkBuilder createSnapshotChunkBuilder();

    @Nonnull
    AppendEntriesRequestBuilder createAppendEntriesRequestBuilder();

    @Nonnull
    AppendEntriesSuccessResponseBuilder createAppendEntriesSuccessResponseBuilder();

    @Nonnull
    AppendEntriesFailureResponseBuilder createAppendEntriesFailureResponseBuilder();

    @Nonnull
    InstallSnapshotRequestBuilder createInstallSnapshotRequestBuilder();

    @Nonnull
    InstallSnapshotResponseBuilder createInstallSnapshotResponseBuilder();

    @Nonnull
    PreVoteRequestBuilder createPreVoteRequestBuilder();

    @Nonnull
    PreVoteResponseBuilder createPreVoteResponseBuilder();

    @Nonnull
    TriggerLeaderElectionRequestBuilder createTriggerLeaderElectionRequestBuilder();

    @Nonnull
    VoteRequestBuilder createVoteRequestBuilder();

    @Nonnull
    VoteResponseBuilder createVoteResponseBuilder();

    @Nonnull
    UpdateRaftGroupMembersOpBuilder createUpdateRaftGroupMembersOpBuilder();

}
