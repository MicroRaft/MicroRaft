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
import io.microraft.integration.RaftNodeRuntime;
import io.microraft.integration.StateMachine;
import io.microraft.model.groupop.TerminateRaftGroupOp.TerminateRaftGroupOpBuilder;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp.UpdateRaftGroupMembersOpBuilder;
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

import javax.annotation.Nonnull;

/**
 * Used for creating {@link RaftModel} objects.
 * <p>
 * Users of MicroRaft must provide an implementation of this interface while
 * creating {@link RaftNode} instances. {@link RaftModel} objects created by
 * a Raft model factory implementation are passed to {@link RaftNodeRuntime},
 * {@link StateMachine} and {@link RaftStore} for networking, operation
 * execution, and persistence.
 * <p>
 * Raft model objects are populated and created via the builder interfaces
 * returned from the methods of this interface.
 *
 * @author metanet
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
    TerminateRaftGroupOpBuilder createTerminateRaftGroupOpBuilder();

    @Nonnull
    UpdateRaftGroupMembersOpBuilder createUpdateRaftGroupMembersOpBuilder();

}
