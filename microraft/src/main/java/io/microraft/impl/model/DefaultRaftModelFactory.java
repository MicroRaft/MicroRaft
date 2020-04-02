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

package io.microraft.impl.model;

import io.microraft.impl.model.groupop.DefaultTerminateRaftGroupOp.DefaultTerminateRaftGroupOpBuilder;
import io.microraft.impl.model.groupop.DefaultUpdateRaftGroupMembersOp.DefaultUpdateRaftGroupMembersOpBuilder;
import io.microraft.impl.model.log.DefaultLogEntry.DefaultLogEntryBuilder;
import io.microraft.impl.model.log.DefaultSnapshotChunk.DefaultSnapshotChunkBuilder;
import io.microraft.impl.model.log.DefaultSnapshotEntry.DefaultSnapshotEntryBuilder;
import io.microraft.impl.model.message.DefaultAppendEntriesFailureResponse.DefaultAppendEntriesFailureResponseBuilder;
import io.microraft.impl.model.message.DefaultAppendEntriesRequest.DefaultAppendEntriesRequestBuilder;
import io.microraft.impl.model.message.DefaultAppendEntriesSuccessResponse.DefaultAppendEntriesSuccessResponseBuilder;
import io.microraft.impl.model.message.DefaultInstallSnapshotRequest.DefaultInstallSnapshotRequestBuilder;
import io.microraft.impl.model.message.DefaultInstallSnapshotResponse.DefaultInstallSnapshotResponseBuilder;
import io.microraft.impl.model.message.DefaultPreVoteRequest.DefaultPreVoteRequestBuilder;
import io.microraft.impl.model.message.DefaultPreVoteResponse.DefaultPreVoteResponseBuilder;
import io.microraft.impl.model.message.DefaultTriggerLeaderElectionRequest.DefaultTriggerLeaderElectionRequestBuilder;
import io.microraft.impl.model.message.DefaultVoteRequest.DefaultVoteRequestBuilder;
import io.microraft.impl.model.message.DefaultVoteResponse.DefaultVoteResponseBuilder;
import io.microraft.model.RaftModel;
import io.microraft.model.RaftModelFactory;
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

import javax.annotation.Nonnull;

/**
 * Used for creating {@link RaftModel} POJOs.
 * <p>
 * Could be used for testing purposes.
 *
 * @author metanet
 */
public final class DefaultRaftModelFactory
        implements RaftModelFactory {

    public static final RaftModelFactory INSTANCE = new DefaultRaftModelFactory();

    private DefaultRaftModelFactory() {
    }

    @Nonnull
    @Override
    public LogEntryBuilder createLogEntryBuilder() {
        return new DefaultLogEntryBuilder();
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder createSnapshotEntryBuilder() {
        return new DefaultSnapshotEntryBuilder();
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder createSnapshotChunkBuilder() {
        return new DefaultSnapshotChunkBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder createAppendEntriesRequestBuilder() {
        return new DefaultAppendEntriesRequestBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder createAppendEntriesSuccessResponseBuilder() {
        return new DefaultAppendEntriesSuccessResponseBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder createAppendEntriesFailureResponseBuilder() {
        return new DefaultAppendEntriesFailureResponseBuilder();
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder createInstallSnapshotRequestBuilder() {
        return new DefaultInstallSnapshotRequestBuilder();
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder createInstallSnapshotResponseBuilder() {
        return new DefaultInstallSnapshotResponseBuilder();
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder createPreVoteRequestBuilder() {
        return new DefaultPreVoteRequestBuilder();
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder createPreVoteResponseBuilder() {
        return new DefaultPreVoteResponseBuilder();
    }

    @Nonnull
    @Override
    public TriggerLeaderElectionRequestBuilder createTriggerLeaderElectionRequestBuilder() {
        return new DefaultTriggerLeaderElectionRequestBuilder();
    }

    @Nonnull
    @Override
    public VoteRequestBuilder createVoteRequestBuilder() {
        return new DefaultVoteRequestBuilder();
    }

    @Nonnull
    @Override
    public VoteResponseBuilder createVoteResponseBuilder() {
        return new DefaultVoteResponseBuilder();
    }

    @Nonnull
    @Override
    public TerminateRaftGroupOpBuilder createTerminateRaftGroupOpBuilder() {
        return new DefaultTerminateRaftGroupOpBuilder();
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder createUpdateRaftGroupMembersOpBuilder() {
        return new DefaultUpdateRaftGroupMembersOpBuilder();
    }

}
