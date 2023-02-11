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

package io.microraft.model.impl;

import io.microraft.model.RaftModel;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp.UpdateRaftGroupMembersOpBuilder;
import io.microraft.model.impl.groupop.DefaultUpdateRaftGroupMembersOpOrBuilder;
import io.microraft.model.impl.log.DefaultLogEntryOrBuilder;
import io.microraft.model.impl.log.DefaultRaftGroupMembersViewOrBuilder;
import io.microraft.model.impl.log.DefaultSnapshotChunkOrBuilder;
import io.microraft.model.impl.log.DefaultSnapshotEntryOrBuilder;
import io.microraft.model.impl.message.DefaultAppendEntriesFailureResponseOrBuilder;
import io.microraft.model.impl.message.DefaultAppendEntriesRequestOrBuilder;
import io.microraft.model.impl.message.DefaultAppendEntriesSuccessResponseOrBuilder;
import io.microraft.model.impl.message.DefaultInstallSnapshotRequestOrBuilder;
import io.microraft.model.impl.message.DefaultInstallSnapshotResponseOrBuilder;
import io.microraft.model.impl.message.DefaultPreVoteRequestOrBuilder;
import io.microraft.model.impl.message.DefaultPreVoteResponseOrBuilder;
import io.microraft.model.impl.message.DefaultTriggerLeaderElectionRequestOrBuilder;
import io.microraft.model.impl.message.DefaultVoteRequestOrBuilder;
import io.microraft.model.impl.message.DefaultVoteResponseOrBuilder;
import io.microraft.model.log.LogEntry.LogEntryBuilder;
import io.microraft.model.log.RaftGroupMembersView.RaftGroupMembersViewBuilder;
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
import io.microraft.model.persistence.RaftEndpointPersistentState.RaftEndpointPersistentStateBuilder;
import io.microraft.model.persistence.RaftTermPersistentState.RaftTermPersistentStateBuilder;
import io.microraft.model.impl.persistence.DefaultRaftEndpointPersistentStateOrBuilder;
import io.microraft.model.impl.persistence.DefaultRaftTermPersistentStateOrBuilder;

import javax.annotation.Nonnull;

/**
 * The default implementation of {@link RaftModelFactory}.
 * <p>
 * Creates POJO-style implementations of the {@link RaftModel} objects.
 */
public class DefaultRaftModelFactory implements RaftModelFactory {

    @Nonnull
    @Override
    public LogEntryBuilder createLogEntryBuilder() {
        return new DefaultLogEntryOrBuilder();
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder createSnapshotEntryBuilder() {
        return new DefaultSnapshotEntryOrBuilder();
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder createSnapshotChunkBuilder() {
        return new DefaultSnapshotChunkOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder createAppendEntriesRequestBuilder() {
        return new DefaultAppendEntriesRequestOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder createAppendEntriesSuccessResponseBuilder() {
        return new DefaultAppendEntriesSuccessResponseOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder createAppendEntriesFailureResponseBuilder() {
        return new DefaultAppendEntriesFailureResponseOrBuilder();
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder createInstallSnapshotRequestBuilder() {
        return new DefaultInstallSnapshotRequestOrBuilder();
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder createInstallSnapshotResponseBuilder() {
        return new DefaultInstallSnapshotResponseOrBuilder();
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder createPreVoteRequestBuilder() {
        return new DefaultPreVoteRequestOrBuilder();
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder createPreVoteResponseBuilder() {
        return new DefaultPreVoteResponseOrBuilder();
    }

    @Nonnull
    @Override
    public TriggerLeaderElectionRequestBuilder createTriggerLeaderElectionRequestBuilder() {
        return new DefaultTriggerLeaderElectionRequestOrBuilder();
    }

    @Nonnull
    @Override
    public VoteRequestBuilder createVoteRequestBuilder() {
        return new DefaultVoteRequestOrBuilder();
    }

    @Nonnull
    @Override
    public VoteResponseBuilder createVoteResponseBuilder() {
        return new DefaultVoteResponseOrBuilder();
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder createUpdateRaftGroupMembersOpBuilder() {
        return new DefaultUpdateRaftGroupMembersOpOrBuilder();
    }

    @Nonnull
    @Override
    public RaftGroupMembersViewBuilder createRaftGroupMembersViewBuilder() {
        return new DefaultRaftGroupMembersViewOrBuilder();
    }

    @Nonnull
    @Override
    public RaftEndpointPersistentStateBuilder createRaftEndpointPersistentStateBuilder() {
        return new DefaultRaftEndpointPersistentStateOrBuilder();
    }

    @Nonnull
    @Override
    public RaftTermPersistentStateBuilder createRaftTermPersistentStateBuilder() {
        return new DefaultRaftTermPersistentStateOrBuilder();
    }

}
