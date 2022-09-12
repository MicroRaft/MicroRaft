/*
 * Copyright (c) 2020, AfloatDB.
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

package io.afloatdb.internal.raft.impl.model;

import io.afloatdb.internal.raft.impl.model.groupop.UpdateRaftGroupMembersOpOrBuilder;
import io.afloatdb.internal.raft.impl.model.log.LogEntryOrBuilder;
import io.afloatdb.internal.raft.impl.model.log.SnapshotChunkOrBuilder;
import io.afloatdb.internal.raft.impl.model.log.SnapshotEntryOrBuilder;
import io.afloatdb.internal.raft.impl.model.log.RaftGroupMembersViewOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.AppendEntriesFailureResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.AppendEntriesRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.AppendEntriesSuccessResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.InstallSnapshotRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.InstallSnapshotResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.PreVoteRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.PreVoteResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.TriggerLeaderElectionRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.VoteRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.VoteResponseOrBuilder;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp.UpdateRaftGroupMembersOpBuilder;
import io.microraft.model.log.LogEntry.LogEntryBuilder;
import io.microraft.model.log.RaftGroupMembersView.RaftGroupMembersViewBuilder;
import io.microraft.model.log.SnapshotChunk;
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
import javax.inject.Singleton;

@Singleton
public class ProtoRaftModelFactory implements RaftModelFactory {

    @Nonnull
    @Override
    public LogEntryBuilder createLogEntryBuilder() {
        return new LogEntryOrBuilder();
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder createSnapshotEntryBuilder() {
        return new SnapshotEntryOrBuilder();
    }

    @Nonnull
    @Override
    public SnapshotChunk.SnapshotChunkBuilder createSnapshotChunkBuilder() {
        return new SnapshotChunkOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder createAppendEntriesRequestBuilder() {
        return new AppendEntriesRequestOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder createAppendEntriesSuccessResponseBuilder() {
        return new AppendEntriesSuccessResponseOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder createAppendEntriesFailureResponseBuilder() {
        return new AppendEntriesFailureResponseOrBuilder();
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder createInstallSnapshotRequestBuilder() {
        return new InstallSnapshotRequestOrBuilder();
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder createInstallSnapshotResponseBuilder() {
        return new InstallSnapshotResponseOrBuilder();
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder createPreVoteRequestBuilder() {
        return new PreVoteRequestOrBuilder();
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder createPreVoteResponseBuilder() {
        return new PreVoteResponseOrBuilder();
    }

    @Nonnull
    @Override
    public TriggerLeaderElectionRequestBuilder createTriggerLeaderElectionRequestBuilder() {
        return new TriggerLeaderElectionRequestOrBuilder();
    }

    @Nonnull
    @Override
    public VoteRequestBuilder createVoteRequestBuilder() {
        return new VoteRequestOrBuilder();
    }

    @Nonnull
    @Override
    public VoteResponseBuilder createVoteResponseBuilder() {
        return new VoteResponseOrBuilder();
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder createUpdateRaftGroupMembersOpBuilder() {
        return new UpdateRaftGroupMembersOpOrBuilder();
    }

    @Override
    public RaftGroupMembersViewBuilder createRaftGroupMembersViewBuilder() {
        return new RaftGroupMembersViewOrBuilder();
    }

}
