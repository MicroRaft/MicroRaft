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

package io.afloatdb.internal.utils;

import io.afloatdb.admin.proto.RaftGroupMembersProto;
import io.afloatdb.admin.proto.RaftLogStatsProto;
import io.afloatdb.admin.proto.RaftNodeReportProto;
import io.afloatdb.admin.proto.RaftNodeReportReasonProto;
import io.afloatdb.admin.proto.RaftNodeStatusProto;
import io.afloatdb.admin.proto.RaftRoleProto;
import io.afloatdb.admin.proto.RaftTermProto;
import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.internal.raft.impl.model.message.AppendEntriesFailureResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.AppendEntriesRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.AppendEntriesSuccessResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.InstallSnapshotRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.InstallSnapshotResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.PreVoteRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.PreVoteResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.RaftRequestAware;
import io.afloatdb.internal.raft.impl.model.message.TriggerLeaderElectionRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.VoteRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.VoteResponseOrBuilder;
import io.afloatdb.raft.proto.RaftRequest;
import io.microraft.RaftNodeStatus;
import io.microraft.RaftRole;
import io.microraft.model.message.RaftMessage;
import io.microraft.report.RaftGroupMembers;
import io.microraft.report.RaftLogStats;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReport.RaftNodeReportReason;
import io.microraft.report.RaftTerm;
import javax.annotation.Nonnull;

public final class Serialization {

    private Serialization() {
    }

    public static RaftNodeReportProto toProto(RaftNodeReport report) {
        return RaftNodeReportProto.newBuilder().setReason(toProto(report.getReason()))
                .setGroupId((String) report.getGroupId()).setEndpoint(AfloatDBEndpoint.unwrap(report.getEndpoint()))
                .setInitialMembers(toProto(report.getInitialMembers()))
                .setCommittedMembers(toProto(report.getCommittedMembers()))
                .setEffectiveMembers(toProto(report.getEffectiveMembers())).setRole(toProto(report.getRole()))
                .setStatus(toProto(report.getStatus())).setTerm(toProto(report.getTerm()))
                .setLog(toProto(report.getLog())).build();
    }

    public static RaftNodeReportReasonProto toProto(RaftNodeReportReason reason) {
        switch (reason) {
            case STATUS_CHANGE :
                return RaftNodeReportReasonProto.STATUS_CHANGE;
            case ROLE_CHANGE :
                return RaftNodeReportReasonProto.ROLE_CHANGE;
            case GROUP_MEMBERS_CHANGE :
                return RaftNodeReportReasonProto.GROUP_MEMBERS_CHANGE;
            case TAKE_SNAPSHOT :
                return RaftNodeReportReasonProto.TAKE_SNAPSHOT;
            case INSTALL_SNAPSHOT :
                return RaftNodeReportReasonProto.INSTALL_SNAPSHOT;
            case PERIODIC :
                return RaftNodeReportReasonProto.PERIODIC;
            case API_CALL :
                return RaftNodeReportReasonProto.API_CALL;
            default :
                throw new IllegalArgumentException("Invalid RaftNodeReportReason: " + reason);
        }
    }

    public static RaftGroupMembersProto toProto(RaftGroupMembers groupMembers) {
        RaftGroupMembersProto.Builder builder = RaftGroupMembersProto.newBuilder();
        builder.setLogIndex(groupMembers.getLogIndex());

        groupMembers.getMembers().stream().map(AfloatDBEndpoint::unwrap).forEach(builder::addMember);

        return builder.build();
    }

    public static RaftRoleProto toProto(RaftRole role) {
        switch (role) {
            case LEADER :
                return RaftRoleProto.LEADER;
            case CANDIDATE :
                return RaftRoleProto.CANDIDATE;
            case FOLLOWER :
                return RaftRoleProto.FOLLOWER;
            case LEARNER :
                return RaftRoleProto.LEARNER;
            default :
                throw new IllegalArgumentException("Invalid RaftRole: " + role);
        }
    }

    public static RaftNodeStatusProto toProto(RaftNodeStatus status) {
        switch (status) {
            case INITIAL :
                return RaftNodeStatusProto.INITIAL;
            case ACTIVE :
                return RaftNodeStatusProto.ACTIVE;
            case UPDATING_RAFT_GROUP_MEMBER_LIST :
                return RaftNodeStatusProto.UPDATING_RAFT_GROUP_MEMBER_LIST;
            case TERMINATED :
                return RaftNodeStatusProto.TERMINATED;
            default :
                throw new IllegalArgumentException("Invalid RaftNodeStatus: " + status);
        }
    }

    public static RaftTermProto toProto(RaftTerm term) {
        RaftTermProto.Builder builder = RaftTermProto.newBuilder();

        builder.setTerm(term.getTerm());

        if (term.getLeaderEndpoint() != null) {
            builder.setLeaderEndpoint(AfloatDBEndpoint.unwrap(term.getLeaderEndpoint()));
        }

        if (term.getVotedEndpoint() != null) {
            builder.setVotedEndpoint(AfloatDBEndpoint.unwrap(term.getVotedEndpoint()));
        }

        return builder.build();
    }

    public static RaftLogStatsProto toProto(RaftLogStats log) {
        RaftLogStatsProto.Builder builder = RaftLogStatsProto.newBuilder();
        builder.setCommitIndex(log.getCommitIndex()).setLastLogOrSnapshotIndex(log.getLastLogOrSnapshotIndex())
                .setLastLogOrSnapshotTerm(log.getLastLogOrSnapshotTerm()).setSnapshotIndex(log.getLastSnapshotIndex())
                .setSnapshotTerm(log.getLastSnapshotTerm()).setTakeSnapshotCount(log.getTakeSnapshotCount())
                .setInstallSnapshotCount(log.getInstallSnapshotCount());

        log.getFollowerMatchIndices()
                .forEach((key, value) -> builder.putFollowerMatchIndex(key.getId().toString(), value));

        return builder.build();
    }

    public static RaftMessage unwrap(@Nonnull RaftRequest request) {
        switch (request.getMessageCase()) {
            case VOTEREQUEST :
                return new VoteRequestOrBuilder(request.getVoteRequest());
            case VOTERESPONSE :
                return new VoteResponseOrBuilder(request.getVoteResponse());
            case APPENDENTRIESREQUEST :
                return new AppendEntriesRequestOrBuilder(request.getAppendEntriesRequest());
            case APPENDENTRIESSUCCESSRESPONSE :
                return new AppendEntriesSuccessResponseOrBuilder(request.getAppendEntriesSuccessResponse());
            case APPENDENTRIESFAILURERESPONSE :
                return new AppendEntriesFailureResponseOrBuilder(request.getAppendEntriesFailureResponse());
            case INSTALLSNAPSHOTREQUEST :
                return new InstallSnapshotRequestOrBuilder(request.getInstallSnapshotRequest());
            case INSTALLSNAPSHOTRESPONSE :
                return new InstallSnapshotResponseOrBuilder(request.getInstallSnapshotResponse());
            case PREVOTEREQUEST :
                return new PreVoteRequestOrBuilder(request.getPreVoteRequest());
            case PREVOTERESPONSE :
                return new PreVoteResponseOrBuilder(request.getPreVoteResponse());
            case TRIGGERLEADERELECTIONREQUEST :
                return new TriggerLeaderElectionRequestOrBuilder(request.getTriggerLeaderElectionRequest());
            default :
                throw new IllegalArgumentException("Invalid request: " + request);
        }
    }

    public static RaftRequest wrap(@Nonnull RaftMessage message) {
        RaftRequest.Builder builder = RaftRequest.newBuilder();
        if (message instanceof RaftRequestAware) {
            ((RaftRequestAware) message).populate(builder);
        } else {
            throw new IllegalArgumentException("Cannot convert " + message + " to proto");
        }

        return builder.build();
    }
}
