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

package io.afloatdb.internal.raft.impl.model.groupop;

import io.afloatdb.AfloatDBException;
import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.raft.proto.UpdateRaftGroupMembersOpProto;
import io.afloatdb.raft.proto.UpdateRaftGroupMembersOpProto.MembershipChangeModeProto;
import io.microraft.MembershipChangeMode;
import io.microraft.RaftEndpoint;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp.UpdateRaftGroupMembersOpBuilder;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

public class UpdateRaftGroupMembersOpOrBuilder implements UpdateRaftGroupMembersOp, UpdateRaftGroupMembersOpBuilder {

    private UpdateRaftGroupMembersOpProto.Builder builder;
    private UpdateRaftGroupMembersOpProto op;
    private Collection<RaftEndpoint> members = new LinkedHashSet<>();
    private Collection<RaftEndpoint> votingMembers = new LinkedHashSet<>();
    private RaftEndpoint endpoint;

    public UpdateRaftGroupMembersOpOrBuilder() {
        this.builder = UpdateRaftGroupMembersOpProto.newBuilder();
    }

    public UpdateRaftGroupMembersOpOrBuilder(UpdateRaftGroupMembersOpProto op) {
        this.op = op;
        op.getMemberList().stream().map(AfloatDBEndpoint::wrap).forEach(members::add);
        op.getVotingMemberList().stream().map(AfloatDBEndpoint::wrap).forEach(votingMembers::add);
        this.endpoint = AfloatDBEndpoint.wrap(op.getEndpoint());
    }

    public UpdateRaftGroupMembersOpProto getOp() {
        return op;
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getMembers() {
        return Collections.unmodifiableCollection(members);
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getVotingMembers() {
        return Collections.unmodifiableCollection(votingMembers);
    }

    @Nonnull
    @Override
    public RaftEndpoint getEndpoint() {
        return endpoint;
    }

    @Nonnull
    @Override
    public MembershipChangeMode getMode() {
        if (op.getMode() == MembershipChangeModeProto.ADD_LEARNER) {
            return MembershipChangeMode.ADD_LEARNER;
        } else if (op.getMode() == MembershipChangeModeProto.ADD_OR_PROMOTE_TO_FOLLOWER) {
            return MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER;
        } else if (op.getMode() == MembershipChangeModeProto.REMOVE_MEMBER) {
            return MembershipChangeMode.REMOVE_MEMBER;
        }
        throw new IllegalStateException();
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setMembers(@Nonnull Collection<RaftEndpoint> members) {
        members.stream().map(AfloatDBEndpoint::unwrap).forEach(builder::addMember);
        this.members.clear();
        this.members.addAll(members);
        return this;
    }

    @Override
    public UpdateRaftGroupMembersOpBuilder setVotingMembers(Collection<RaftEndpoint> votingMembers) {
        members.stream().map(AfloatDBEndpoint::unwrap).forEach(builder::addVotingMember);
        this.votingMembers.clear();
        this.votingMembers.addAll(votingMembers);
        return this;
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setEndpoint(@Nonnull RaftEndpoint endpoint) {
        builder.setEndpoint(AfloatDBEndpoint.unwrap(endpoint));
        this.endpoint = endpoint;
        return this;
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setMode(@Nonnull MembershipChangeMode mode) {
        if (mode == MembershipChangeMode.ADD_LEARNER) {
            builder.setMode(MembershipChangeModeProto.ADD_LEARNER);
            return this;
        } else if (mode == MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER) {
            builder.setMode(MembershipChangeModeProto.ADD_OR_PROMOTE_TO_FOLLOWER);
            return this;
        } else if (mode == MembershipChangeMode.REMOVE_MEMBER) {
            builder.setMode(MembershipChangeModeProto.REMOVE_MEMBER);
            return this;
        }

        throw new AfloatDBException("Invalid mode: " + mode);
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOp build() {
        op = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "UpdateRaftGroupMembersOp{builder=" + builder + "}";
        }

        List<Object> memberIds = members.stream().map(RaftEndpoint::getId).collect(Collectors.toList());
        List<Object> votingMemberIds = votingMembers.stream().map(RaftEndpoint::getId).collect(Collectors.toList());
        return "UpdateRaftGroupMembersOp{" + "members=" + memberIds + ", votingMembers=" + votingMemberIds
                + ", endpoint=" + endpoint.getId() + ", " + "mode=" + getMode() + '}';
    }

}
