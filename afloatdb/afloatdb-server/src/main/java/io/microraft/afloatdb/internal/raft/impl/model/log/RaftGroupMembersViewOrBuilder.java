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

package io.microraft.afloatdb.internal.raft.impl.model.log;

import io.microraft.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.microraft.afloatdb.raft.proto.RaftGroupMembersViewProto;
import io.microraft.RaftEndpoint;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.RaftGroupMembersView.RaftGroupMembersViewBuilder;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;

public class RaftGroupMembersViewOrBuilder implements RaftGroupMembersView, RaftGroupMembersViewBuilder {

    private RaftGroupMembersViewProto.Builder builder;
    private RaftGroupMembersViewProto groupMembersView;
    private Collection<RaftEndpoint> members = new LinkedHashSet<>();
    private Collection<RaftEndpoint> votingMembers = new LinkedHashSet<>();

    public RaftGroupMembersViewOrBuilder() {
        this.builder = RaftGroupMembersViewProto.newBuilder();
    }

    public RaftGroupMembersViewOrBuilder(RaftGroupMembersViewProto groupMembersView) {
        this.groupMembersView = groupMembersView;
        groupMembersView.getMemberList().stream().map(AfloatDBEndpoint::wrap).forEach(members::add);
        groupMembersView.getVotingMemberList().stream().map(AfloatDBEndpoint::wrap).forEach(votingMembers::add);
    }

    public RaftGroupMembersViewProto getGroupMembersView() {
        return groupMembersView;
    }

    @Override
    public long getLogIndex() {
        return groupMembersView.getLogIndex();
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
    public RaftGroupMembersViewBuilder setLogIndex(long logIndex) {
        builder.setLogIndex(logIndex);
        return this;
    }

    @Nonnull
    @Override
    public RaftGroupMembersViewBuilder setMembers(@Nonnull Collection<RaftEndpoint> members) {
        members.stream().map(AfloatDBEndpoint::unwrap).forEach(builder::addMember);
        this.members.clear();
        this.members.addAll(members);
        return this;
    }

    @Nonnull
    @Override
    public RaftGroupMembersViewBuilder setVotingMembers(@Nonnull Collection<RaftEndpoint> votingMembers) {
        votingMembers.stream().map(AfloatDBEndpoint::unwrap).forEach(builder::addVotingMember);
        this.votingMembers.clear();
        this.votingMembers.addAll(votingMembers);
        return this;
    }

    @Nonnull
    @Override
    public RaftGroupMembersView build() {
        groupMembersView = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "RaftGroupMembersView{builder=" + builder + "}";
        }

        return "RaftGroupMembersView{" + "logIndex=" + getLogIndex() + ", members=" + getMembers() + ", votingMembers="
                + getVotingMembers() + '}';
    }

}