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

package io.microraft.model.impl.log;

import io.microraft.RaftEndpoint;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.RaftGroupMembersView.RaftGroupMembersViewBuilder;

import javax.annotation.Nonnull;
import java.util.Collection;

import static java.util.Objects.requireNonNull;

public class DefaultRaftGroupMembersViewOrBuilder
        implements RaftGroupMembersView, RaftGroupMembersViewBuilder {

    private long logIndex;
    private Collection<RaftEndpoint> members;
    private Collection<RaftEndpoint> votingMembers;
    private DefaultRaftGroupMembersViewOrBuilder builder = this;

    @Override public long getLogIndex() {
        return logIndex;
    }

    @Nonnull @Override public Collection<RaftEndpoint> getMembers() {
        return members;
    }

    @Nonnull @Override public Collection<RaftEndpoint> getVotingMembers() {
        return votingMembers;
    }

    @Nonnull @Override public RaftGroupMembersViewBuilder setLogIndex(long logIndex) {
        builder.logIndex = logIndex;
        return this;
    }

    @Nonnull @Override public RaftGroupMembersViewBuilder setMembers(@Nonnull Collection<RaftEndpoint> members) {
        builder.members = members;
        return this;
    }

    @Nonnull @Override public RaftGroupMembersViewBuilder setVotingMembers(@Nonnull Collection<RaftEndpoint> votingMembers) {
        builder.votingMembers = votingMembers;
        return this;
    }

    @Nonnull @Override public RaftGroupMembersView build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override public String toString() {
        String header = builder != null ? "RaftGroupMembersViewBuilder" : "RaftGroupMembersView";
        return header + "{" + "logIndex=" + logIndex + ", members=" + members + ", votingMembers=" + votingMembers + '}';
    }

}
