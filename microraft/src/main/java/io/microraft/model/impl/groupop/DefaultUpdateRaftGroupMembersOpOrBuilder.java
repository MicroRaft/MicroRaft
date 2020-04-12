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

package io.microraft.model.impl.groupop;

import io.microraft.MembershipChangeMode;
import io.microraft.RaftEndpoint;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp.UpdateRaftGroupMembersOpBuilder;

import javax.annotation.Nonnull;
import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public final class DefaultUpdateRaftGroupMembersOpOrBuilder
        implements UpdateRaftGroupMembersOp, UpdateRaftGroupMembersOpBuilder {

    private Collection<RaftEndpoint> members;
    private RaftEndpoint endpoint;
    private MembershipChangeMode mode;
    private DefaultUpdateRaftGroupMembersOpOrBuilder builder = this;

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getMembers() {
        return members;
    }

    @Nonnull
    @Override
    public RaftEndpoint getEndpoint() {
        return endpoint;
    }

    @Nonnull
    @Override
    public MembershipChangeMode getMode() {
        return mode;
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setMembers(@Nonnull Collection<RaftEndpoint> members) {
        builder.members = members;
        return this;
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setEndpoint(@Nonnull RaftEndpoint endpoint) {
        builder.endpoint = endpoint;
        return this;
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder setMode(@Nonnull MembershipChangeMode mode) {
        builder.mode = mode;
        return this;
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOp build() {
        requireNonNull(builder);
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        String header = builder != null ? "UpdateRaftGroupMembersOpBuilder" : "UpdateRaftGroupMembersOp";
        return header + "{" + "members=" + members + ", endpoint=" + endpoint + ", mode=" + mode + '}';
    }

}
