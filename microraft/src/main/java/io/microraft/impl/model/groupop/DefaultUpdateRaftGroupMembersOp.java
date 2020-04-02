/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright 2020, MicroRaft.
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

package io.microraft.impl.model.groupop;

import io.microraft.MembershipChangeMode;
import io.microraft.RaftEndpoint;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp;

import javax.annotation.Nonnull;
import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * @author metanet
 */
public final class DefaultUpdateRaftGroupMembersOp
        implements UpdateRaftGroupMembersOp {

    private Collection<RaftEndpoint> members;
    private RaftEndpoint endpoint;
    private MembershipChangeMode mode;

    private DefaultUpdateRaftGroupMembersOp() {
    }

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

    @Override
    public String toString() {
        return "UpdateRaftGroupMembersOp{" + "members=" + members + ", endpoint=" + endpoint + ", mode=" + mode + '}';
    }

    public static final class DefaultUpdateRaftGroupMembersOpBuilder
            implements UpdateRaftGroupMembersOpBuilder {

        private DefaultUpdateRaftGroupMembersOp op = new DefaultUpdateRaftGroupMembersOp();

        @Nonnull
        @Override
        public UpdateRaftGroupMembersOpBuilder setMembers(@Nonnull Collection<RaftEndpoint> members) {
            requireNonNull(members);
            op.members = members;
            return this;
        }

        @Nonnull
        @Override
        public UpdateRaftGroupMembersOpBuilder setEndpoint(@Nonnull RaftEndpoint endpoint) {
            requireNonNull(endpoint);
            op.endpoint = endpoint;
            return this;
        }

        @Nonnull
        @Override
        public UpdateRaftGroupMembersOpBuilder setMode(@Nonnull MembershipChangeMode mode) {
            requireNonNull(mode);
            op.mode = mode;
            return this;
        }

        @Override
        public UpdateRaftGroupMembersOp build() {
            if (this.op == null) {
                throw new IllegalStateException("UpdateRaftGroupMembersOp already built!");
            }

            UpdateRaftGroupMembersOp op = this.op;
            this.op = null;
            return op;
        }

    }

}
