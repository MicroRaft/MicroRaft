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

package io.microraft.model.groupop;

import io.microraft.MembershipChangeMode;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Updates member list of a Raft group.
 * <p>
 * This operation is replicated when a Raft group membership change is triggered via
 * {@link RaftNode#changeMembership(RaftEndpoint, MembershipChangeMode, long)}.
 */
public interface UpdateRaftGroupMembersOp extends RaftGroupOp {

    @Nonnull
    Collection<RaftEndpoint> getMembers();

    @Nonnull
    Collection<RaftEndpoint> getVotingMembers();

    @Nonnull
    RaftEndpoint getEndpoint();

    @Nonnull
    MembershipChangeMode getMode();

    /**
     * The builder interface for {@link UpdateRaftGroupMembersOp}.
     */
    interface UpdateRaftGroupMembersOpBuilder {

        @Nonnull
        UpdateRaftGroupMembersOpBuilder setMembers(@Nonnull Collection<RaftEndpoint> members);

        @Nonnull
        UpdateRaftGroupMembersOpBuilder setVotingMembers(@Nonnull Collection<RaftEndpoint> votingMembers);

        @Nonnull
        UpdateRaftGroupMembersOpBuilder setEndpoint(@Nonnull RaftEndpoint endpoint);

        @Nonnull
        UpdateRaftGroupMembersOpBuilder setMode(@Nonnull MembershipChangeMode mode);

        @Nonnull
        UpdateRaftGroupMembersOp build();

    }

}
