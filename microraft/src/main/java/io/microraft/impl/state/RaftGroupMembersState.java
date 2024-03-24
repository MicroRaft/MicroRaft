/*
 * Original work Copyright (c) 2008-2020, Hazelcast, Inc.
 * Modified work Copyright (c) 2020, MicroRaft.
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

package io.microraft.impl.state;

import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.annotation.Nonnull;

import io.microraft.RaftEndpoint;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.RaftGroupMembersView.RaftGroupMembersViewBuilder;
import io.microraft.report.RaftGroupMembers;

/**
 * Contains member list of a Raft group.
 **/
public final class RaftGroupMembersState implements RaftGroupMembers {

    private final long index;
    private final Collection<RaftEndpoint> members;
    private final Collection<RaftEndpoint> votingMembers;
    private final Collection<RaftEndpoint> remoteMembers;
    private final Collection<RaftEndpoint> remoteVotingMembers;
    private final int majority;

    public RaftGroupMembersState(long index, Collection<RaftEndpoint> members, Collection<RaftEndpoint> votingMembers,
            RaftEndpoint localMember) {
        if (index < 0) {
            throw new IllegalArgumentException("Invalid Raft group members log index: " + index);
        }
        requireNonNull(localMember);
        this.index = index;
        this.members = unmodifiableSet(new LinkedHashSet<>(requireNonNull(members)));
        Set<RaftEndpoint> voting = new LinkedHashSet<>(members);
        voting.retainAll(requireNonNull(votingMembers));
        if (voting.isEmpty()) {
            throw new IllegalArgumentException("Cannot have empty voting members!");
        }
        this.votingMembers = unmodifiableSet(voting);
        this.majority = votingMembers.size() / 2 + 1;
        Set<RaftEndpoint> remoteMembers = new LinkedHashSet<>(members);
        remoteMembers.remove(localMember);
        Set<RaftEndpoint> remoteVotingMembers = new LinkedHashSet<>(votingMembers);
        remoteVotingMembers.remove(localMember);
        remoteVotingMembers.retainAll(votingMembers);
        this.remoteMembers = unmodifiableSet(remoteMembers);
        this.remoteVotingMembers = unmodifiableSet(remoteVotingMembers);
    }

    /**
     * Returns index in the Raft log into which this member list is appended.
     */
    @Override
    public long getLogIndex() {
        return index;
    }

    /**
     * Returns all members in the Raft group.
     *
     * @see #remoteMembers()
     */
    @Nonnull
    @Override
    public Collection<RaftEndpoint> getMembers() {
        return members;
    }

    public Collection<RaftEndpoint> getMembersList() {
        return new ArrayList<>(members);
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getVotingMembers() {
        return votingMembers;
    }

    public Collection<RaftEndpoint> getVotingMembersList() {
        return new ArrayList<>(votingMembers);
    }

    /**
     * Returns the number of members in the Raft group.
     */
    public int memberCount() {
        return members.size();
    }

    /**
     * Returns the number of voting members in the Raft group.
     */
    public int votingMemberCount() {
        return votingMembers.size();
    }

    /**
     * Returns the members in the Raft group, excluding the local endpoint.
     */
    public Collection<RaftEndpoint> remoteMembers() {
        return remoteMembers;
    }

    /**
     * Returns the list of voting members in the Raft group member list, excluding
     * the local endpoint.
     */
    public Collection<RaftEndpoint> remoteVotingMembers() {
        return remoteVotingMembers;
    }

    /**
     * Returns the majority number of the Raft group member list.
     */
    @Override
    public int getMajorityQuorumSize() {
        return majority;
    }

    /**
     * Returns true if the given endpoint is a member of the Raft group, false
     * otherwise.
     */
    public boolean isKnownMember(RaftEndpoint endpoint) {
        return members.contains(endpoint);
    }

    /**
     * Returns true if the given endpoint is a voting member of the Raft group,
     * false otherwise.
     */
    public boolean isVotingMember(RaftEndpoint endpoint) {
        return votingMembers.contains(endpoint);
    }

    public RaftGroupMembersView populate(RaftGroupMembersViewBuilder builder) {
        return builder.setLogIndex(index).setMembers(members).setVotingMembers(votingMembers).build();
    }

    @Override
    public String toString() {
        return "RaftGroupMembers{" + "index=" + index + ", members=" + members + '}';
    }

}
