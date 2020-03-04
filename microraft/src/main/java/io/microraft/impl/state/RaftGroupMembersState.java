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

package io.microraft.impl.state;

import io.microraft.RaftEndpoint;
import io.microraft.report.RaftGroupMembers;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * Contains member list of a Raft group.
 *
 * @author mdogan
 * @author metanet
 **/
public class RaftGroupMembersState
        implements RaftGroupMembers {

    private long index;
    private Collection<RaftEndpoint> members;
    private Collection<RaftEndpoint> remoteMembers;

    public RaftGroupMembersState(long index, Collection<RaftEndpoint> members, RaftEndpoint localMember) {
        this.index = index;
        this.members = unmodifiableSet(new LinkedHashSet<>(members));
        Set<RaftEndpoint> remoteMembers = new LinkedHashSet<>(members);
        remoteMembers.remove(localMember);
        this.remoteMembers = unmodifiableSet(remoteMembers);
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

    /**
     * Returns the number of members in the Raft group.
     */
    public int memberCount() {
        return members.size();
    }

    /**
     * Returns the members in the Raft group, excluding the local endpoint.
     */
    public Collection<RaftEndpoint> remoteMembers() {
        return remoteMembers;
    }

    /**
     * Returns the majority number of the Raft group.
     */
    public int majority() {
        return members.size() / 2 + 1;
    }

    /**
     * Returns true if the given endpoint is a member of the Raft group,
     * false otherwise.
     */
    public boolean isKnownMember(RaftEndpoint endpoint) {
        return members.contains(endpoint);
    }

    @Override
    public String toString() {
        return "RaftGroupMembers{" + "index=" + index + ", members=" + members + '}';
    }

}
