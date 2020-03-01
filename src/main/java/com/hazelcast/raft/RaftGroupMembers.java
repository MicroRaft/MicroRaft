package com.hazelcast.raft;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * Keeps member list of a Raft group with an index identifying on which log
 * index the given member list is append to the Raft log.
 * <p>
 * The initial member list of a Raft group has index of 0.
 *
 * @author mdogan
 * @author metanet
 */
public class RaftGroupMembers {

    private long index;
    private Collection<RaftEndpoint> members;
    private Collection<RaftEndpoint> remoteMembers;

    public RaftGroupMembers(long index, Collection<RaftEndpoint> members, RaftEndpoint localMember) {
        this.index = index;
        this.members = unmodifiableSet(new LinkedHashSet<>(members));
        Set<RaftEndpoint> remoteMembers = new LinkedHashSet<>(members);
        remoteMembers.remove(localMember);
        this.remoteMembers = unmodifiableSet(remoteMembers);
    }

    /**
     * Returns position in the Raft log when this member list is appended.
     */
    public long index() {
        return index;
    }

    /**
     * Returns all members in the Raft group.
     *
     * @see #remoteMembers()
     */
    public Collection<RaftEndpoint> members() {
        return members;
    }

    /**
     * Returns the members in the Raft group, excluding the local endpoint.
     */
    public Collection<RaftEndpoint> remoteMembers() {
        return remoteMembers;
    }

    /**
     * Returns the number of members in the Raft group.
     */
    public int memberCount() {
        return members.size();
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
