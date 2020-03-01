package com.hazelcast.raft.impl.groupop;

import com.hazelcast.raft.MembershipChangeMode;
import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftNode;

import java.util.Collection;

/**
 * Updates member list of a Raft group. This operation is replicated when a
 * Raft group membership change is triggered via
 * {@link RaftNode#changeMembership(RaftEndpoint, MembershipChangeMode, long)}.
 *
 * @author mdogan
 * @author metanet
 */
public class UpdateRaftGroupMembersOp
        extends RaftGroupOp {

    private static final long serialVersionUID = 7186635208600255009L;

    private Collection<RaftEndpoint> members;
    private RaftEndpoint endpoint;
    private MembershipChangeMode mode;

    public UpdateRaftGroupMembersOp() {
    }

    public UpdateRaftGroupMembersOp(Collection<RaftEndpoint> members, RaftEndpoint endpoint, MembershipChangeMode mode) {
        this.members = members;
        this.endpoint = endpoint;
        this.mode = mode;
    }

    public Collection<RaftEndpoint> getMembers() {
        return members;
    }

    public RaftEndpoint getEndpoint() {
        return endpoint;
    }

    public MembershipChangeMode getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return "ChangeRaftGroupMembersOp{" + "members=" + members + ", endpoint=" + endpoint + ", mode=" + mode + '}';
    }
}
