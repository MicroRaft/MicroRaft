package com.hazelcast.raft.exception;

import com.hazelcast.raft.RaftEndpoint;

import java.util.Collection;

/**
 * Thrown when a membership change is triggered with an expected
 * members-commit-index that doesn't match the actual members-commit-index
 * in the local state of the Raft group leader.
 *
 * @author mdogan
 * @author metanet
 */
public class MismatchingRaftGroupMembersCommitIndexException
        extends RaftException {

    private static final long serialVersionUID = -109570074579015635L;

    private final long commitIndex;
    private final Collection<RaftEndpoint> members;

    public MismatchingRaftGroupMembersCommitIndexException(long commitIndex, Collection<RaftEndpoint> members) {
        super("commit index: " + commitIndex + " members: " + members, null);
        this.commitIndex = commitIndex;
        this.members = members;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public Collection<RaftEndpoint> getMembers() {
        return members;
    }
}
