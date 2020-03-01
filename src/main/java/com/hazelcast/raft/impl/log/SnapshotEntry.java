package com.hazelcast.raft.impl.log;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.impl.msg.InstallSnapshotRequest;

import java.util.Collection;

/**
 * Represents a snapshot in the {@link RaftLog}.
 * <p>
 * Snapshot entry is sent to followers via
 * {@link InstallSnapshotRequest}.
 *
 * @author mdogan
 * @author metanet
 */
public class SnapshotEntry
        extends LogEntry {

    private static final long serialVersionUID = -4666840498564683538L;

    private long groupMembersLogIndex;
    private Collection<RaftEndpoint> groupMembers;

    public SnapshotEntry() {
    }

    public SnapshotEntry(int term, long index, Object operation, long groupMembersLogIndex,
                         Collection<RaftEndpoint> groupMembers) {
        super(term, index, operation);
        this.groupMembersLogIndex = groupMembersLogIndex;
        this.groupMembers = groupMembers;
    }

    public long groupMembersLogIndex() {
        return groupMembersLogIndex;
    }

    public Collection<RaftEndpoint> groupMembers() {
        return groupMembers;
    }

    public String toString(boolean detailed) {
        return "SnapshotEntry{" + "term=" + term() + ", index=" + index() + (detailed ? ", operation=" + operation() : "")
                + ", groupMembersLogIndex=" + groupMembersLogIndex + ", groupMembers=" + groupMembers + '}';
    }

    @Override
    public String toString() {
        return toString(false);
    }
}
