package com.hazelcast.raft.impl.msg;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftMsg;
import com.hazelcast.raft.impl.log.SnapshotEntry;

/**
 * Raft message for the InstallSnapshot RPC.
 * <p>
 * See <i>7 Log compaction</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 * <p>
 * Invoked by leader to send chunks of a snapshot to a follower.
 * Leaders always send chunks in order.
 *
 * @author mdogan
 * @author metanet
 */
public class InstallSnapshotRequest
        implements RaftMsg {

    private static final long serialVersionUID = 6365973114621119759L;

    private RaftEndpoint leader;
    private int term;
    private SnapshotEntry snapshot;
    private long queryRound;

    public InstallSnapshotRequest() {
    }

    public InstallSnapshotRequest(RaftEndpoint leader, int term, SnapshotEntry snapshot, long queryRound) {
        this.leader = leader;
        this.term = term;
        this.snapshot = snapshot;
        this.queryRound = queryRound;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public int term() {
        return term;
    }

    public SnapshotEntry snapshot() {
        return snapshot;
    }

    public long queryRound() {
        return queryRound;
    }

    @Override
    public String toString() {
        return "InstallSnapshot{" + "leader=" + leader + ", term=" + term + ", snapshot=" + snapshot + ", queryRound="
                + queryRound + '}';
    }

}
