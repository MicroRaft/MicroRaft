package com.hazelcast.raft.impl.msg;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftMsg;
import com.hazelcast.raft.impl.handler.AppendEntriesRequestHandler;
import com.hazelcast.raft.impl.log.LogEntry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

/**
 * Raft message for the AppendEntries RPC.
 * <p>
 * See <i>5.3 Log replication</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 * <p>
 * Invoked by leader to replicate log entries (§5.3);
 * also used as heartbeat (§5.2).
 *
 * @author mdogan
 * @author metanet
 * @see AppendEntriesRequestHandler
 */
public class AppendEntriesRequest
        implements RaftMsg {

    private static final long serialVersionUID = -7399063044528480713L;

    private RaftEndpoint leader;
    private int term;
    private int prevLogTerm;
    private long prevLogIndex;
    private long leaderCommitIndex;
    private LogEntry[] entries;
    private long queryRound;

    public AppendEntriesRequest() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public AppendEntriesRequest(RaftEndpoint leader, int term, int prevLogTerm, long prevLogIndex, long leaderCommitIndex,
                                LogEntry[] entries, long queryRound) {
        this.leader = leader;
        this.term = term;
        this.prevLogTerm = prevLogTerm;
        this.prevLogIndex = prevLogIndex;
        this.leaderCommitIndex = leaderCommitIndex;
        this.entries = entries;
        this.queryRound = queryRound;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public int term() {
        return term;
    }

    public int prevLogTerm() {
        return prevLogTerm;
    }

    public long prevLogIndex() {
        return prevLogIndex;
    }

    public long leaderCommitIndex() {
        return leaderCommitIndex;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public LogEntry[] entries() {
        return entries;
    }

    public int entryCount() {
        return entries.length;
    }

    public long queryRound() {
        return queryRound;
    }

    @Override
    public String toString() {
        return "AppendRequest{" + "leader=" + leader + ", term=" + term + ", prevLogTerm=" + prevLogTerm + ", prevLogIndex="
                + prevLogIndex + ", leaderCommitIndex=" + leaderCommitIndex + ", queryRound=" + queryRound + ", entries=" + Arrays
                .toString(entries) + '}';
    }

}
