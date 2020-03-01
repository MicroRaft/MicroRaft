package com.hazelcast.raft.impl.msg;

import com.hazelcast.raft.RaftEndpoint;
import com.hazelcast.raft.RaftMsg;

/**
 * Raft message for the VoteRequest RPC.
 * <p>
 * See <i>5.2 Leader election</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 * <p>
 * Invoked by candidates to gather votes (§5.2).
 *
 * @author mdogan
 * @author metanet
 */
public class VoteRequest
        implements RaftMsg {

    private static final long serialVersionUID = 3812253469033695911L;

    private RaftEndpoint candidate;
    private int term;
    private int lastLogTerm;
    private long lastLogIndex;
    private boolean disruptive;

    public VoteRequest() {
    }

    public VoteRequest(RaftEndpoint candidate, int term, int lastLogTerm, long lastLogIndex, boolean disruptive) {
        this.term = term;
        this.candidate = candidate;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
        this.disruptive = disruptive;
    }

    public RaftEndpoint candidate() {
        return candidate;
    }

    public int term() {
        return term;
    }

    public int lastLogTerm() {
        return lastLogTerm;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    public boolean isDisruptive() {
        return disruptive;
    }

    @Override
    public String toString() {
        return "VoteRequest{" + "candidate=" + candidate + ", term=" + term + ", lastLogTerm=" + lastLogTerm + ", lastLogIndex="
                + lastLogIndex + ", disruptive=" + disruptive + '}';
    }

}
